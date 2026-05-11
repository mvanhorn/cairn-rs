use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use cairn_domain::*;
use cairn_store::projections::{
    ApprovalDelegationReadModel, ApprovalDelegationRecord, ApprovalReadModel, ApprovalRecord,
    RunReadModel,
};
use cairn_store::EventLog;

use super::event_helpers::make_envelope;
use crate::approvals::ApprovalService;
use crate::error::RuntimeError;

/// Monotonic counter for `delegation_id` mint. Combined with the wall
/// clock (`delegated_at_ms`) it guarantees uniqueness even when two
/// delegations of the same approval to the same operator race into
/// the same millisecond — the PK was previously `(approval_id,
/// delegated_to, delegated_at_ms)` which silently dropped one of those
/// rows under `ON CONFLICT DO NOTHING`. Same pattern as
/// `AUDIT_COUNTER` in `audit_impl.rs`.
static DELEGATION_COUNTER: AtomicU64 = AtomicU64::new(1);

fn next_delegation_id(approval_id: &ApprovalId, delegated_at_ms: u64) -> String {
    let seq = DELEGATION_COUNTER.fetch_add(1, Ordering::SeqCst);
    format!("deleg_{}_{delegated_at_ms}_{seq}", approval_id.as_str())
}

pub struct ApprovalServiceImpl<S> {
    store: Arc<S>,
}

impl<S> ApprovalServiceImpl<S> {
    pub fn new(store: Arc<S>) -> Self {
        Self { store }
    }
}

#[async_trait]
impl<S> ApprovalService for ApprovalServiceImpl<S>
where
    S: EventLog + ApprovalReadModel + ApprovalDelegationReadModel + RunReadModel + 'static,
{
    async fn request(
        &self,
        project: &ProjectKey,
        approval_id: ApprovalId,
        run_id: Option<RunId>,
        task_id: Option<TaskId>,
        requirement: ApprovalRequirement,
    ) -> Result<ApprovalRecord, RuntimeError> {
        self.request_with_context(
            project,
            approval_id,
            run_id,
            task_id,
            requirement,
            None,
            None,
        )
        .await
    }

    async fn request_with_context(
        &self,
        project: &ProjectKey,
        approval_id: ApprovalId,
        run_id: Option<RunId>,
        task_id: Option<TaskId>,
        requirement: ApprovalRequirement,
        title: Option<String>,
        description: Option<String>,
    ) -> Result<ApprovalRecord, RuntimeError> {
        let saved_run_id = run_id.clone();
        let event = make_envelope(RuntimeEvent::ApprovalRequested(ApprovalRequested {
            project: project.clone(),
            approval_id: approval_id.clone(),
            run_id,
            task_id,
            requirement,
            title,
            description,
        }));

        // T3-H2: batch ApprovalRequested + (optional) RunStateChanged into
        // a single append so the cross-aggregate state lands atomically.
        // Pre-fix: two successive appends; a crash in-between left the
        // approval pending but the run still Running, which then confused
        // `resume`'s pending-approval gate.
        //
        // T3-L7: if a `run_id` is supplied but the run doesn't exist, fail
        // loudly. Pre-fix this produced an orphan approval with no run
        // state while silently skipping the transition block.
        let mut events = vec![event];
        if let Some(ref rid) = saved_run_id {
            let run = RunReadModel::get(self.store.as_ref(), rid)
                .await?
                .ok_or_else(|| RuntimeError::NotFound {
                    entity: "run",
                    id: rid.to_string(),
                })?;
            if can_transition_run_state(run.state, RunState::WaitingApproval) {
                events.push(make_envelope(RuntimeEvent::RunStateChanged(
                    RunStateChanged {
                        project: project.clone(),
                        run_id: rid.clone(),
                        transition: StateTransition {
                            from: Some(run.state),
                            to: RunState::WaitingApproval,
                        },
                        failure_class: None,
                        pause_reason: None,
                        resume_trigger: None,
                    },
                )));
            }
        }
        self.store.append(&events).await?;

        ApprovalReadModel::get(self.store.as_ref(), &approval_id)
            .await?
            .ok_or_else(|| RuntimeError::Internal("approval not found after request".into()))
    }

    async fn get(&self, approval_id: &ApprovalId) -> Result<Option<ApprovalRecord>, RuntimeError> {
        Ok(ApprovalReadModel::get(self.store.as_ref(), approval_id).await?)
    }

    async fn resolve(
        &self,
        approval_id: &ApprovalId,
        decision: ApprovalDecision,
    ) -> Result<ApprovalRecord, RuntimeError> {
        let approval = ApprovalReadModel::get(self.store.as_ref(), approval_id)
            .await?
            .ok_or_else(|| RuntimeError::NotFound {
                entity: "approval",
                id: approval_id.to_string(),
            })?;

        if approval.decision.is_some() {
            return Err(RuntimeError::InvalidTransition {
                entity: "approval",
                from: "resolved".into(),
                to: format!("{decision:?}"),
            });
        }

        let event = make_envelope(RuntimeEvent::ApprovalResolved(ApprovalResolved {
            project: approval.project.clone(),
            approval_id: approval_id.clone(),
            decision,
        }));

        // T3-H2: batch ApprovalResolved + cascading RunStateChanged into
        // a single append so the cross-aggregate state lands atomically.
        // Pre-fix: two successive appends; a crash in-between left the
        // approval resolved but the run stranded in WaitingApproval.
        let mut events = vec![event];
        if let Some(ref run_id) = approval.run_id {
            if let Some(run) = RunReadModel::get(self.store.as_ref(), run_id).await? {
                let (to_state, failure_class, resume_trigger) = match decision {
                    ApprovalDecision::Approved => {
                        (RunState::Running, None, Some(ResumeTrigger::OperatorResume))
                    }
                    ApprovalDecision::Rejected => {
                        (RunState::Failed, Some(FailureClass::ApprovalRejected), None)
                    }
                };
                if can_transition_run_state(run.state, to_state) {
                    events.push(make_envelope(RuntimeEvent::RunStateChanged(
                        RunStateChanged {
                            project: run.project.clone(),
                            run_id: run_id.clone(),
                            transition: StateTransition {
                                from: Some(run.state),
                                to: to_state,
                            },
                            failure_class,
                            pause_reason: None,
                            resume_trigger,
                        },
                    )));
                }
            }
        }
        self.store.append(&events).await?;

        ApprovalReadModel::get(self.store.as_ref(), approval_id)
            .await?
            .ok_or_else(|| RuntimeError::Internal("approval not found after resolve".into()))
    }

    async fn list_pending(
        &self,
        project: &ProjectKey,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<ApprovalRecord>, RuntimeError> {
        Ok(self.store.list_pending(project, limit, offset).await?)
    }

    async fn list_all(
        &self,
        project: &ProjectKey,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<ApprovalRecord>, RuntimeError> {
        Ok(self.store.list_all(project, limit, offset).await?)
    }

    async fn delegate(
        &self,
        approval_id: &ApprovalId,
        delegated_to: String,
    ) -> Result<ApprovalDelegationRecord, RuntimeError> {
        if delegated_to.trim().is_empty() {
            return Err(RuntimeError::Validation {
                reason: "delegated_to must not be empty".to_owned(),
            });
        }

        let approval = ApprovalReadModel::get(self.store.as_ref(), approval_id)
            .await?
            .ok_or_else(|| RuntimeError::NotFound {
                entity: "approval",
                id: approval_id.to_string(),
            })?;

        if approval.decision.is_some() {
            return Err(RuntimeError::InvalidTransition {
                entity: "approval",
                from: "resolved".into(),
                to: "delegated".into(),
            });
        }

        // Clock safety (Copilot #571): `duration_since(UNIX_EPOCH)` can
        // fail if the host clock is before the epoch (a mis-configured
        // VM, a failed NTP sync on cold boot). The prior
        // `unwrap_or_default()` silently produced ms = 0, which would
        // both lose audit-trail ordering and create PK collisions with
        // the earliest possible delegation. The `as_millis()` → `u64`
        // cast was also unchecked. Fail loudly on either anomaly so
        // operators see the bad clock rather than a corrupted audit
        // trail.
        let delegated_at_ms = u64::try_from(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|err| {
                    RuntimeError::Internal(format!("system clock is before UNIX_EPOCH: {err}"))
                })?
                .as_millis(),
        )
        .map_err(|_| {
            RuntimeError::Internal(
                "delegated_at_ms overflow converting u128 milliseconds to u64".to_owned(),
            )
        })?;

        // Copilot #571 round 4: `delegation_id` widens the PK so two
        // delegations of the same approval to the same operator in the
        // same millisecond both survive the projection. Previously the
        // PK was `(approval_id, delegated_to, delegated_at_ms)` which
        // would silently drop the second row under ON CONFLICT DO NOTHING.
        let delegation_id = next_delegation_id(approval_id, delegated_at_ms);

        let event = make_envelope(RuntimeEvent::ApprovalDelegated(ApprovalDelegated {
            approval_id: approval_id.clone(),
            delegated_to: delegated_to.clone(),
            delegated_at_ms,
            delegation_id: delegation_id.clone(),
        }));
        self.store.append(&[event]).await?;

        Ok(ApprovalDelegationRecord {
            approval_id: approval_id.clone(),
            delegated_to,
            delegated_at_ms,
            delegation_id,
        })
    }

    async fn list_delegations(
        &self,
        approval_id: &ApprovalId,
    ) -> Result<Vec<ApprovalDelegationRecord>, RuntimeError> {
        Ok(
            ApprovalDelegationReadModel::list_for_approval(self.store.as_ref(), approval_id)
                .await?,
        )
    }
}
