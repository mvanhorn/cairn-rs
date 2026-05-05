use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use cairn_domain::events::EventEnvelope;
use cairn_domain::events::{
    EventSource, RunCreated, RunStateChanged, RuntimeEvent, SessionCreated, SessionStateChanged,
    StateTransition, TaskCreated, TaskLeaseClaimed, TaskStateChanged,
};
use cairn_domain::ids::{EventId, RunId, SessionId, TaskId};
use cairn_domain::lifecycle::{
    FailureClass, PauseReason, ResumeTrigger, RunState, SessionState, TaskState,
};
use cairn_domain::tenancy::ProjectKey;
use cairn_store::event_log::EventLog;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

#[derive(Clone, Debug)]
pub enum BridgeEvent {
    ExecutionCreated {
        run_id: RunId,
        session_id: SessionId,
        project: ProjectKey,
        /// Parent run id for subagent / child runs. `None` for top-level.
        /// FF's exec_core already carries this in the `cairn.parent_run_id`
        /// tag, but the bridge must thread it through to `RunCreated` so the
        /// cairn-store projection can reconstruct the run tree.
        parent_run_id: Option<RunId>,
        /// External correlation id (sqeq ingress etc.). Tagged onto the
        /// resulting `EventEnvelope.correlation_id` so audit / SSE
        /// downstreams can join back to the originating request. `None`
        /// for internal starts.
        correlation_id: Option<String>,
        /// #670 G6: agent role id for subagent/child runs created via
        /// the `spawn_subagent` path. `None` for operator-initiated
        /// top-level runs — the orchestrator loop defaults to the
        /// `"orchestrator"` role when this is `None`. For a spawned
        /// subagent, this carries the role the parent's LLM picked
        /// (`executor`, `researcher`, `reviewer`, or a custom role
        /// from the registry) so the child's orchestrator loop can
        /// select the correct system prompt on its first iteration.
        agent_role_id: Option<String>,
    },
    ExecutionCompleted {
        run_id: RunId,
        project: ProjectKey,
        prev_state: Option<RunState>,
    },
    ExecutionFailed {
        run_id: RunId,
        project: ProjectKey,
        failure_class: FailureClass,
        prev_state: Option<RunState>,
    },
    ExecutionCancelled {
        run_id: RunId,
        project: ProjectKey,
        prev_state: Option<RunState>,
    },
    ExecutionSuspended {
        run_id: RunId,
        project: ProjectKey,
        prev_state: Option<RunState>,
        /// Post-suspension state from the service's `read_run_record`
        /// (already adjusted for FF blocking_reason). Callers emit the
        /// observed state so projection/SSE don't drift from HGETALL.
        /// Approval-gated suspensions become `WaitingApproval`; plain
        /// operator pauses stay `Paused`.
        to: RunState,
        /// Why the run suspended. Threads through to the
        /// `RunStateChanged.pause_reason` column on the event log so
        /// the `pause_schedules` projection can INSERT a row with the
        /// correct `resume_after_ms` (issue #591). Populate this
        /// whenever the emitter has a structured reason — including
        /// worker-SDK subagent suspensions, which now emit
        /// `PauseReasonKind::RuntimeSuspension` with a
        /// `subagent:<child_task_id>` detail. Use `None` only for
        /// callers that genuinely have no structured pause
        /// classification available at the emission site.
        #[allow(clippy::struct_field_names)]
        pause_reason: Option<PauseReason>,
    },
    ExecutionResumed {
        run_id: RunId,
        project: ProjectKey,
        prev_state: Option<RunState>,
        /// Resume source. Mirrors the pause-side `pause_reason` for
        /// symmetry: callers that can classify the resume trigger
        /// (operator vs timer-fired vs runtime signal) thread it
        /// through so the projection row records who unpaused the run.
        /// Approval-granted resumes are operator-driven and emit
        /// `Some(ResumeTrigger::OperatorResume)`. `None` is reserved
        /// for call sites where the trigger is genuinely indeterminate
        /// at emission time.
        #[allow(clippy::struct_field_names)]
        resume_trigger: Option<ResumeTrigger>,
    },
    TaskCreated {
        task_id: TaskId,
        project: ProjectKey,
        /// Session the task was minted against (same binding used by
        /// `id_map::session_task_to_execution_id`). `None` for bare
        /// (session-less) submissions.
        session_id: Option<SessionId>,
        parent_run_id: Option<RunId>,
        parent_task_id: Option<TaskId>,
    },
    TaskLeaseClaimed {
        task_id: TaskId,
        project: ProjectKey,
        lease_owner: String,
        lease_epoch: u64,
        lease_expires_at_ms: u64,
    },
    TaskStateChanged {
        task_id: TaskId,
        project: ProjectKey,
        to: TaskState,
        failure_class: Option<FailureClass>,
    },
    ExecutionRetryScheduled {
        run_id: RunId,
        project: ProjectKey,
        /// FF attempt counter. Carried so downstream observability can
        /// show retry progress; not persisted in RunStateChanged today.
        attempt: u32,
        /// Previous public_state before retry was scheduled. FF can retry
        /// from `Running`, `Suspended` (waitpoint expiry), or `Delayed`
        /// (chained retries) — hardcoding `Running` falsifies history in
        /// the projection.
        prev_state: Option<RunState>,
    },
    SessionCreated {
        session_id: SessionId,
        project: ProjectKey,
    },
    SessionArchived {
        session_id: SessionId,
        project: ProjectKey,
    },
    /// Emitted after a successful `declare_dependency` FCALL sequence
    /// on the Fabric layer. Written to the EventLog as
    /// `RuntimeEvent::TaskDependencyAdded` for audit. No projection
    /// reads it — dependency authority lives in FF, and
    /// `check_dependencies` reads edge state live via
    /// `ff_evaluate_flow_eligibility`.
    TaskDependencyAdded {
        dependent_task_id: TaskId,
        prerequisite_task_id: TaskId,
        project: ProjectKey,
        edge_id: String,
        flow_id: String,
        created_at_ms: u64,
        dependency_kind: cairn_domain::DependencyKind,
        data_passing_ref: Option<String>,
    },
    /// F64: terminal-write recovery loop outcome. Emitted once per
    /// complete/fail/cancel that entered the recovery loop, regardless
    /// of whether the loop recovered or timed out. Persisted as
    /// `RuntimeEvent::TerminalRecoveryAttempted` on the event log +
    /// `runs.terminal_write_recovery_json` on the projection so
    /// operators see the attempt summary on the run detail page.
    TerminalRecoveryAttempted {
        run_id: RunId,
        project: ProjectKey,
        fcall: String,
        attempts: u32,
        wall_time_ms: u64,
        outcome: String,
        occurred_at_ms: u64,
    },
    /// Issue #670 G1+G2: LLM-initiated subagent spawn. Emitted by the
    /// `FabricTaskServiceAdapter::spawn_subagent` override immediately
    /// after the underlying `FabricTaskService::submit` (which emits
    /// its own `TaskCreated`). Carries the LLM's delegation intent —
    /// the sub-goal and the role — so the `subagent_spawns` projection
    /// captures the spawn audit row with the operator context the
    /// parent actually delegated with.
    ///
    /// Distinct from the operator-initiated path
    /// (`POST /v1/runs/:id/spawn` → `RunService::spawn_subagent`),
    /// which creates a child `RunRecord` via the `RunCreated` event
    /// and does NOT flow through `TaskService::spawn_subagent`.
    SubagentSpawned {
        parent_run_id: RunId,
        parent_task_id: Option<TaskId>,
        child_task_id: TaskId,
        child_session_id: SessionId,
        /// Child run is created by a separate increment (G3 in `#670`);
        /// this field is always `None` for G1+G2 and reserved for the
        /// follow-up PR that wires child-run creation.
        child_run_id: Option<RunId>,
        project: ProjectKey,
        /// Sub-goal the parent delegated, taken from the LLM's
        /// `ActionProposal.tool_args["goal"]` string.
        goal: String,
        /// Agent role the parent delegated to, taken from the LLM's
        /// `ActionProposal.tool_name` string (pre-validated against the
        /// known-role allow-list by the execute layer).
        role: String,
    },
}

/// Internal consumer-channel payload. Wraps `BridgeEvent` with an
/// in-band flush marker so `EventBridge::flush()` can observe that all
/// previously-emitted events have been appended to the event log.
///
/// FIFO on the mpsc channel guarantees ordering: every event `emit`ted
/// before a flush lands in the store before the flush ack fires, and
/// every event `emit`ted after the flush lands after. Callers can thus
/// safely read-after-write against their own emit by awaiting a flush
/// between the emit and the store read (issue #568).
///
/// `#[allow(clippy::large_enum_variant)]`: the `Event` variant wraps
/// a `BridgeEvent` which is intentionally a flat enum for cache
/// locality on the hot path. Boxing every event would add an alloc
/// per emit on a channel that carries every runtime event in the
/// process — a perf regression bigger than the memory saving. The
/// gap (`Event` ≈ 240 B, `Flush` ≈ 8 B) is tolerated because `Flush`
/// fires rarely (operator-triggered read-after-write barriers).
#[allow(clippy::large_enum_variant)]
enum ConsumerItem {
    Event(BridgeEvent),
    Flush(oneshot::Sender<()>),
}

pub struct EventBridge {
    tx: mpsc::Sender<ConsumerItem>,
    cancel: CancellationToken,
    append_failures: Arc<AtomicU64>,
    /// Counts events dropped because the consumer channel was closed
    /// (i.e. the bridge background task exited before the producer).
    /// Distinct from `append_failures`, which counts events that reached
    /// the consumer but failed to persist to the event log.
    emit_failures: Arc<AtomicU64>,
}

const MAX_RETRY_ATTEMPTS: u32 = 3;
const RETRY_BACKOFF_MS: u64 = 100;

impl EventBridge {
    pub fn start(event_log: Arc<dyn EventLog + Send + Sync>) -> (Self, JoinHandle<()>) {
        let (tx, rx) = mpsc::channel::<ConsumerItem>(1024);
        let cancel = CancellationToken::new();
        let append_failures = Arc::new(AtomicU64::new(0));

        let handle = tokio::spawn(Self::run_consumer(
            rx,
            event_log,
            cancel.clone(),
            append_failures.clone(),
        ));

        let bridge = Self {
            tx,
            cancel,
            append_failures,
            emit_failures: Arc::new(AtomicU64::new(0)),
        };
        (bridge, handle)
    }

    /// Number of events dropped because the consumer channel was closed.
    /// Exposed so tests and operator metrics can observe bridge-side loss.
    pub fn emit_failures(&self) -> u64 {
        self.emit_failures.load(Ordering::Relaxed)
    }

    async fn run_consumer(
        mut rx: mpsc::Receiver<ConsumerItem>,
        event_log: Arc<dyn EventLog + Send + Sync>,
        cancel: CancellationToken,
        append_failures: Arc<AtomicU64>,
    ) {
        loop {
            let item = tokio::select! {
                biased;
                ev = rx.recv() => match ev {
                    Some(e) => e,
                    None => break,
                },
                () = cancel.cancelled() => {
                    break;
                }
            };
            Self::handle_item(&event_log, item, &append_failures).await;
        }

        // Drain remaining events after stop signal. Flush acks still
        // fire on remaining items so any flush waiter that slipped in
        // before stop does not hang forever.
        rx.close();
        while let Some(item) = rx.recv().await {
            Self::handle_item(&event_log, item, &append_failures).await;
        }
    }

    async fn handle_item(
        event_log: &Arc<dyn EventLog + Send + Sync>,
        item: ConsumerItem,
        append_failures: &AtomicU64,
    ) {
        match item {
            ConsumerItem::Event(event) => {
                Self::append_with_retry(event_log, &event, append_failures).await;
            }
            ConsumerItem::Flush(ack) => {
                // All items enqueued before this flush have been handled
                // by the consumer loop (mpsc is FIFO). Signal the waiter;
                // ignore a closed receiver — the caller dropped the
                // oneshot, no one is listening.
                let _ = ack.send(());
            }
        }
    }

    async fn append_with_retry(
        event_log: &Arc<dyn EventLog + Send + Sync>,
        event: &BridgeEvent,
        append_failures: &AtomicU64,
    ) {
        let runtime_event = bridge_event_to_runtime_event(event);
        let mut envelope = EventEnvelope::for_runtime_event(
            EventId::new(uuid::Uuid::new_v4().to_string()),
            EventSource::Runtime,
            runtime_event,
        );
        if let Some(corr) = bridge_event_correlation_id(event) {
            envelope = envelope.with_correlation_id(corr);
        }
        let event_type = bridge_event_type_name(event);

        for attempt in 0..MAX_RETRY_ATTEMPTS {
            match event_log.append(std::slice::from_ref(&envelope)).await {
                Ok(_) => return,
                Err(e) => {
                    if attempt + 1 < MAX_RETRY_ATTEMPTS {
                        tracing::warn!(
                            attempt = attempt + 1,
                            event_type,
                            error = %e,
                            "event bridge: append failed, retrying"
                        );
                        tokio::time::sleep(std::time::Duration::from_millis(
                            RETRY_BACKOFF_MS * (1 << attempt) as u64,
                        ))
                        .await;
                    } else {
                        append_failures.fetch_add(1, Ordering::Relaxed);
                        tracing::error!(
                            event_type,
                            error = %e,
                            total_failures = append_failures.load(Ordering::Relaxed),
                            "event bridge: append failed after {MAX_RETRY_ATTEMPTS} attempts"
                        );
                    }
                }
            }
        }
    }

    pub async fn emit(&self, event: BridgeEvent) {
        let event_type = bridge_event_type_name(&event);
        if let Err(e) = self.tx.send(ConsumerItem::Event(event)).await {
            // `fetch_add` returns the previous value; add 1 for the
            // post-increment count without a separate (race-prone) load.
            let total = self.emit_failures.fetch_add(1, Ordering::Relaxed) + 1;
            tracing::error!(
                event_type,
                error = %e,
                total_emit_failures = total,
                "event bridge: channel closed — event dropped, projection will have a gap"
            );
        }
    }

    /// Wait for every event previously passed to [`Self::emit`] from the
    /// calling task to reach the event store. Enqueues a FIFO marker on
    /// the consumer channel and awaits the consumer's ack.
    ///
    /// Issue #568: the bridge is a tokio mpsc + async consumer, so an
    /// immediate `bridge.emit(X); read_store()` can miss `X` — the
    /// consumer hasn't run yet. Callers that need read-after-write on
    /// their own emit (every caller of `publish_runtime_frames_since`)
    /// must await `flush` between the emit and the read.
    ///
    /// Returns immediately (no-op) if the consumer channel is closed —
    /// the bridge has already been stopped and no new events will land.
    /// Callers degrade gracefully: the subsequent store read will simply
    /// see whatever was there before shutdown, matching the pre-flush
    /// behaviour on a shutting-down bridge.
    pub async fn flush(&self) {
        let (ack_tx, ack_rx) = oneshot::channel();
        if self.tx.send(ConsumerItem::Flush(ack_tx)).await.is_err() {
            // Consumer gone — nothing to wait for. Caller will fall
            // through to the store read with whatever is already there.
            tracing::debug!(
                "event bridge: flush requested on closed consumer channel — returning immediately"
            );
            return;
        }
        // Error only if the consumer dropped the sender without calling
        // `send(())` — that happens when the consumer task exits mid-drain
        // (stop + task abort). Treat as the same degradation case as a
        // closed producer channel: degrade gracefully rather than hang.
        if ack_rx.await.is_err() {
            tracing::debug!("event bridge: flush ack lost (consumer exited mid-drain) — returning");
        }
    }

    pub fn stop(&self) {
        self.cancel.cancel();
    }

    pub fn append_failure_count(&self) -> u64 {
        self.append_failures.load(Ordering::Relaxed)
    }
}

fn bridge_event_type_name(event: &BridgeEvent) -> &'static str {
    match event {
        BridgeEvent::ExecutionCreated { .. } => "ExecutionCreated",
        BridgeEvent::ExecutionCompleted { .. } => "ExecutionCompleted",
        BridgeEvent::ExecutionFailed { .. } => "ExecutionFailed",
        BridgeEvent::ExecutionCancelled { .. } => "ExecutionCancelled",
        BridgeEvent::ExecutionSuspended { .. } => "ExecutionSuspended",
        BridgeEvent::ExecutionResumed { .. } => "ExecutionResumed",
        BridgeEvent::TaskCreated { .. } => "TaskCreated",
        BridgeEvent::TaskLeaseClaimed { .. } => "TaskLeaseClaimed",
        BridgeEvent::TaskStateChanged { .. } => "TaskStateChanged",
        BridgeEvent::ExecutionRetryScheduled { .. } => "ExecutionRetryScheduled",
        BridgeEvent::SessionCreated { .. } => "SessionCreated",
        BridgeEvent::SessionArchived { .. } => "SessionArchived",
        BridgeEvent::TaskDependencyAdded { .. } => "TaskDependencyAdded",
        BridgeEvent::TerminalRecoveryAttempted { .. } => "TerminalRecoveryAttempted",
        BridgeEvent::SubagentSpawned { .. } => "SubagentSpawned",
    }
}

/// External correlation id carried by the bridge event, if any. Only
/// `ExecutionCreated` carries one today — sqeq ingress threads a
/// request-level correlation through `start_with_correlation`.
fn bridge_event_correlation_id(event: &BridgeEvent) -> Option<&str> {
    match event {
        BridgeEvent::ExecutionCreated {
            correlation_id: Some(c),
            ..
        } => Some(c.as_str()),
        _ => None,
    }
}

fn bridge_event_to_runtime_event(event: &BridgeEvent) -> RuntimeEvent {
    match event {
        BridgeEvent::ExecutionCreated {
            run_id,
            session_id,
            project,
            parent_run_id,
            correlation_id: _,
            agent_role_id,
        } => RuntimeEvent::RunCreated(RunCreated {
            project: project.clone(),
            session_id: session_id.clone(),
            run_id: run_id.clone(),
            parent_run_id: parent_run_id.clone(),
            prompt_release_id: None,
            agent_role_id: agent_role_id.clone(),
        }),
        BridgeEvent::ExecutionCompleted {
            run_id,
            project,
            prev_state,
        } => RuntimeEvent::RunStateChanged(RunStateChanged {
            project: project.clone(),
            run_id: run_id.clone(),
            transition: StateTransition {
                from: *prev_state,
                to: RunState::Completed,
            },
            failure_class: None,
            pause_reason: None,
            resume_trigger: None,
        }),
        BridgeEvent::ExecutionFailed {
            run_id,
            project,
            failure_class,
            prev_state,
        } => RuntimeEvent::RunStateChanged(RunStateChanged {
            project: project.clone(),
            run_id: run_id.clone(),
            transition: StateTransition {
                from: *prev_state,
                to: RunState::Failed,
            },
            failure_class: Some(*failure_class),
            pause_reason: None,
            resume_trigger: None,
        }),
        BridgeEvent::ExecutionCancelled {
            run_id,
            project,
            prev_state,
        } => RuntimeEvent::RunStateChanged(RunStateChanged {
            project: project.clone(),
            run_id: run_id.clone(),
            transition: StateTransition {
                from: *prev_state,
                to: RunState::Canceled,
            },
            failure_class: Some(FailureClass::CanceledByOperator),
            pause_reason: None,
            resume_trigger: None,
        }),
        BridgeEvent::ExecutionSuspended {
            run_id,
            project,
            prev_state,
            to,
            pause_reason,
        } => RuntimeEvent::RunStateChanged(RunStateChanged {
            project: project.clone(),
            run_id: run_id.clone(),
            transition: StateTransition {
                from: *prev_state,
                to: *to,
            },
            failure_class: None,
            // Issue #591: `pause_reason` (carrying `resume_after_ms`) now
            // threads through to the event log so downstream projections
            // can observe scheduled resumes. Previously hard-coded to
            // `None`, which made timer-fired resumes invisible to the
            // service-layer path.
            pause_reason: pause_reason.clone(),
            resume_trigger: None,
        }),
        BridgeEvent::ExecutionResumed {
            run_id,
            project,
            prev_state,
            resume_trigger,
        } => RuntimeEvent::RunStateChanged(RunStateChanged {
            project: project.clone(),
            run_id: run_id.clone(),
            transition: StateTransition {
                from: *prev_state,
                to: RunState::Running,
            },
            failure_class: None,
            pause_reason: None,
            // Issue #591 symmetry: the matching resume event carries the
            // trigger classification so audit / operator UI can tell
            // a timer-fired resume apart from an operator-initiated one.
            resume_trigger: *resume_trigger,
        }),
        BridgeEvent::TaskCreated {
            task_id,
            project,
            session_id,
            parent_run_id,
            parent_task_id,
        } => RuntimeEvent::TaskCreated(TaskCreated {
            project: project.clone(),
            task_id: task_id.clone(),
            parent_run_id: parent_run_id.clone(),
            parent_task_id: parent_task_id.clone(),
            prompt_release_id: None,
            session_id: session_id.clone(),
        }),
        BridgeEvent::TaskLeaseClaimed {
            task_id,
            project,
            lease_owner,
            lease_epoch,
            lease_expires_at_ms,
        } => RuntimeEvent::TaskLeaseClaimed(TaskLeaseClaimed {
            project: project.clone(),
            task_id: task_id.clone(),
            lease_owner: lease_owner.clone(),
            lease_token: *lease_epoch,
            lease_expires_at_ms: *lease_expires_at_ms,
        }),
        BridgeEvent::TaskStateChanged {
            task_id,
            project,
            to,
            failure_class,
        } => RuntimeEvent::TaskStateChanged(TaskStateChanged {
            project: project.clone(),
            task_id: task_id.clone(),
            transition: StateTransition {
                from: None,
                to: *to,
            },
            failure_class: *failure_class,
            pause_reason: None,
            resume_trigger: None,
        }),
        BridgeEvent::ExecutionRetryScheduled {
            run_id,
            project,
            prev_state,
            attempt: _,
        } => RuntimeEvent::RunStateChanged(RunStateChanged {
            project: project.clone(),
            run_id: run_id.clone(),
            transition: StateTransition {
                from: *prev_state,
                to: RunState::Pending,
            },
            failure_class: None,
            pause_reason: None,
            resume_trigger: None,
        }),
        BridgeEvent::SessionCreated {
            session_id,
            project,
        } => RuntimeEvent::SessionCreated(SessionCreated {
            project: project.clone(),
            session_id: session_id.clone(),
        }),
        BridgeEvent::SessionArchived {
            session_id,
            project,
        } => RuntimeEvent::SessionStateChanged(SessionStateChanged {
            project: project.clone(),
            session_id: session_id.clone(),
            transition: StateTransition {
                from: Some(SessionState::Open),
                to: SessionState::Archived,
            },
        }),
        BridgeEvent::TaskDependencyAdded {
            dependent_task_id,
            prerequisite_task_id,
            created_at_ms,
            dependency_kind,
            data_passing_ref,
            ..
        } => RuntimeEvent::TaskDependencyAdded(cairn_domain::events::TaskDependencyAdded {
            task_id: dependent_task_id.clone(),
            depends_on: prerequisite_task_id.clone(),
            added_at_ms: *created_at_ms,
            dependent_task_id: dependent_task_id.clone(),
            depends_on_task_id: prerequisite_task_id.clone(),
            dependency_kind: *dependency_kind,
            data_passing_ref: data_passing_ref.clone(),
        }),
        BridgeEvent::TerminalRecoveryAttempted {
            run_id,
            project,
            fcall,
            attempts,
            wall_time_ms,
            outcome,
            occurred_at_ms,
        } => RuntimeEvent::TerminalRecoveryAttempted(
            cairn_domain::events::TerminalRecoveryAttempted {
                project: project.clone(),
                run_id: run_id.clone(),
                fcall: fcall.clone(),
                attempts: *attempts,
                wall_time_ms: *wall_time_ms,
                outcome: outcome.clone(),
                occurred_at_ms: *occurred_at_ms,
            },
        ),
        // #670 G1+G2: LLM-initiated subagent spawn. Translates
        // straight across — the domain event carries the same field
        // set plus the two new G2 strings (goal + role) the execute
        // layer populates from the `ActionProposal`.
        BridgeEvent::SubagentSpawned {
            parent_run_id,
            parent_task_id,
            child_task_id,
            child_session_id,
            child_run_id,
            project,
            goal,
            role,
        } => RuntimeEvent::SubagentSpawned(cairn_domain::events::SubagentSpawned {
            project: project.clone(),
            parent_run_id: parent_run_id.clone(),
            parent_task_id: parent_task_id.clone(),
            child_task_id: child_task_id.clone(),
            child_session_id: child_session_id.clone(),
            child_run_id: child_run_id.clone(),
            goal: goal.clone(),
            role: role.clone(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bridge_event_to_runtime_created() {
        let event = BridgeEvent::ExecutionCreated {
            run_id: RunId::new("run_1"),
            session_id: SessionId::new("sess_1"),
            project: ProjectKey::new("t", "w", "p"),
            parent_run_id: None,
            correlation_id: None,
            agent_role_id: None,
        };
        let runtime = bridge_event_to_runtime_event(&event);
        assert!(matches!(runtime, RuntimeEvent::RunCreated(_)));
    }

    // T4-C2 regression: parent_run_id threads through to the RunCreated
    // projection so subagent run trees don't orphan.
    #[test]
    fn bridge_event_to_runtime_created_propagates_parent_run_id() {
        let event = BridgeEvent::ExecutionCreated {
            run_id: RunId::new("child_run"),
            session_id: SessionId::new("sess_1"),
            project: ProjectKey::new("t", "w", "p"),
            parent_run_id: Some(RunId::new("parent_run")),
            correlation_id: None,
            agent_role_id: None,
        };
        match bridge_event_to_runtime_event(&event) {
            RuntimeEvent::RunCreated(rc) => {
                assert_eq!(rc.parent_run_id, Some(RunId::new("parent_run")));
            }
            _ => panic!("expected RunCreated"),
        }
    }

    // #670 G6 regression: agent_role_id threads through to RunCreated
    // so the child's orchestrator loop picks up the delegated role's
    // system prompt on its first iteration.
    #[test]
    fn bridge_event_to_runtime_created_propagates_agent_role_id() {
        let event = BridgeEvent::ExecutionCreated {
            run_id: RunId::new("child_run"),
            session_id: SessionId::new("sess_1"),
            project: ProjectKey::new("t", "w", "p"),
            parent_run_id: Some(RunId::new("parent_run")),
            correlation_id: None,
            agent_role_id: Some("researcher".to_owned()),
        };
        match bridge_event_to_runtime_event(&event) {
            RuntimeEvent::RunCreated(rc) => {
                assert_eq!(rc.agent_role_id, Some("researcher".to_owned()));
            }
            _ => panic!("expected RunCreated"),
        }
    }

    #[test]
    fn bridge_event_correlation_id_extracts_execution_created() {
        let with_corr = BridgeEvent::ExecutionCreated {
            run_id: RunId::new("run_1"),
            session_id: SessionId::new("sess_1"),
            project: ProjectKey::new("t", "w", "p"),
            parent_run_id: None,
            correlation_id: Some("corr_xyz".to_owned()),
            agent_role_id: None,
        };
        assert_eq!(bridge_event_correlation_id(&with_corr), Some("corr_xyz"));

        let without_corr = BridgeEvent::ExecutionCreated {
            run_id: RunId::new("run_1"),
            session_id: SessionId::new("sess_1"),
            project: ProjectKey::new("t", "w", "p"),
            parent_run_id: None,
            correlation_id: None,
            agent_role_id: None,
        };
        assert_eq!(bridge_event_correlation_id(&without_corr), None);

        // Other variants never carry a correlation today.
        let other = BridgeEvent::SessionCreated {
            session_id: SessionId::new("sess_1"),
            project: ProjectKey::new("t", "w", "p"),
        };
        assert_eq!(bridge_event_correlation_id(&other), None);
    }

    #[test]
    fn bridge_event_to_runtime_completed() {
        let event = BridgeEvent::ExecutionCompleted {
            run_id: RunId::new("run_1"),
            project: ProjectKey::new("t", "w", "p"),
            prev_state: Some(RunState::Running),
        };
        let runtime = bridge_event_to_runtime_event(&event);
        match runtime {
            RuntimeEvent::RunStateChanged(rsc) => {
                assert_eq!(rsc.transition.from, Some(RunState::Running));
                assert_eq!(rsc.transition.to, RunState::Completed);
                assert!(rsc.failure_class.is_none());
            }
            _ => panic!("expected RunStateChanged"),
        }
    }

    #[test]
    fn bridge_event_to_runtime_completed_from_waiting() {
        let event = BridgeEvent::ExecutionCompleted {
            run_id: RunId::new("run_1"),
            project: ProjectKey::new("t", "w", "p"),
            prev_state: Some(RunState::WaitingDependency),
        };
        let runtime = bridge_event_to_runtime_event(&event);
        match runtime {
            RuntimeEvent::RunStateChanged(rsc) => {
                assert_eq!(rsc.transition.from, Some(RunState::WaitingDependency));
                assert_eq!(rsc.transition.to, RunState::Completed);
            }
            _ => panic!("expected RunStateChanged"),
        }
    }

    #[test]
    fn bridge_event_to_runtime_failed() {
        let event = BridgeEvent::ExecutionFailed {
            run_id: RunId::new("run_1"),
            project: ProjectKey::new("t", "w", "p"),
            failure_class: FailureClass::TimedOut,
            prev_state: Some(RunState::Running),
        };
        let runtime = bridge_event_to_runtime_event(&event);
        match runtime {
            RuntimeEvent::RunStateChanged(rsc) => {
                assert_eq!(rsc.transition.from, Some(RunState::Running));
                assert_eq!(rsc.transition.to, RunState::Failed);
                assert_eq!(rsc.failure_class, Some(FailureClass::TimedOut));
            }
            _ => panic!("expected RunStateChanged"),
        }
    }

    #[test]
    fn bridge_event_to_runtime_cancelled() {
        let event = BridgeEvent::ExecutionCancelled {
            run_id: RunId::new("run_1"),
            project: ProjectKey::new("t", "w", "p"),
            prev_state: Some(RunState::Running),
        };
        let runtime = bridge_event_to_runtime_event(&event);
        match runtime {
            RuntimeEvent::RunStateChanged(rsc) => {
                assert_eq!(rsc.transition.from, Some(RunState::Running));
                assert_eq!(rsc.transition.to, RunState::Canceled);
            }
            _ => panic!("expected RunStateChanged"),
        }
    }

    #[test]
    fn bridge_event_to_runtime_suspended_pauses_by_default() {
        let event = BridgeEvent::ExecutionSuspended {
            run_id: RunId::new("run_1"),
            project: ProjectKey::new("t", "w", "p"),
            prev_state: Some(RunState::Running),
            to: RunState::Paused,
            pause_reason: None,
        };
        let runtime = bridge_event_to_runtime_event(&event);
        match runtime {
            RuntimeEvent::RunStateChanged(rsc) => {
                assert_eq!(rsc.transition.from, Some(RunState::Running));
                assert_eq!(rsc.transition.to, RunState::Paused);
                assert!(rsc.pause_reason.is_none());
            }
            _ => panic!("expected RunStateChanged"),
        }
    }

    // T4-C1 regression: suspended-for-approval must land as WaitingApproval
    // in the projection, not Paused.
    #[test]
    fn bridge_event_to_runtime_suspended_for_approval() {
        let event = BridgeEvent::ExecutionSuspended {
            run_id: RunId::new("run_1"),
            project: ProjectKey::new("t", "w", "p"),
            prev_state: Some(RunState::Running),
            to: RunState::WaitingApproval,
            pause_reason: None,
        };
        let runtime = bridge_event_to_runtime_event(&event);
        match runtime {
            RuntimeEvent::RunStateChanged(rsc) => {
                assert_eq!(rsc.transition.from, Some(RunState::Running));
                assert_eq!(rsc.transition.to, RunState::WaitingApproval);
            }
            _ => panic!("expected RunStateChanged"),
        }
    }

    // Issue #591 regression: `pause_reason` (including `resume_after_ms`)
    // must survive the bridge→RunStateChanged conversion so the
    // `pause_schedules` projection lands a row. Previously hard-coded to
    // `None`, which silently dropped scheduled resumes emitted via the
    // service path.
    #[test]
    fn bridge_event_to_runtime_suspended_threads_pause_reason() {
        use cairn_domain::lifecycle::{PauseReason, PauseReasonKind};

        let reason = PauseReason {
            kind: PauseReasonKind::OperatorPause,
            detail: Some("on-call handoff".to_owned()),
            resume_after_ms: Some(60_000),
            actor: Some("alice".to_owned()),
        };
        let event = BridgeEvent::ExecutionSuspended {
            run_id: RunId::new("run_1"),
            project: ProjectKey::new("t", "w", "p"),
            prev_state: Some(RunState::Running),
            to: RunState::Paused,
            pause_reason: Some(reason.clone()),
        };
        match bridge_event_to_runtime_event(&event) {
            RuntimeEvent::RunStateChanged(rsc) => {
                let got = rsc.pause_reason.expect("pause_reason must survive");
                assert_eq!(got, reason);
            }
            _ => panic!("expected RunStateChanged"),
        }
    }

    #[test]
    fn bridge_event_to_runtime_resumed() {
        let event = BridgeEvent::ExecutionResumed {
            run_id: RunId::new("run_1"),
            project: ProjectKey::new("t", "w", "p"),
            prev_state: Some(RunState::Paused),
            resume_trigger: None,
        };
        let runtime = bridge_event_to_runtime_event(&event);
        match runtime {
            RuntimeEvent::RunStateChanged(rsc) => {
                assert_eq!(rsc.transition.from, Some(RunState::Paused));
                assert_eq!(rsc.transition.to, RunState::Running);
                assert!(rsc.resume_trigger.is_none());
            }
            _ => panic!("expected RunStateChanged"),
        }
    }

    // Issue #591 symmetry: `resume_trigger` threads through the same
    // conversion so the audit trail can distinguish timer-fired from
    // operator-initiated resumes.
    #[test]
    fn bridge_event_to_runtime_resumed_threads_resume_trigger() {
        let event = BridgeEvent::ExecutionResumed {
            run_id: RunId::new("run_1"),
            project: ProjectKey::new("t", "w", "p"),
            prev_state: Some(RunState::Paused),
            resume_trigger: Some(ResumeTrigger::ResumeAfterTimer),
        };
        match bridge_event_to_runtime_event(&event) {
            RuntimeEvent::RunStateChanged(rsc) => {
                assert_eq!(rsc.resume_trigger, Some(ResumeTrigger::ResumeAfterTimer));
            }
            _ => panic!("expected RunStateChanged"),
        }
    }

    #[test]
    fn bridge_event_prev_state_none_produces_none_from() {
        let event = BridgeEvent::ExecutionCompleted {
            run_id: RunId::new("run_1"),
            project: ProjectKey::new("t", "w", "p"),
            prev_state: None,
        };
        let runtime = bridge_event_to_runtime_event(&event);
        match runtime {
            RuntimeEvent::RunStateChanged(rsc) => {
                assert!(rsc.transition.from.is_none());
                assert_eq!(rsc.transition.to, RunState::Completed);
            }
            _ => panic!("expected RunStateChanged"),
        }
    }

    #[test]
    fn bridge_event_task_created() {
        let event = BridgeEvent::TaskCreated {
            task_id: TaskId::new("task_1"),
            project: ProjectKey::new("t", "w", "p"),
            session_id: Some(SessionId::new("sess_1")),
            parent_run_id: Some(RunId::new("run_1")),
            parent_task_id: None,
        };
        let runtime = bridge_event_to_runtime_event(&event);
        match runtime {
            RuntimeEvent::TaskCreated(tc) => {
                assert_eq!(tc.task_id.as_str(), "task_1");
                assert_eq!(tc.project.tenant_id.as_str(), "t");
                assert_eq!(tc.parent_run_id.as_ref().unwrap().as_str(), "run_1");
                assert!(tc.parent_task_id.is_none());
                assert!(tc.prompt_release_id.is_none());
            }
            _ => panic!("expected TaskCreated"),
        }
    }

    #[test]
    fn bridge_event_task_created_with_parent_task() {
        let event = BridgeEvent::TaskCreated {
            task_id: TaskId::new("task_child"),
            project: ProjectKey::new("t", "w", "p"),
            session_id: None,
            parent_run_id: Some(RunId::new("run_1")),
            parent_task_id: Some(TaskId::new("task_parent")),
        };
        let runtime = bridge_event_to_runtime_event(&event);
        match runtime {
            RuntimeEvent::TaskCreated(tc) => {
                assert_eq!(tc.task_id.as_str(), "task_child");
                assert_eq!(tc.parent_task_id.as_ref().unwrap().as_str(), "task_parent");
            }
            _ => panic!("expected TaskCreated"),
        }
    }

    #[test]
    fn bridge_event_task_state_changed_completed() {
        let event = BridgeEvent::TaskStateChanged {
            task_id: TaskId::new("task_3"),
            project: ProjectKey::new("t", "w", "p"),
            to: TaskState::Completed,
            failure_class: None,
        };
        let runtime = bridge_event_to_runtime_event(&event);
        match runtime {
            RuntimeEvent::TaskStateChanged(tsc) => {
                assert_eq!(tsc.task_id.as_str(), "task_3");
                assert_eq!(tsc.transition.to, TaskState::Completed);
                assert!(tsc.transition.from.is_none());
                assert!(tsc.failure_class.is_none());
            }
            _ => panic!("expected TaskStateChanged"),
        }
    }

    #[test]
    fn bridge_event_task_state_changed_failed_preserves_class() {
        let event = BridgeEvent::TaskStateChanged {
            task_id: TaskId::new("task_4"),
            project: ProjectKey::new("t", "w", "p"),
            to: TaskState::Failed,
            failure_class: Some(FailureClass::TimedOut),
        };
        let runtime = bridge_event_to_runtime_event(&event);
        match runtime {
            RuntimeEvent::TaskStateChanged(tsc) => {
                assert_eq!(tsc.transition.to, TaskState::Failed);
                assert_eq!(tsc.failure_class, Some(FailureClass::TimedOut));
            }
            _ => panic!("expected TaskStateChanged"),
        }
    }

    #[test]
    fn bridge_task_lease_claimed_uses_epoch_as_token() {
        let event = BridgeEvent::TaskLeaseClaimed {
            task_id: TaskId::new("task_1"),
            project: ProjectKey::new("t", "w", "p"),
            lease_owner: "worker_a".to_owned(),
            lease_epoch: 7,
            lease_expires_at_ms: 99_000,
        };
        let runtime = bridge_event_to_runtime_event(&event);
        match runtime {
            RuntimeEvent::TaskLeaseClaimed(tlc) => {
                assert_eq!(tlc.task_id.as_str(), "task_1");
                assert_eq!(tlc.lease_owner, "worker_a");
                assert_eq!(tlc.lease_token, 7);
                assert_eq!(tlc.lease_expires_at_ms, 99_000);
            }
            _ => panic!("expected TaskLeaseClaimed"),
        }
    }

    #[test]
    fn bridge_session_archived_emits_session_state_changed() {
        let event = BridgeEvent::SessionArchived {
            session_id: SessionId::new("sess_1"),
            project: ProjectKey::new("t", "w", "p"),
        };
        let runtime = bridge_event_to_runtime_event(&event);
        match runtime {
            RuntimeEvent::SessionStateChanged(ssc) => {
                assert_eq!(ssc.session_id.as_str(), "sess_1");
                assert_eq!(ssc.transition.from, Some(SessionState::Open));
                assert_eq!(ssc.transition.to, SessionState::Archived);
            }
            _ => panic!("expected SessionStateChanged, got {runtime:?}"),
        }
    }

    #[test]
    fn bridge_session_created_emits_session_created_envelope() {
        // SessionCreated must map to a SessionCreated envelope — handlers
        // that read a session by id before starting a run depend on this.
        let event = BridgeEvent::SessionCreated {
            session_id: SessionId::new("sess_brand_new"),
            project: ProjectKey::new("tenant_x", "workspace_y", "project_z"),
        };
        let runtime = bridge_event_to_runtime_event(&event);
        match runtime {
            RuntimeEvent::SessionCreated(sc) => {
                assert_eq!(sc.session_id.as_str(), "sess_brand_new");
                assert_eq!(sc.project.tenant_id.as_str(), "tenant_x");
                assert_eq!(sc.project.workspace_id.as_str(), "workspace_y");
                assert_eq!(sc.project.project_id.as_str(), "project_z");
            }
            _ => panic!("expected SessionCreated, got {runtime:?}"),
        }
    }

    #[tokio::test]
    async fn bridge_session_created_round_trips_through_event_log() {
        // End-to-end: start an EventBridge, emit SessionCreated, let the
        // consumer drain, verify SessionReadModel sees the projection.
        // Mirrors the existing TaskCreated / ExecutionCreated round-trip
        // tests above — proves the new variant reaches cairn-store so
        // FabricSessionServiceAdapter::get succeeds on the next request.
        use cairn_store::projections::SessionReadModel;
        use cairn_store::InMemoryStore;

        let store = Arc::new(InMemoryStore::new());
        let event_log: Arc<dyn EventLog + Send + Sync> = store.clone();
        let (bridge, handle) = EventBridge::start(event_log);

        let session_id = SessionId::new("sess_rt_1");
        let project = ProjectKey::new("t_rt", "w_rt", "p_rt");

        bridge
            .emit(BridgeEvent::SessionCreated {
                session_id: session_id.clone(),
                project: project.clone(),
            })
            .await;

        // Stop the bridge and wait for the consumer to drain. `stop`
        // cancels the loop AFTER processing everything already in the
        // channel (the loop uses `biased` select so recv drains before
        // the cancel arm fires).
        bridge.stop();
        let _ = handle.await;

        let record = SessionReadModel::get(store.as_ref(), &session_id)
            .await
            .expect("projection read must not error")
            .expect("SessionCreated must populate SessionReadModel");
        assert_eq!(record.session_id, session_id);
        assert_eq!(record.project, project);
    }

    // Issue #568 regression: without `flush`, a read immediately after
    // `emit` can miss its own event because the consumer runs on a
    // separate task. `flush` must guarantee every event `emit`ted
    // before the flush is visible to a subsequent store read.
    //
    // The test uses a store-head probe because the append-to-store
    // side of the bridge is the only observable artefact on the
    // cairn-store trait surface. If the assertion `head_before_flush
    // < head_after_flush` does not hold deterministically, the race
    // is still live.
    #[tokio::test]
    async fn flush_blocks_until_prior_emits_reach_store() {
        use cairn_store::InMemoryStore;

        let store = Arc::new(InMemoryStore::new());
        let event_log: Arc<dyn EventLog + Send + Sync> = store.clone();
        let (bridge, handle) = EventBridge::start(event_log);

        let head_before = store.head_position().await.expect("head read").map(|p| p.0);

        // Emit two events back-to-back. Without `flush` the consumer
        // may not have processed either by the time the next line
        // runs; `flush` must drain both before returning.
        bridge
            .emit(BridgeEvent::SessionCreated {
                session_id: SessionId::new("sess_flush_a"),
                project: ProjectKey::new("t", "w", "p"),
            })
            .await;
        bridge
            .emit(BridgeEvent::SessionCreated {
                session_id: SessionId::new("sess_flush_b"),
                project: ProjectKey::new("t", "w", "p"),
            })
            .await;

        bridge.flush().await;

        let head_after = store.head_position().await.expect("head read").map(|p| p.0);
        assert!(
            head_after > head_before,
            "flush must block until the event log head has advanced past the emitted events \
             (before={head_before:?}, after={head_after:?})"
        );

        // Both session projections must also be visible — flush is a
        // store-level barrier, not just a channel-drain barrier.
        use cairn_store::projections::SessionReadModel;
        for sid in ["sess_flush_a", "sess_flush_b"] {
            let record = SessionReadModel::get(store.as_ref(), &SessionId::new(sid))
                .await
                .expect("projection read");
            assert!(
                record.is_some(),
                "session {sid} must be visible in the projection after flush",
            );
        }

        bridge.stop();
        let _ = handle.await;
    }

    // Flush on a closed consumer must not hang — it must degrade to a
    // no-op. This proves the graceful-degradation contract documented
    // on `EventBridge::flush` so callers can reach publish after stop
    // without deadlocking the handler.
    #[tokio::test]
    async fn flush_after_stop_returns_immediately() {
        use cairn_store::InMemoryStore;

        let store = Arc::new(InMemoryStore::new());
        let event_log: Arc<dyn EventLog + Send + Sync> = store.clone();
        let (bridge, handle) = EventBridge::start(event_log);

        bridge.stop();
        let _ = handle.await;

        // If `flush` doesn't return within the timeout the degradation
        // contract is broken and callers will hang forever on a stopped
        // bridge.
        let result = tokio::time::timeout(std::time::Duration::from_secs(1), bridge.flush()).await;
        assert!(
            result.is_ok(),
            "flush must return (not hang) when the consumer channel is already closed"
        );
    }
}
