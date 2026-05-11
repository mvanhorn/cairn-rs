use crate::approvals::{ApprovalMatchPolicy, ApprovalScope};
use crate::errors::RuntimeEntityRef;
use crate::ids::{
    ApprovalId, CheckpointId, DecisionId, EvalRunId, EventId, IngestJobId, MailboxMessageId,
    OperatorId, OutcomeId, PromptAssetId, PromptReleaseId, PromptVersionId, ProviderBindingId,
    ProviderCallId, ProviderConnectionId, ProviderModelId, RouteAttemptId, RouteDecisionId, RunId,
    RunTemplateId, ScheduledTaskId, SessionId, SignalId, TaskId, TenantId, ToolCallId,
    ToolInvocationId, TriggerId, WorkspaceId,
};
use crate::lifecycle::{
    CheckpointDisposition, FailureClass, PauseReason, ResumeTrigger, RunState, SessionState,
    TaskState,
};
use crate::policy::{ApprovalDecision, ApprovalRequirement, ExecutionClass};
use crate::tenancy::{OwnershipKey, ProjectKey};
use crate::tool_invocation::{ToolInvocationOutcomeKind, ToolInvocationTarget};
use crate::workers::ExternalWorkerReport;
use serde::{Deserialize, Serialize};

/// Shared event envelope for canonical product events.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventEnvelope<T> {
    pub event_id: EventId,
    pub source: EventSource,
    pub ownership: OwnershipKey,
    pub causation_id: Option<crate::ids::CommandId>,
    pub correlation_id: Option<String>,
    pub payload: T,
}

impl<T> EventEnvelope<T> {
    pub fn new(
        event_id: impl Into<EventId>,
        source: EventSource,
        ownership: impl Into<OwnershipKey>,
        payload: T,
    ) -> Self {
        Self {
            event_id: event_id.into(),
            source,
            ownership: ownership.into(),
            causation_id: None,
            correlation_id: None,
            payload,
        }
    }

    pub fn with_causation_id(mut self, causation_id: impl Into<crate::ids::CommandId>) -> Self {
        self.causation_id = Some(causation_id.into());
        self
    }

    pub fn with_correlation_id(mut self, correlation_id: impl Into<String>) -> Self {
        self.correlation_id = Some(correlation_id.into());
        self
    }
}

impl EventEnvelope<RuntimeEvent> {
    pub fn for_runtime_event(
        event_id: impl Into<EventId>,
        source: EventSource,
        payload: RuntimeEvent,
    ) -> Self {
        let ownership = payload.project().clone();
        Self::new(event_id, source, ownership, payload)
    }

    pub fn project(&self) -> &ProjectKey {
        self.payload.project()
    }

    pub fn primary_entity_ref(&self) -> Option<RuntimeEntityRef> {
        self.payload.primary_entity_ref()
    }
}

/// Event source information used by runtime, operators, and workers.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "source_type", rename_all = "snake_case")]
pub enum EventSource {
    Operator { operator_id: crate::ids::OperatorId },
    Runtime,
    Scheduler,
    ExternalWorker { worker: String },
    System,
}

/// Minimal runtime event set used as the Week 1 shared contract.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum RuntimeEvent {
    SessionCreated(SessionCreated),
    SessionStateChanged(SessionStateChanged),
    RunCreated(RunCreated),
    RunStateChanged(RunStateChanged),
    TaskCreated(TaskCreated),
    TaskLeaseClaimed(TaskLeaseClaimed),
    TaskLeaseHeartbeated(TaskLeaseHeartbeated),
    TaskStateChanged(TaskStateChanged),
    ApprovalRequested(ApprovalRequested),
    ApprovalResolved(ApprovalResolved),
    /// PR BP-1: tool-call approval proposed (foundation — not yet
    /// emitted; legacy `ApprovalRequested` is still the live tool-call
    /// approval event).
    ToolCallProposed(ToolCallProposed),
    /// PR BP-1: tool-call approval granted (foundation — not yet emitted).
    ToolCallApproved(ToolCallApproved),
    /// PR BP-1: tool-call approval denied (foundation — not yet emitted).
    ToolCallRejected(ToolCallRejected),
    /// PR BP-1: operator amended proposed tool-call arguments before
    /// resolving (foundation — not yet emitted).
    ToolCallAmended(ToolCallAmended),
    CheckpointRecorded(CheckpointRecorded),
    CheckpointRestored(CheckpointRestored),
    MailboxMessageAppended(MailboxMessageAppended),
    ToolInvocationStarted(ToolInvocationStarted),
    ToolInvocationCompleted(ToolInvocationCompleted),
    ToolInvocationFailed(ToolInvocationFailed),
    /// RFC 020 Track 3: tool call served from `ToolCallResultCache` on replay.
    ToolInvocationCacheHit(ToolInvocationCacheHit),
    /// RFC 020 Track 3: recovery paused on a `DangerousPause` tool with no cached result.
    ToolRecoveryPaused(ToolRecoveryPaused),
    SignalIngested(SignalIngested),
    ExternalWorkerRegistered(ExternalWorkerRegistered),
    ExternalWorkerReported(ExternalWorkerReported),
    ExternalWorkerSuspended(ExternalWorkerSuspended),
    ExternalWorkerReactivated(ExternalWorkerReactivated),
    SubagentSpawned(SubagentSpawned),
    RecoveryAttempted(RecoveryAttempted),
    RecoveryCompleted(RecoveryCompleted),
    /// RFC 020 Track 4: once-per-boot aggregate summary of the recovery
    /// sweep. Emitted at the end of `RecoveryService::recover_all` with
    /// per-branch counts (runs, sandboxes, cache entries, …).
    RecoverySummaryEmitted(RecoverySummaryEmitted),
    UserMessageAppended(UserMessageAppended),
    IngestJobStarted(IngestJobStarted),
    IngestJobCompleted(IngestJobCompleted),
    EvalRunStarted(EvalRunStarted),
    EvalRunCompleted(EvalRunCompleted),
    /// Issue #244: eval run soft-deleted. The run record is preserved for
    /// audit/history; list endpoints filter it out by default.
    EvalRunArchived(EvalRunArchived),
    /// RFC-025 Phase 1 (#435): eval run metrics recorded via
    /// `POST /v1/evals/runs/:id/score`. Prior to this event the handler
    /// mutated `state.evals` in-process only, so scores were lost on
    /// restart. The event carries the full `EvalMetrics` struct so
    /// per-metric fields survive replay and are byte-equal across
    /// pg/sqlite/in-memory projections.
    EvalRunScored(EvalRunScored),
    /// RFC-025 Phase 1 (#435): rubric scoring recorded via
    /// `POST /v1/evals/runs/:id/rubric-score`. Carries the per-dimension
    /// weighted scores + overall weighted score so the projection can
    /// rebuild the rubric verdict after a process restart.
    EvalRubricScored(EvalRubricScored),
    PromptAssetCreated(PromptAssetCreated),
    PromptVersionCreated(PromptVersionCreated),
    ApprovalPolicyCreated(ApprovalPolicyCreated),
    PromptReleaseCreated(PromptReleaseCreated),
    PromptReleaseTransitioned(PromptReleaseTransitioned),
    /// RFC 001: gradual traffic rollout started.
    PromptRolloutStarted(PromptRolloutStarted),
    TenantCreated(TenantCreated),
    /// RFC 026 PR-A2: tenant PATCH edit (rename).
    TenantUpdated(TenantUpdated),
    WorkspaceCreated(WorkspaceCreated),
    WorkspaceArchived(WorkspaceArchived),
    ProjectCreated(ProjectCreated),
    RouteDecisionMade(RouteDecisionMade),
    ProviderCallCompleted(ProviderCallCompleted),
    /// Issue #668: post-redaction LLM round-trip body. Emitted alongside
    /// `ProviderCallCompleted` so operators can audit the actual prompt
    /// the LLM received + the response it returned, not just the
    /// token/cost metadata. See `LlmCompletionRecorded` for the body
    /// shape and `LlmPromptOutputReadModel` for the projection.
    LlmCompletionRecorded(LlmCompletionRecorded),
    /// #789: compacted per-iteration reasoning record. See
    /// `RunReasoningStep` for the body shape. Run-keyed, intended to
    /// power the live `/v1/admin/agents/live` view and the
    /// `/v1/runs/:id/trajectory` post-mortem replay endpoint without
    /// loading the full LLM body trace.
    RunReasoningStepRecorded(RunReasoningStep),
    SoulPatchProposed(SoulPatchProposed),
    SoulPatchApplied(SoulPatchApplied),
    /// GAP-006: session-level accumulated cost updated after a provider call.
    SessionCostUpdated(SessionCostUpdated),
    /// Run-level cost updated after a provider call.
    RunCostUpdated(RunCostUpdated),
    /// GAP-006: tenant-level spend alert triggered.
    SpendAlertTriggered(SpendAlertTriggered),
    ProviderBudgetSet(ProviderBudgetSet),
    ChannelCreated(ChannelCreated),
    ChannelMessageSent(ChannelMessageSent),
    ChannelMessageConsumed(ChannelMessageConsumed),
    DefaultSettingSet(DefaultSettingSet),
    DefaultSettingCleared(DefaultSettingCleared),
    LicenseActivated(LicenseActivated),
    EntitlementOverrideSet(EntitlementOverrideSet),
    NotificationPreferenceSet(NotificationPreferenceSet),
    NotificationSent(NotificationSent),
    ProviderPoolCreated(ProviderPoolCreated),
    ProviderPoolConnectionAdded(ProviderPoolConnectionAdded),
    ProviderPoolConnectionRemoved(ProviderPoolConnectionRemoved),
    TenantQuotaSet(TenantQuotaSet),
    TenantQuotaViolated(TenantQuotaViolated),
    RetentionPolicySet(RetentionPolicySet),
    RunCostAlertSet(RunCostAlertSet),
    RunCostAlertTriggered(RunCostAlertTriggered),
    WorkspaceMemberAdded(WorkspaceMemberAdded),
    WorkspaceMemberRemoved(WorkspaceMemberRemoved),
    // ── Second-wave events ──────────────────────────────────────────────────
    ApprovalDelegated(ApprovalDelegated),
    AuditLogEntryRecorded(AuditLogEntryRecorded),
    CheckpointStrategySet(CheckpointStrategySet),
    CredentialKeyRotated(CredentialKeyRotated),
    CredentialRevoked(CredentialRevoked),
    CredentialStored(CredentialStored),
    EvalBaselineLocked(EvalBaselineLocked),
    EvalBaselineSet(EvalBaselineSet),
    EvalDatasetCreated(EvalDatasetCreated),
    EvalDatasetEntryAdded(EvalDatasetEntryAdded),
    EvalRubricCreated(EvalRubricCreated),
    EventLogCompacted(EventLogCompacted),
    GuardrailPolicyCreated(GuardrailPolicyCreated),
    GuardrailPolicyEvaluated(GuardrailPolicyEvaluated),
    OperatorIntervention(OperatorIntervention),
    OperatorProfileCreated(OperatorProfileCreated),
    OperatorProfileUpdated(OperatorProfileUpdated),
    /// RFC 026 PR-A0: tenant-admin role grant — upserts
    /// `operator_tenant_roles` keyed on `(tenant_id, operator_id)`.
    TenantRoleGranted(TenantRoleGranted),
    /// RFC 026 PR-A0: tenant-admin role revocation — marks the row
    /// revoked (not deleted; the audit trail survives).
    TenantRoleRevoked(TenantRoleRevoked),
    PauseScheduled(PauseScheduled),
    PermissionDecisionRecorded(PermissionDecisionRecorded),
    ProviderBindingCreated(ProviderBindingCreated),
    ProviderBindingStateChanged(ProviderBindingStateChanged),
    ProviderBudgetAlertTriggered(ProviderBudgetAlertTriggered),
    ProviderBudgetExceeded(ProviderBudgetExceeded),
    ProviderConnectionRegistered(ProviderConnectionRegistered),
    /// Operator removed a provider connection. The projection hard-removes
    /// the row so the ID can be re-used; full history remains in the event
    /// log. F40.
    ProviderConnectionDeleted(ProviderConnectionDeleted),
    ProviderHealthChecked(ProviderHealthChecked),
    ProviderHealthScheduleSet(ProviderHealthScheduleSet),
    ProviderHealthScheduleTriggered(ProviderHealthScheduleTriggered),
    ProviderMarkedDegraded(ProviderMarkedDegraded),
    ProviderModelRegistered(ProviderModelRegistered),
    ProviderRecovered(ProviderRecovered),
    ProviderRetryPolicySet(ProviderRetryPolicySet),
    RecoveryEscalated(RecoveryEscalated),
    ResourceShareRevoked(ResourceShareRevoked),
    ResourceShared(ResourceShared),
    RoutePolicyCreated(RoutePolicyCreated),
    RoutePolicyUpdated(RoutePolicyUpdated),
    RunSlaBreached(RunSlaBreached),
    RunSlaSet(RunSlaSet),
    SignalRouted(SignalRouted),
    SignalSubscriptionCreated(SignalSubscriptionCreated),
    TriggerCreated(TriggerCreated),
    TriggerEnabled(TriggerEnabled),
    TriggerDisabled(TriggerDisabled),
    TriggerSuspended(TriggerSuspended),
    TriggerResumed(TriggerResumed),
    TriggerDeleted(TriggerDeleted),
    TriggerFired(TriggerFired),
    TriggerSkipped(TriggerSkipped),
    TriggerDenied(TriggerDenied),
    TriggerRateLimited(TriggerRateLimited),
    TriggerPendingApproval(TriggerPendingApproval),
    RunTemplateCreated(RunTemplateCreated),
    RunTemplateDeleted(RunTemplateDeleted),
    SnapshotCreated(SnapshotCreated),
    TaskDependencyAdded(TaskDependencyAdded),
    TaskDependencyResolved(TaskDependencyResolved),
    TaskLeaseExpired(TaskLeaseExpired),
    TaskPriorityChanged(TaskPriorityChanged),
    ToolInvocationProgressUpdated(ToolInvocationProgressUpdated),
    /// Evaluator–optimizer feedback loop: agents record observed outcomes
    /// so downstream eval pipelines can compare against expected outcomes.
    OutcomeRecorded(OutcomeRecorded),
    /// A tenant-scoped scheduled task was registered.
    ScheduledTaskCreated(ScheduledTaskCreated),
    // ── Plan review events (RFC 018) ──────────────────────────────────────
    /// A Plan-mode run emitted a `<proposed_plan>` artifact.
    PlanProposed(PlanProposed),
    /// An operator approved a plan artifact; next step is creating an Execute run.
    PlanApproved(PlanApproved),
    /// An operator rejected a plan artifact.
    PlanRejected(PlanRejected),
    /// An operator requested a revision; a new Plan-mode run was created.
    PlanRevisionRequested(PlanRevisionRequested),
    // ── Decision layer persistence (RFC 019 + RFC 020) ────────────────────
    /// A decision was evaluated and (optionally) cached. Durable audit
    /// record that lets the decision cache projection be rebuilt on
    /// startup — closes the RFC 020 §"Decision Cache Survival" gap.
    DecisionRecorded(DecisionRecorded),
    /// Startup replay completed for the decision cache. Counts how many
    /// entries were restored and how many were dropped because their TTL
    /// had already expired at replay time.
    DecisionCacheWarmup(DecisionCacheWarmup),
    /// F47 PR2: run completion annotated with the LLM's free-text summary
    /// and the extractor-produced `CompletionVerification` sidecar.
    ///
    /// Emitted AFTER the orchestrator loop returns `LoopTermination::Completed`
    /// and after `runs.complete` has flipped the run to the terminal state.
    /// This event does not drive state transitions — `RunStateChanged` is
    /// still the authority. It purely annotates the already-terminal run
    /// with the truth-vs-claim gap so operators can inspect the evidence
    /// on a run detail page after the SSE stream is gone.
    ///
    /// Replay-safe: the projection stores summary + verification on
    /// nullable columns, so event logs written before F47 PR2 deserialize
    /// cleanly and surface `completion: None` at the REST boundary.
    RunCompletionAnnotated(RunCompletionAnnotated),
    /// RFC 032 §2.3: emitted when a run's [`CompletionContract`]
    /// resolves — either declared explicitly on run creation / spawn,
    /// or inferred from goal text at first orchestrate boot, or
    /// re-inferred after a goal-text change mid-run.
    ///
    /// Carries the resolved contract + source so the operator
    /// trajectory endpoint (#794) can render "what is cairn grading
    /// this run against" well before the gate ever fires. PR-2 lands
    /// the variant; PR-4 wires emission.
    ///
    /// Ephemeral in PR-2 (no read-model table yet); PR-4 may upgrade
    /// to `Projected` against a dedicated `completion_contracts`
    /// table. See `projection_registry.rs`.
    CompletionContractResolved(CompletionContractResolved),
    /// F64: emitted when the cairn-side terminal-write recovery loop
    /// runs (the bridge workaround for the FF#371 dual-door deadlock).
    /// Carries the attempts, wall-time, and outcome so operators can
    /// see whether recovery saved the run or whether F62's
    /// `TerminalWriteDeadlock` fallback fired.
    ///
    /// Once FF#371 lands upstream, the active cairn-side recovery loop
    /// retires — no new events are emitted — but the variant itself
    /// stays on `RuntimeEvent` (and its projection on `runs`) so
    /// historical event logs + audit rows continue to deserialize
    /// cleanly. Do NOT remove this variant when the upstream fix
    /// ships; deleting it would break replay of any log containing
    /// legacy recovery attempts.
    TerminalRecoveryAttempted(TerminalRecoveryAttempted),
    /// F65 PR-1: orchestrator session redesign foundation. The variants
    /// below carry durable observability of session attempts, breaker trips,
    /// checkpoint + workspace-snapshot lifecycle, and the rich terminal
    /// session outcome. PR-1 only adds the shapes — they are not emitted
    /// from the runtime yet; PR-2/3/6 wire emission.
    SessionAttemptStarted(SessionAttemptStarted),
    SessionAttemptCompleted(SessionAttemptCompleted),
    CircuitBreakerTripped(CircuitBreakerTripped),
    BudgetThresholdCrossed(BudgetThresholdCrossed),
    CheckpointPersisted(CheckpointPersisted),
    WorkspaceSnapshotCreated(WorkspaceSnapshotCreated),
    WorkspaceSnapshotReaped(WorkspaceSnapshotReaped),
    SessionOutcomeEmitted(SessionOutcomeEmitted),
    OrchestratorDecisionMade(OrchestratorDecisionMade),
    SummarizerFallback(SummarizerFallback),
    WorkspaceBackendDegraded(WorkspaceBackendDegraded),
    /// F65 PR-5 (#359): crash-recovery sweep successfully unmounted a
    /// dangling overlay mount the previous cairn-app process left behind.
    SandboxCrashRecovered(SandboxCrashRecovered),

    // ── RFC 029 pluggable knowledge providers ──
    /// Project configured or re-configured its knowledge provider.
    /// Upserts a row on `project_knowledge_providers` with `kind = "configured"`.
    KnowledgeProviderConfigured(KnowledgeProviderConfigured),
    /// Query-time failure: the configured provider is not reachable /
    /// spawned / authorized. Audit row on `project_knowledge_providers`
    /// (`kind = "unavailable"`), never overwrites the `configured` row.
    KnowledgeProviderUnavailable(KnowledgeProviderUnavailable),
    /// Plugin restart produced a handshake snapshot that differs from the
    /// previous spawn. Audit row (`kind = "capability_changed"`).
    KnowledgeProviderCapabilityChanged(KnowledgeProviderCapabilityChanged),
    /// Knowledge-document ingest kicked off (cairn-default or plugin).
    /// Row on `knowledge_ingest_jobs`.
    KnowledgeIngestSubmitted(KnowledgeIngestSubmitted),
    /// Ingest refused before dispatch (e.g., read-only provider).
    /// Row on `knowledge_ingest_jobs`.
    KnowledgeIngestRejected(KnowledgeIngestRejected),
    /// Ingest status transition reported by the provider.
    /// Updates the row on `knowledge_ingest_jobs`.
    KnowledgeIngestStatusUpdated(KnowledgeIngestStatusUpdated),

    // ── RFC 030 pluggable memory providers ──
    /// Project configured or re-configured its memory provider. Upserts a
    /// row on `project_memory_providers` with `kind = "configured"`.
    MemoryProviderConfigured(MemoryProviderConfigured),
    /// Query-time failure: the configured memory provider is not reachable
    /// / spawned / authorized. Audit row on `project_memory_providers`
    /// (`kind = "unavailable"`), never overwrites the `configured` row.
    MemoryProviderUnavailable(MemoryProviderUnavailable),
    /// Plugin restart produced a handshake snapshot that differs from the
    /// previous spawn. Audit row (`kind = "capability_changed"`).
    MemoryProviderCapabilityChanged(MemoryProviderCapabilityChanged),
    /// Memory-document ingest kicked off (cairn-default or plugin).
    /// Row on `memory_ingest_jobs`.
    MemoryIngestSubmitted(MemoryIngestSubmitted),
    /// Ingest refused before dispatch (e.g., auto_extract provider).
    /// Row on `memory_ingest_jobs`.
    MemoryIngestRejected(MemoryIngestRejected),
    /// Ingest status transition reported by the memory provider.
    /// Updates the row on `memory_ingest_jobs`.
    MemoryIngestStatusUpdated(MemoryIngestStatusUpdated),

    /// RFC 030 §Rollout: the startup family-mismatch scan found a
    /// project whose knowledge-slot provider advertised memory-family
    /// semantics at handshake (or vice versa). Emitted once per
    /// offending project per boot. Feeds the operator-health badge +
    /// gives the operator a pointer to reconfigure the slot.
    ///
    /// Audit-only: the event does not change the configured
    /// provider_ref — operators must PUT the correct family-specific
    /// endpoint (`/v1/projects/:p/memory-provider` /
    /// `.../knowledge-provider`).
    KnowledgeProviderFamilyMismatch(KnowledgeProviderFamilyMismatch),
    /// Memory-slot twin of `KnowledgeProviderFamilyMismatch`.
    MemoryProviderFamilyMismatch(MemoryProviderFamilyMismatch),

    // ── RFC 031 operator-defined agent roles ──
    /// An operator defined or updated a per-project agent role.
    /// Upserts a row on `project_agent_roles`; latest-wins per
    /// `(project_key, role_id)`. See RFC 031 §D6.
    AgentRoleDefined(AgentRoleDefined),
    /// An operator retracted a per-project agent role. Sets
    /// `retracted_at` on the `project_agent_roles` row; resolve
    /// falls back to the built-in or generic. See RFC 031 §D7.
    AgentRoleRetracted(AgentRoleRetracted),
    /// Observability: the orchestrator's allowlist filter found a
    /// tool id declared in `role.tools` that is not currently
    /// registered. Ephemeral — not projected; deduped per
    /// `(run_id, role_id, tool_id)` on the run's
    /// `OrchestrationContext::declared_but_missing` HashSet. See
    /// RFC 031 §D3 + §Runtime Resolution Delta.
    ToolDeclaredButMissing(ToolDeclaredButMissing),
}

impl RuntimeEvent {
    pub fn project(&self) -> &ProjectKey {
        match self {
            RuntimeEvent::SessionCreated(event) => &event.project,
            RuntimeEvent::SessionStateChanged(event) => &event.project,
            RuntimeEvent::RunCreated(event) => &event.project,
            RuntimeEvent::RunStateChanged(event) => &event.project,
            RuntimeEvent::TaskCreated(event) => &event.project,
            RuntimeEvent::TaskLeaseClaimed(event) => &event.project,
            RuntimeEvent::TaskLeaseHeartbeated(event) => &event.project,
            RuntimeEvent::TaskStateChanged(event) => &event.project,
            RuntimeEvent::ApprovalRequested(event) => &event.project,
            RuntimeEvent::ApprovalResolved(event) => &event.project,
            RuntimeEvent::ToolCallProposed(event) => &event.project,
            RuntimeEvent::ToolCallApproved(event) => &event.project,
            RuntimeEvent::ToolCallRejected(event) => &event.project,
            RuntimeEvent::ToolCallAmended(event) => &event.project,
            RuntimeEvent::CheckpointRecorded(event) => &event.project,
            RuntimeEvent::CheckpointRestored(event) => &event.project,
            RuntimeEvent::MailboxMessageAppended(event) => &event.project,
            RuntimeEvent::ToolInvocationStarted(event) => &event.project,
            RuntimeEvent::ToolInvocationCompleted(event) => &event.project,
            RuntimeEvent::ToolInvocationFailed(event) => &event.project,
            RuntimeEvent::ToolInvocationCacheHit(event) => &event.project,
            RuntimeEvent::ToolRecoveryPaused(event) => &event.project,
            RuntimeEvent::SignalIngested(event) => &event.project,
            RuntimeEvent::ExternalWorkerRegistered(event) => &event.sentinel_project,
            RuntimeEvent::ExternalWorkerReported(event) => &event.report.project,
            RuntimeEvent::ExternalWorkerSuspended(event) => &event.sentinel_project,
            RuntimeEvent::ExternalWorkerReactivated(event) => &event.sentinel_project,
            RuntimeEvent::SubagentSpawned(event) => &event.project,
            RuntimeEvent::RecoveryAttempted(event) => &event.project,
            RuntimeEvent::RecoveryCompleted(event) => &event.project,
            RuntimeEvent::RecoverySummaryEmitted(event) => &event.sentinel_project,
            RuntimeEvent::UserMessageAppended(event) => &event.project,
            RuntimeEvent::IngestJobStarted(event) => &event.project,
            RuntimeEvent::IngestJobCompleted(event) => &event.project,
            RuntimeEvent::EvalRunStarted(event) => &event.project,
            RuntimeEvent::EvalRunCompleted(event) => &event.project,
            RuntimeEvent::EvalRunArchived(event) => &event.project,
            RuntimeEvent::EvalRunScored(event) => &event.project,
            RuntimeEvent::EvalRubricScored(event) => &event.project,
            RuntimeEvent::PromptAssetCreated(event) => &event.project,
            RuntimeEvent::PromptVersionCreated(event) => &event.project,
            RuntimeEvent::ApprovalPolicyCreated(event) => &event.project,
            RuntimeEvent::PromptReleaseCreated(event) => &event.project,
            RuntimeEvent::PromptReleaseTransitioned(event) => &event.project,
            RuntimeEvent::PromptRolloutStarted(event) => &event.project,
            RuntimeEvent::TenantCreated(event) => &event.project,
            RuntimeEvent::TenantUpdated(event) => &event.project,
            RuntimeEvent::WorkspaceCreated(event) => &event.project,
            RuntimeEvent::WorkspaceArchived(event) => &event.project,
            RuntimeEvent::ProjectCreated(event) => &event.project,
            RuntimeEvent::RouteDecisionMade(event) => &event.project,
            RuntimeEvent::ProviderCallCompleted(event) => &event.project,
            RuntimeEvent::LlmCompletionRecorded(event) => &event.project,
            RuntimeEvent::RunReasoningStepRecorded(event) => &event.project,
            RuntimeEvent::SoulPatchProposed(event) => &event.project,
            RuntimeEvent::SoulPatchApplied(event) => &event.project,
            RuntimeEvent::SessionCostUpdated(event) => &event.project,
            RuntimeEvent::RunCostUpdated(event) => &event.project,
            RuntimeEvent::SpendAlertTriggered(event) => &event.project,
            RuntimeEvent::OutcomeRecorded(event) => &event.project,
            RuntimeEvent::PlanProposed(event) => &event.project,
            RuntimeEvent::PlanApproved(event) => &event.project,
            RuntimeEvent::PlanRejected(event) => &event.project,
            RuntimeEvent::PlanRevisionRequested(event) => &event.project,
            RuntimeEvent::DecisionRecorded(event) => &event.project,
            RuntimeEvent::RunCompletionAnnotated(event) => &event.project,
            RuntimeEvent::CompletionContractResolved(event) => &event.project,
            RuntimeEvent::TerminalRecoveryAttempted(event) => &event.project,
            // F65 PR-1: orchestrator session redesign events are all
            // project-scoped — they route through the operator-facing
            // session UI and cost rollups.
            RuntimeEvent::SessionAttemptStarted(event) => &event.project,
            RuntimeEvent::SessionAttemptCompleted(event) => &event.project,
            RuntimeEvent::CircuitBreakerTripped(event) => &event.project,
            RuntimeEvent::BudgetThresholdCrossed(event) => &event.project,
            RuntimeEvent::CheckpointPersisted(event) => &event.project,
            RuntimeEvent::WorkspaceSnapshotCreated(event) => &event.project,
            RuntimeEvent::WorkspaceSnapshotReaped(event) => &event.project,
            RuntimeEvent::SessionOutcomeEmitted(event) => &event.project,
            RuntimeEvent::OrchestratorDecisionMade(event) => &event.project,
            RuntimeEvent::SummarizerFallback(event) => &event.project,
            RuntimeEvent::WorkspaceBackendDegraded(event) => &event.project,
            RuntimeEvent::SandboxCrashRecovered(event) => &event.project,
            RuntimeEvent::KnowledgeProviderConfigured(event) => &event.project,
            RuntimeEvent::KnowledgeProviderUnavailable(event) => &event.project,
            RuntimeEvent::KnowledgeProviderCapabilityChanged(event) => &event.project,
            RuntimeEvent::KnowledgeIngestSubmitted(event) => &event.project,
            RuntimeEvent::KnowledgeIngestRejected(event) => &event.project,
            RuntimeEvent::KnowledgeIngestStatusUpdated(event) => &event.project,
            RuntimeEvent::MemoryProviderConfigured(event) => &event.project,
            RuntimeEvent::MemoryProviderUnavailable(event) => &event.project,
            RuntimeEvent::MemoryProviderCapabilityChanged(event) => &event.project,
            RuntimeEvent::MemoryIngestSubmitted(event) => &event.project,
            RuntimeEvent::MemoryIngestRejected(event) => &event.project,
            RuntimeEvent::MemoryIngestStatusUpdated(event) => &event.project,
            RuntimeEvent::KnowledgeProviderFamilyMismatch(event) => &event.project,
            RuntimeEvent::MemoryProviderFamilyMismatch(event) => &event.project,
            // RFC 031 operator-defined agent roles
            RuntimeEvent::AgentRoleDefined(event) => &event.project,
            RuntimeEvent::AgentRoleRetracted(event) => &event.project,
            RuntimeEvent::ToolDeclaredButMissing(event) => &event.project,
            RuntimeEvent::TriggerCreated(event) => &event.project,
            RuntimeEvent::TriggerEnabled(event) => &event.project,
            RuntimeEvent::TriggerDisabled(event) => &event.project,
            RuntimeEvent::TriggerSuspended(event) => &event.project,
            RuntimeEvent::TriggerResumed(event) => &event.project,
            RuntimeEvent::TriggerDeleted(event) => &event.project,
            RuntimeEvent::TriggerFired(event) => &event.project,
            RuntimeEvent::TriggerSkipped(event) => &event.project,
            RuntimeEvent::TriggerDenied(event) => &event.project,
            RuntimeEvent::TriggerRateLimited(event) => &event.project,
            RuntimeEvent::TriggerPendingApproval(event) => &event.project,
            RuntimeEvent::RunTemplateCreated(event) => &event.project,
            RuntimeEvent::RunTemplateDeleted(event) => &event.project,
            RuntimeEvent::ProviderBudgetSet(_)
            | RuntimeEvent::ChannelCreated(_)
            | RuntimeEvent::ChannelMessageSent(_)
            | RuntimeEvent::ChannelMessageConsumed(_)
            | RuntimeEvent::DefaultSettingSet(_)
            | RuntimeEvent::DefaultSettingCleared(_)
            | RuntimeEvent::LicenseActivated(_)
            | RuntimeEvent::EntitlementOverrideSet(_)
            | RuntimeEvent::NotificationPreferenceSet(_)
            | RuntimeEvent::NotificationSent(_)
            | RuntimeEvent::ProviderPoolCreated(_)
            | RuntimeEvent::ProviderPoolConnectionAdded(_)
            | RuntimeEvent::ProviderPoolConnectionRemoved(_)
            | RuntimeEvent::TenantQuotaSet(_)
            | RuntimeEvent::TenantQuotaViolated(_)
            | RuntimeEvent::RetentionPolicySet(_)
            | RuntimeEvent::RunCostAlertSet(_)
            | RuntimeEvent::RunCostAlertTriggered(_)
            | RuntimeEvent::WorkspaceMemberAdded(_)
            | RuntimeEvent::WorkspaceMemberRemoved(_)
            | RuntimeEvent::ApprovalDelegated(_)
            | RuntimeEvent::AuditLogEntryRecorded(_)
            | RuntimeEvent::CheckpointStrategySet(_)
            | RuntimeEvent::CredentialKeyRotated(_)
            | RuntimeEvent::CredentialRevoked(_)
            | RuntimeEvent::CredentialStored(_)
            | RuntimeEvent::EvalBaselineLocked(_)
            | RuntimeEvent::EvalBaselineSet(_)
            | RuntimeEvent::EvalDatasetCreated(_)
            | RuntimeEvent::EvalDatasetEntryAdded(_)
            | RuntimeEvent::EvalRubricCreated(_)
            | RuntimeEvent::EventLogCompacted(_)
            | RuntimeEvent::GuardrailPolicyCreated(_)
            | RuntimeEvent::GuardrailPolicyEvaluated(_)
            | RuntimeEvent::OperatorIntervention(_)
            | RuntimeEvent::OperatorProfileCreated(_)
            | RuntimeEvent::OperatorProfileUpdated(_)
            | RuntimeEvent::TenantRoleGranted(_)
            | RuntimeEvent::TenantRoleRevoked(_)
            | RuntimeEvent::PauseScheduled(_)
            | RuntimeEvent::PermissionDecisionRecorded(_)
            | RuntimeEvent::ProviderBindingCreated(_)
            | RuntimeEvent::ProviderBindingStateChanged(_)
            | RuntimeEvent::ProviderBudgetAlertTriggered(_)
            | RuntimeEvent::ProviderBudgetExceeded(_)
            | RuntimeEvent::ProviderConnectionRegistered(_)
            | RuntimeEvent::ProviderConnectionDeleted(_)
            | RuntimeEvent::ProviderHealthChecked(_)
            | RuntimeEvent::ProviderHealthScheduleSet(_)
            | RuntimeEvent::ProviderHealthScheduleTriggered(_)
            | RuntimeEvent::ProviderMarkedDegraded(_)
            | RuntimeEvent::ProviderModelRegistered(_)
            | RuntimeEvent::ProviderRecovered(_)
            | RuntimeEvent::ProviderRetryPolicySet(_)
            | RuntimeEvent::RecoveryEscalated(_)
            | RuntimeEvent::ResourceShareRevoked(_)
            | RuntimeEvent::ResourceShared(_)
            | RuntimeEvent::RoutePolicyCreated(_)
            | RuntimeEvent::RoutePolicyUpdated(_)
            | RuntimeEvent::RunSlaBreached(_)
            | RuntimeEvent::RunSlaSet(_)
            | RuntimeEvent::SignalRouted(_)
            | RuntimeEvent::SignalSubscriptionCreated(_)
            | RuntimeEvent::SnapshotCreated(_)
            | RuntimeEvent::TaskDependencyAdded(_)
            | RuntimeEvent::TaskDependencyResolved(_)
            | RuntimeEvent::TaskLeaseExpired(_)
            | RuntimeEvent::TaskPriorityChanged(_)
            | RuntimeEvent::ToolInvocationProgressUpdated(_)
            | RuntimeEvent::ScheduledTaskCreated(_)
            | RuntimeEvent::DecisionCacheWarmup(_) => {
                // These events are tenant-scoped rather than project-scoped.
                // Return a static placeholder key.
                static SYSTEM_KEY: std::sync::OnceLock<crate::tenancy::ProjectKey> =
                    std::sync::OnceLock::new();
                SYSTEM_KEY.get_or_init(|| {
                    crate::tenancy::ProjectKey::new(
                        crate::ids::TenantId::new("_system"),
                        crate::ids::WorkspaceId::new("_system"),
                        "_system".to_owned(),
                    )
                })
            }
        }
    }

    pub fn primary_entity_ref(&self) -> Option<RuntimeEntityRef> {
        match self {
            RuntimeEvent::SessionCreated(event) => Some(RuntimeEntityRef::Session {
                session_id: event.session_id.clone(),
            }),
            RuntimeEvent::SessionStateChanged(event) => Some(RuntimeEntityRef::Session {
                session_id: event.session_id.clone(),
            }),
            RuntimeEvent::RunCreated(event) => Some(RuntimeEntityRef::Run {
                run_id: event.run_id.clone(),
            }),
            RuntimeEvent::RunStateChanged(event) => Some(RuntimeEntityRef::Run {
                run_id: event.run_id.clone(),
            }),
            RuntimeEvent::TaskCreated(event) => Some(RuntimeEntityRef::Task {
                task_id: event.task_id.clone(),
            }),
            RuntimeEvent::TaskLeaseClaimed(event) => Some(RuntimeEntityRef::Task {
                task_id: event.task_id.clone(),
            }),
            RuntimeEvent::TaskLeaseHeartbeated(event) => Some(RuntimeEntityRef::Task {
                task_id: event.task_id.clone(),
            }),
            RuntimeEvent::TaskStateChanged(event) => Some(RuntimeEntityRef::Task {
                task_id: event.task_id.clone(),
            }),
            RuntimeEvent::ApprovalRequested(event) => Some(RuntimeEntityRef::Approval {
                approval_id: event.approval_id.clone(),
            }),
            RuntimeEvent::ApprovalResolved(event) => Some(RuntimeEntityRef::Approval {
                approval_id: event.approval_id.clone(),
            }),
            // PR BP-1: ToolCallId is not (yet) a `RuntimeEntityRef`
            // variant — returning `None` here keeps the foundation
            // additive. A follow-up PR that introduces projection state
            // will extend `RuntimeEntityRef` and update these arms.
            RuntimeEvent::ToolCallProposed(_) => None,
            RuntimeEvent::ToolCallApproved(_) => None,
            RuntimeEvent::ToolCallRejected(_) => None,
            RuntimeEvent::ToolCallAmended(_) => None,
            RuntimeEvent::CheckpointRecorded(event) => Some(RuntimeEntityRef::Checkpoint {
                checkpoint_id: event.checkpoint_id.clone(),
            }),
            RuntimeEvent::CheckpointRestored(event) => Some(RuntimeEntityRef::Checkpoint {
                checkpoint_id: event.checkpoint_id.clone(),
            }),
            RuntimeEvent::MailboxMessageAppended(event) => Some(RuntimeEntityRef::MailboxMessage {
                message_id: event.message_id.clone(),
            }),
            RuntimeEvent::ToolInvocationStarted(event) => Some(RuntimeEntityRef::ToolInvocation {
                invocation_id: event.invocation_id.clone(),
            }),
            RuntimeEvent::ToolInvocationCompleted(event) => {
                Some(RuntimeEntityRef::ToolInvocation {
                    invocation_id: event.invocation_id.clone(),
                })
            }
            RuntimeEvent::ToolInvocationFailed(event) => Some(RuntimeEntityRef::ToolInvocation {
                invocation_id: event.invocation_id.clone(),
            }),
            RuntimeEvent::ToolInvocationCacheHit(event) => Some(RuntimeEntityRef::ToolInvocation {
                invocation_id: event.invocation_id.clone(),
            }),
            RuntimeEvent::ToolRecoveryPaused(event) => Some(RuntimeEntityRef::Run {
                run_id: event.run_id.clone(),
            }),
            RuntimeEvent::SignalIngested(event) => Some(RuntimeEntityRef::Signal {
                signal_id: event.signal_id.clone(),
            }),
            RuntimeEvent::ExternalWorkerRegistered(_) => None,
            RuntimeEvent::ExternalWorkerReported(event) => Some(RuntimeEntityRef::Task {
                task_id: event.report.task_id.clone(),
            }),
            RuntimeEvent::ExternalWorkerSuspended(_) => None,
            RuntimeEvent::ExternalWorkerReactivated(_) => None,
            RuntimeEvent::SubagentSpawned(event) => Some(RuntimeEntityRef::Task {
                task_id: event.child_task_id.clone(),
            }),
            RuntimeEvent::RecoveryAttempted(event) => event
                .task_id
                .clone()
                .map(|task_id| RuntimeEntityRef::Task { task_id })
                .or_else(|| {
                    event
                        .run_id
                        .clone()
                        .map(|run_id| RuntimeEntityRef::Run { run_id })
                }),
            RuntimeEvent::RecoveryCompleted(event) => event
                .task_id
                .clone()
                .map(|task_id| RuntimeEntityRef::Task { task_id })
                .or_else(|| {
                    event
                        .run_id
                        .clone()
                        .map(|run_id| RuntimeEntityRef::Run { run_id })
                }),
            RuntimeEvent::UserMessageAppended(event) => Some(RuntimeEntityRef::Run {
                run_id: event.run_id.clone(),
            }),
            RuntimeEvent::IngestJobStarted(event) => Some(RuntimeEntityRef::IngestJob {
                job_id: event.job_id.clone(),
            }),
            RuntimeEvent::IngestJobCompleted(event) => Some(RuntimeEntityRef::IngestJob {
                job_id: event.job_id.clone(),
            }),
            RuntimeEvent::EvalRunStarted(event) => Some(RuntimeEntityRef::EvalRun {
                eval_run_id: event.eval_run_id.clone(),
            }),
            RuntimeEvent::EvalRunCompleted(event) => Some(RuntimeEntityRef::EvalRun {
                eval_run_id: event.eval_run_id.clone(),
            }),
            RuntimeEvent::EvalRunArchived(event) => Some(RuntimeEntityRef::EvalRun {
                eval_run_id: event.eval_run_id.clone(),
            }),
            RuntimeEvent::EvalRunScored(event) => Some(RuntimeEntityRef::EvalRun {
                eval_run_id: event.eval_run_id.clone(),
            }),
            RuntimeEvent::EvalRubricScored(event) => Some(RuntimeEntityRef::EvalRun {
                eval_run_id: event.eval_run_id.clone(),
            }),
            RuntimeEvent::OutcomeRecorded(event) => Some(RuntimeEntityRef::Run {
                run_id: event.run_id.clone(),
            }),
            RuntimeEvent::PlanProposed(event) => Some(RuntimeEntityRef::Run {
                run_id: event.plan_run_id.clone(),
            }),
            RuntimeEvent::PlanApproved(event) => Some(RuntimeEntityRef::Run {
                run_id: event.plan_run_id.clone(),
            }),
            RuntimeEvent::PlanRejected(event) => Some(RuntimeEntityRef::Run {
                run_id: event.plan_run_id.clone(),
            }),
            RuntimeEvent::PlanRevisionRequested(event) => Some(RuntimeEntityRef::Run {
                run_id: event.original_plan_run_id.clone(),
            }),
            RuntimeEvent::PromptAssetCreated(event) => Some(RuntimeEntityRef::PromptAsset {
                prompt_asset_id: event.prompt_asset_id.clone(),
            }),
            RuntimeEvent::PromptVersionCreated(event) => Some(RuntimeEntityRef::PromptVersion {
                prompt_version_id: event.prompt_version_id.clone(),
            }),
            RuntimeEvent::ApprovalPolicyCreated(_) => None,
            RuntimeEvent::PromptReleaseCreated(event) => Some(RuntimeEntityRef::PromptRelease {
                prompt_release_id: event.prompt_release_id.clone(),
            }),
            RuntimeEvent::PromptReleaseTransitioned(event) => {
                Some(RuntimeEntityRef::PromptRelease {
                    prompt_release_id: event.prompt_release_id.clone(),
                })
            }
            RuntimeEvent::PromptRolloutStarted(event) => Some(RuntimeEntityRef::PromptRelease {
                prompt_release_id: event.prompt_release_id.clone(),
            }),
            RuntimeEvent::TenantCreated(_) => None,
            RuntimeEvent::TenantUpdated(_) => None,
            RuntimeEvent::WorkspaceCreated(_) => None,
            RuntimeEvent::WorkspaceArchived(_) => None,
            RuntimeEvent::ProjectCreated(_) => None,
            RuntimeEvent::RouteDecisionMade(_) => None,
            RuntimeEvent::ProviderCallCompleted(_) => None,
            RuntimeEvent::LlmCompletionRecorded(_) => None,
            // #789: run-keyed so `read_by_entity(Run(id))` returns
            // the full per-run trajectory in chronological order.
            RuntimeEvent::RunReasoningStepRecorded(event) => Some(RuntimeEntityRef::Run {
                run_id: event.run_id.clone(),
            }),
            RuntimeEvent::SoulPatchProposed(_) => None,
            RuntimeEvent::SoulPatchApplied(_) => None,
            RuntimeEvent::SessionCostUpdated(_) => None,
            RuntimeEvent::RunCostUpdated(_) => None,
            RuntimeEvent::SpendAlertTriggered(_) => None,
            RuntimeEvent::ProviderBudgetSet(_)
            | RuntimeEvent::ChannelCreated(_)
            | RuntimeEvent::ChannelMessageSent(_)
            | RuntimeEvent::ChannelMessageConsumed(_)
            | RuntimeEvent::DefaultSettingSet(_)
            | RuntimeEvent::DefaultSettingCleared(_)
            | RuntimeEvent::LicenseActivated(_)
            | RuntimeEvent::EntitlementOverrideSet(_)
            | RuntimeEvent::NotificationPreferenceSet(_)
            | RuntimeEvent::NotificationSent(_)
            | RuntimeEvent::ProviderPoolCreated(_)
            | RuntimeEvent::ProviderPoolConnectionAdded(_)
            | RuntimeEvent::ProviderPoolConnectionRemoved(_)
            | RuntimeEvent::TenantQuotaSet(_)
            | RuntimeEvent::TenantQuotaViolated(_)
            | RuntimeEvent::RetentionPolicySet(_)
            | RuntimeEvent::RunCostAlertSet(_)
            | RuntimeEvent::RunCostAlertTriggered(_)
            | RuntimeEvent::WorkspaceMemberAdded(_)
            | RuntimeEvent::WorkspaceMemberRemoved(_)
            | RuntimeEvent::ApprovalDelegated(_)
            | RuntimeEvent::AuditLogEntryRecorded(_)
            | RuntimeEvent::CheckpointStrategySet(_)
            | RuntimeEvent::CredentialKeyRotated(_)
            | RuntimeEvent::CredentialRevoked(_)
            | RuntimeEvent::CredentialStored(_)
            | RuntimeEvent::EvalBaselineLocked(_)
            | RuntimeEvent::EvalBaselineSet(_)
            | RuntimeEvent::EvalDatasetCreated(_)
            | RuntimeEvent::EvalDatasetEntryAdded(_)
            | RuntimeEvent::EvalRubricCreated(_)
            | RuntimeEvent::EventLogCompacted(_)
            | RuntimeEvent::GuardrailPolicyCreated(_)
            | RuntimeEvent::GuardrailPolicyEvaluated(_)
            | RuntimeEvent::OperatorIntervention(_)
            | RuntimeEvent::OperatorProfileCreated(_)
            | RuntimeEvent::OperatorProfileUpdated(_)
            | RuntimeEvent::TenantRoleGranted(_)
            | RuntimeEvent::TenantRoleRevoked(_)
            | RuntimeEvent::PauseScheduled(_)
            | RuntimeEvent::PermissionDecisionRecorded(_)
            | RuntimeEvent::ProviderBindingCreated(_)
            | RuntimeEvent::ProviderBindingStateChanged(_)
            | RuntimeEvent::ProviderBudgetAlertTriggered(_)
            | RuntimeEvent::ProviderBudgetExceeded(_)
            | RuntimeEvent::ProviderConnectionRegistered(_)
            | RuntimeEvent::ProviderConnectionDeleted(_)
            | RuntimeEvent::ProviderHealthChecked(_)
            | RuntimeEvent::ProviderHealthScheduleSet(_)
            | RuntimeEvent::ProviderHealthScheduleTriggered(_)
            | RuntimeEvent::ProviderMarkedDegraded(_)
            | RuntimeEvent::ProviderModelRegistered(_)
            | RuntimeEvent::ProviderRecovered(_)
            | RuntimeEvent::ProviderRetryPolicySet(_)
            | RuntimeEvent::RecoveryEscalated(_)
            | RuntimeEvent::ResourceShareRevoked(_)
            | RuntimeEvent::ResourceShared(_)
            | RuntimeEvent::RoutePolicyCreated(_)
            | RuntimeEvent::RoutePolicyUpdated(_)
            | RuntimeEvent::RunSlaBreached(_)
            | RuntimeEvent::RunSlaSet(_)
            | RuntimeEvent::SignalRouted(_)
            | RuntimeEvent::SignalSubscriptionCreated(_)
            | RuntimeEvent::TriggerCreated(_)
            | RuntimeEvent::TriggerEnabled(_)
            | RuntimeEvent::TriggerDisabled(_)
            | RuntimeEvent::TriggerSuspended(_)
            | RuntimeEvent::TriggerResumed(_)
            | RuntimeEvent::TriggerDeleted(_)
            | RuntimeEvent::TriggerFired(_)
            | RuntimeEvent::TriggerSkipped(_)
            | RuntimeEvent::TriggerDenied(_)
            | RuntimeEvent::TriggerRateLimited(_)
            | RuntimeEvent::TriggerPendingApproval(_)
            | RuntimeEvent::RunTemplateCreated(_)
            | RuntimeEvent::RunTemplateDeleted(_)
            | RuntimeEvent::SnapshotCreated(_)
            | RuntimeEvent::TaskDependencyAdded(_)
            | RuntimeEvent::TaskDependencyResolved(_)
            | RuntimeEvent::TaskLeaseExpired(_)
            | RuntimeEvent::TaskPriorityChanged(_)
            | RuntimeEvent::ToolInvocationProgressUpdated(_)
            | RuntimeEvent::RecoverySummaryEmitted(_)
            | RuntimeEvent::ScheduledTaskCreated(_)
            | RuntimeEvent::DecisionRecorded(_)
            | RuntimeEvent::DecisionCacheWarmup(_) => None,
            RuntimeEvent::RunCompletionAnnotated(event) => Some(RuntimeEntityRef::Run {
                run_id: event.run_id.clone(),
            }),
            RuntimeEvent::CompletionContractResolved(event) => Some(RuntimeEntityRef::Run {
                run_id: event.run_id.clone(),
            }),
            RuntimeEvent::TerminalRecoveryAttempted(event) => Some(RuntimeEntityRef::Run {
                run_id: event.run_id.clone(),
            }),
            // F65 PR-1: orchestrator session redesign foundation.
            // Session-lifecycle + orchestrator-level events (attempt
            // start/complete, outcome, decision, summarizer fallback,
            // workspace-backend degraded) resolve back to the session.
            // Breaker / budget trips drill down to the active run so
            // operator attention lands on the specific failing run,
            // while the checkpoint variant points at the checkpoint
            // it just persisted. Workspace-snapshot events have no
            // matching `RuntimeEntityRef` variant yet (extension deferred
            // to PR-2 with the projection).
            RuntimeEvent::SessionAttemptStarted(event) => Some(RuntimeEntityRef::Session {
                session_id: event.session_id.clone(),
            }),
            RuntimeEvent::SessionAttemptCompleted(event) => Some(RuntimeEntityRef::Session {
                session_id: event.session_id.clone(),
            }),
            RuntimeEvent::CircuitBreakerTripped(event) => Some(RuntimeEntityRef::Run {
                run_id: event.run_id.clone(),
            }),
            RuntimeEvent::BudgetThresholdCrossed(event) => Some(RuntimeEntityRef::Run {
                run_id: event.run_id.clone(),
            }),
            RuntimeEvent::CheckpointPersisted(event) => Some(RuntimeEntityRef::Checkpoint {
                checkpoint_id: event.checkpoint_id.clone(),
            }),
            // Workspace snapshot events: no current RuntimeEntityRef variant
            // covers snapshots. Extending the enum is deferred to PR-2 when
            // the projection lands and operator drill-down requires it.
            RuntimeEvent::WorkspaceSnapshotCreated(_) => None,
            RuntimeEvent::WorkspaceSnapshotReaped(_) => None,
            RuntimeEvent::SessionOutcomeEmitted(event) => Some(RuntimeEntityRef::Session {
                session_id: event.session_id.clone(),
            }),
            RuntimeEvent::OrchestratorDecisionMade(event) => Some(RuntimeEntityRef::Session {
                session_id: event.session_id.clone(),
            }),
            RuntimeEvent::SummarizerFallback(event) => Some(RuntimeEntityRef::Session {
                session_id: event.session_id.clone(),
            }),
            RuntimeEvent::WorkspaceBackendDegraded(event) => Some(RuntimeEntityRef::Session {
                session_id: event.session_id.clone(),
            }),
            RuntimeEvent::SandboxCrashRecovered(event) => Some(RuntimeEntityRef::Session {
                session_id: event.session_id.clone(),
            }),
            // RFC 029: knowledge-provider lifecycle events are keyed by
            // (project, provider_ref) — not any existing `RuntimeEntityRef`
            // variant. Ingest events COULD map to `IngestJob` but the
            // plugin ingest path doesn't produce an `IngestJobId`; they
            // carry a `KnowledgeDocumentId` instead. Returning `None` is
            // correct — operator UI joins these events via their own
            // projection tables rather than the entity-ref index.
            RuntimeEvent::KnowledgeProviderConfigured(_) => None,
            RuntimeEvent::KnowledgeProviderUnavailable(_) => None,
            RuntimeEvent::KnowledgeProviderCapabilityChanged(_) => None,
            RuntimeEvent::KnowledgeIngestSubmitted(_) => None,
            RuntimeEvent::KnowledgeIngestRejected(_) => None,
            RuntimeEvent::KnowledgeIngestStatusUpdated(_) => None,
            RuntimeEvent::MemoryProviderConfigured(_) => None,
            RuntimeEvent::MemoryProviderUnavailable(_) => None,
            RuntimeEvent::MemoryProviderCapabilityChanged(_) => None,
            RuntimeEvent::MemoryIngestSubmitted(_) => None,
            RuntimeEvent::MemoryIngestRejected(_) => None,
            RuntimeEvent::MemoryIngestStatusUpdated(_) => None,
            RuntimeEvent::KnowledgeProviderFamilyMismatch(_) => None,
            RuntimeEvent::MemoryProviderFamilyMismatch(_) => None,
            // RFC 031: agent-role lifecycle events live on the
            // project-agent-roles projection, not on any existing
            // `RuntimeEntityRef` variant. `ToolDeclaredButMissing`
            // points at the run whose DECIDE-filter emitted it.
            RuntimeEvent::AgentRoleDefined(_) => None,
            RuntimeEvent::AgentRoleRetracted(_) => None,
            RuntimeEvent::ToolDeclaredButMissing(event) => Some(RuntimeEntityRef::Run {
                run_id: event.run_id.clone(),
            }),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StateTransition<S> {
    pub from: Option<S>,
    pub to: S,
}

impl StateTransition<crate::RunState> {
    /// True iff this transition crosses an orchestrator-resume boundary —
    /// i.e. the run was paused (`WaitingApproval`, `WaitingDependency`,
    /// `Paused`) and is now back in `Running`. Centralised here so the
    /// three projection backends (in-memory, Postgres, SQLite) cannot
    /// drift on which transitions count as a resume — #795 originally
    /// landed in three sites, and Gemini called out the duplication on
    /// PR #801.
    pub fn is_run_resume_boundary(&self) -> bool {
        matches!(
            self.from,
            Some(crate::RunState::WaitingApproval)
                | Some(crate::RunState::WaitingDependency)
                | Some(crate::RunState::Paused)
        ) && self.to == crate::RunState::Running
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionCreated {
    pub project: ProjectKey,
    pub session_id: SessionId,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionStateChanged {
    pub project: ProjectKey,
    pub session_id: SessionId,
    pub transition: StateTransition<SessionState>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunCreated {
    pub project: ProjectKey,
    pub session_id: SessionId,
    pub run_id: RunId,
    pub parent_run_id: Option<RunId>,
    pub prompt_release_id: Option<crate::ids::PromptReleaseId>,
    /// GAP-011: optional agent role attached at run creation.
    #[serde(default)]
    pub agent_role_id: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunStateChanged {
    pub project: ProjectKey,
    pub run_id: RunId,
    pub transition: StateTransition<RunState>,
    pub failure_class: Option<FailureClass>,
    pub pause_reason: Option<PauseReason>,
    pub resume_trigger: Option<ResumeTrigger>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskCreated {
    pub project: ProjectKey,
    pub task_id: TaskId,
    pub parent_run_id: Option<RunId>,
    pub parent_task_id: Option<TaskId>,
    pub prompt_release_id: Option<crate::ids::PromptReleaseId>,
    /// Session the task is scoped to. `None` for bare (session-less)
    /// tasks that route through the solo `task_to_execution_id` mint path.
    ///
    /// Kept optional so event streams written before this field existed
    /// still deserialize; the projection falls back to walking
    /// `parent_run_id → session` when the field is `None`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<SessionId>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskLeaseClaimed {
    pub project: ProjectKey,
    pub task_id: TaskId,
    pub lease_owner: String,
    pub lease_token: u64,
    pub lease_expires_at_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskLeaseHeartbeated {
    pub project: ProjectKey,
    pub task_id: TaskId,
    pub lease_token: u64,
    pub lease_expires_at_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskStateChanged {
    pub project: ProjectKey,
    pub task_id: TaskId,
    pub transition: StateTransition<TaskState>,
    pub failure_class: Option<FailureClass>,
    pub pause_reason: Option<PauseReason>,
    pub resume_trigger: Option<ResumeTrigger>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApprovalRequested {
    pub project: ProjectKey,
    pub approval_id: ApprovalId,
    pub run_id: Option<RunId>,
    pub task_id: Option<TaskId>,
    pub requirement: ApprovalRequirement,
    /// What the agent wants to do (e.g., "Create PR for issue #18").
    #[serde(default)]
    pub title: Option<String>,
    /// Detailed context — the agent's proposal, reasoning, affected files.
    #[serde(default)]
    pub description: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApprovalResolved {
    pub project: ProjectKey,
    pub approval_id: ApprovalId,
    pub decision: ApprovalDecision,
}

// ── Tool-call approval events (PR BP-1 foundation) ────────────────────────────
//
// These four events form the type-level foundation for tool-call approval
// workflows. They are strictly additive — the legacy
// `ApprovalRequested` / `ApprovalResolved` pair above remains in use for
// plan review (RFC 018), RFC 022 trigger approvals, prompt-release
// governance, and the current tool-call approval emission in
// `execute_impl.rs`. A later PR in the wave migrates the tool-call
// emission site from `ApprovalRequested` to [`ToolCallProposed`].
//
// All four events carry `project: ProjectKey` so the event log can own
// them; `primary_entity_ref()` returns `None` because `ToolCallId` is not
// (yet) a member of `RuntimeEntityRef` — adding it is deferred to a
// follow-up PR that introduces projection state.

/// The orchestrator has proposed a tool call that requires operator
/// approval before execution.
///
/// The operator surface consumes `display_summary` to render a
/// human-friendly prompt and uses `match_policy` to seed the default
/// "remember this decision for the session" UX.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolCallProposed {
    pub project: ProjectKey,
    pub call_id: ToolCallId,
    pub session_id: SessionId,
    pub run_id: RunId,
    pub tool_name: String,
    pub tool_args: serde_json::Value,
    /// Short human-readable summary of what the tool call would do
    /// (e.g. `"Read /workspaces/cairn/Cargo.toml"`). Rendered in the
    /// approval UI; may be empty if the caller has nothing useful to
    /// offer.
    pub display_summary: String,
    /// How the operator's decision should match future calls if they
    /// pick `ApprovalScope::Session`.
    pub match_policy: ApprovalMatchPolicy,
    pub proposed_at_ms: u64,
}

/// An operator approved a proposed tool call.
///
/// If the operator edited the arguments before approving (the "approve
/// with amendment" flow), `approved_tool_args` holds the edited payload
/// and the execute phase uses those instead of the original
/// `ToolCallProposed.tool_args`.
///
/// # Source-of-truth invariant for the executed arguments
///
/// On replay, the final arguments the execute phase runs are
/// deterministically the **last** of the following to appear for a
/// given `call_id`, in event-log order:
///
/// 1. `ToolCallApproved.approved_tool_args` if `Some`.
/// 2. `ToolCallAmended.new_tool_args` if any amendments were emitted.
/// 3. `ToolCallProposed.tool_args` otherwise.
///
/// Concretely: `ToolCallAmended` records *preview* edits an operator
/// made before resolving. A subsequent `ToolCallApproved` either
/// repeats the amended args in `approved_tool_args: Some(...)` (the
/// normal path, so projections can ignore earlier `ToolCallAmended`
/// events) or carries `approved_tool_args: None`, which means "approve
/// whatever the most recent `ToolCallAmended` settled on, or the
/// original `ToolCallProposed.tool_args` if none was emitted".
///
/// This invariant keeps projection state reconstructable from the
/// event log alone without cross-referencing in-memory UI state.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolCallApproved {
    pub project: ProjectKey,
    pub call_id: ToolCallId,
    pub session_id: SessionId,
    pub operator_id: OperatorId,
    pub scope: ApprovalScope,
    /// Operator-edited arguments attached to the approval itself.
    /// `Some` overrides any earlier `ToolCallAmended` payload and the
    /// original `ToolCallProposed.tool_args`. `None` means "approve
    /// whatever arguments the most recent preceding `ToolCallAmended`
    /// or `ToolCallProposed` carried" (see the struct-level invariant
    /// for the full precedence order).
    pub approved_tool_args: Option<serde_json::Value>,
    pub approved_at_ms: u64,
}

/// An operator rejected a proposed tool call.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolCallRejected {
    pub project: ProjectKey,
    pub call_id: ToolCallId,
    pub session_id: SessionId,
    pub operator_id: OperatorId,
    /// Optional operator-supplied reason; surfaced in audit log and to
    /// the agent as a rejection message.
    pub reason: Option<String>,
    pub rejected_at_ms: u64,
}

/// An operator amended a proposed tool call's arguments without yet
/// resolving it. Enables the "edit before approval" flow where an
/// operator tweaks arguments, reviews the updated display, and then
/// emits a separate [`ToolCallApproved`] / [`ToolCallRejected`].
///
/// This event is intentionally separate from `ToolCallApproved` so that
/// the audit log preserves the full chain of edits an operator made
/// before committing to a final decision.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolCallAmended {
    pub project: ProjectKey,
    pub call_id: ToolCallId,
    /// Session the amended call belongs to. Included (like the other
    /// three tool-call approval events) so downstream projections can
    /// index by session without walking prior events to recover the
    /// association.
    pub session_id: SessionId,
    pub operator_id: OperatorId,
    pub new_tool_args: serde_json::Value,
    pub amended_at_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckpointRecorded {
    pub project: ProjectKey,
    pub run_id: RunId,
    pub checkpoint_id: CheckpointId,
    pub disposition: CheckpointDisposition,
    pub data: Option<serde_json::Value>,
    /// RFC 020 Track 4: dual checkpoint — `Intent` captures the decide
    /// output + planned tool-call IDs before execute; `Result` captures the
    /// post-execute message history after the iteration settles. `None` for
    /// legacy (pre-Track-4) checkpoints.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<crate::recovery::CheckpointKind>,
    /// RFC 020 Track 4: size (bytes) of the serialized message history at
    /// the moment this checkpoint was recorded. Populated for observability
    /// so operators can monitor checkpoint body cost and decide when (if
    /// ever) to add diff-based compaction (Gap 3 — deferred to Track 4b).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message_history_size: Option<u32>,
    /// RFC 020 Track 4: deterministic `ToolCallId`s planned at this
    /// checkpoint. Populated on `Intent`; empty on `Result` (the Intent
    /// checkpoint already carries the full planned list; duplicating on
    /// the Result checkpoint would only inflate the event body).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tool_call_ids: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckpointRestored {
    pub project: ProjectKey,
    pub run_id: RunId,
    pub checkpoint_id: CheckpointId,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MailboxMessageAppended {
    pub project: ProjectKey,
    pub message_id: MailboxMessageId,
    pub run_id: Option<RunId>,
    pub task_id: Option<TaskId>,
    #[serde(default)]
    pub content: String,
    #[serde(default)]
    pub from_run_id: Option<RunId>,
    #[serde(default)]
    pub from_task_id: Option<TaskId>,
    #[serde(default)]
    pub deliver_at_ms: u64,
    /// RFC 002: display name or agent ID of the message sender.
    #[serde(default)]
    pub sender: Option<String>,
    /// RFC 002: display name or agent ID of the intended recipient.
    #[serde(default)]
    pub recipient: Option<String>,
    /// RFC 002: full message body (may differ from content for structured payloads).
    #[serde(default)]
    pub body: Option<String>,
    /// RFC 002: epoch-ms when the message was created by the sender.
    #[serde(default)]
    pub sent_at: Option<u64>,
    /// Delivery lifecycle state.
    #[serde(default)]
    pub delivery_status: Option<MailboxDeliveryStatus>,
}

/// Mailbox delivery lifecycle.
///
/// Wire-compatible snake_case with the pre-enum `String` shape.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MailboxDeliveryStatus {
    /// Message created, not yet delivered.
    Pending,
    /// Message deferred until `deliver_at_ms`.
    Scheduled,
    /// Delivered to the recipient's mailbox.
    Delivered,
    /// Delivery attempt failed terminally.
    Failed,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolInvocationStarted {
    pub project: ProjectKey,
    pub invocation_id: ToolInvocationId,
    pub session_id: Option<SessionId>,
    pub run_id: Option<RunId>,
    pub task_id: Option<TaskId>,
    pub target: ToolInvocationTarget,
    pub execution_class: ExecutionClass,
    pub prompt_release_id: Option<crate::ids::PromptReleaseId>,
    pub requested_at_ms: u64,
    pub started_at_ms: u64,
    /// F55: structured tool arguments captured when the runtime dispatched
    /// the tool. Enables `GET /v1/tool-invocations` to surface "what cairn
    /// ran" to operators. `None` on legacy events and for callers that
    /// could not plumb the args through (kept optional to preserve the
    /// wire contract on old event-log replay).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub args_json: Option<serde_json::Value>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolInvocationCompleted {
    pub project: ProjectKey,
    pub invocation_id: ToolInvocationId,
    pub task_id: Option<TaskId>,
    pub tool_name: String,
    pub finished_at_ms: u64,
    pub outcome: ToolInvocationOutcomeKind,
    /// RFC 020 Track 3: deterministic tool-call ID for idempotent recovery.
    /// `None` when the orchestrator has not minted one (legacy event-log
    /// entries and non-orchestrator callers like `handlers/tools.rs`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_call_id: Option<String>,
    /// RFC 020 Track 3: cached tool-result payload. Enables the startup
    /// replay to rebuild `ToolCallResultCache` from the event log so a
    /// resumed run on a fresh process still serves cache hits.
    /// `None` when the tool returned no useful result or the legacy
    /// record path was used.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result_json: Option<serde_json::Value>,
    /// F55: truncated UTF-8 preview of the tool's captured output. The
    /// full payload lives on `result_json`; this field is what the
    /// projection persists for operator observability. `None` on
    /// legacy events.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_preview: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolInvocationFailed {
    pub project: ProjectKey,
    pub invocation_id: ToolInvocationId,
    pub task_id: Option<TaskId>,
    pub tool_name: String,
    pub finished_at_ms: u64,
    pub outcome: ToolInvocationOutcomeKind,
    pub error_message: Option<String>,
    /// F55: truncated UTF-8 preview of whatever the tool produced before
    /// it failed (stderr tail, partial stdout, etc.). `None` on legacy
    /// events and when the runtime had no output to capture.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_preview: Option<String>,
}

/// RFC 020 Track 3: a tool invocation was served from the result cache
/// instead of being re-dispatched. Emitted when a resumed run recomputes
/// the same `ToolCallId` (deterministic by run_id + step + call_index +
/// tool_name + normalized_args) and finds a prior completion in the cache.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolInvocationCacheHit {
    pub project: ProjectKey,
    pub invocation_id: ToolInvocationId,
    pub run_id: Option<RunId>,
    pub task_id: Option<TaskId>,
    pub tool_name: String,
    /// Deterministic tool-call identifier (stringified `ToolCallId`).
    pub tool_call_id: String,
    /// When the cached result was first produced (ms since epoch).
    pub original_completed_at_ms: u64,
    /// When the cache hit was served on this boot (ms since epoch).
    pub served_at_ms: u64,
}

/// RFC 020 Track 3: recovery of a tool call that cannot be safely re-dispatched
/// (classified as `DangerousPause`) — the run transitions to `WaitingApproval`
/// and the operator must confirm before proceeding.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolRecoveryPaused {
    pub project: ProjectKey,
    pub run_id: RunId,
    pub task_id: Option<TaskId>,
    pub tool_name: String,
    /// Deterministic tool-call identifier (stringified `ToolCallId`).
    pub tool_call_id: String,
    /// Human-readable reason — e.g. "DangerousPause tool with no cached result on recovery".
    pub reason: String,
    pub paused_at_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExternalWorkerRegistered {
    /// Sentinel project key (tenant-scoped event has no project).
    pub sentinel_project: ProjectKey,
    pub worker_id: crate::ids::WorkerId,
    pub tenant_id: TenantId,
    pub display_name: String,
    pub registered_at: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExternalWorkerReported {
    pub report: ExternalWorkerReport,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExternalWorkerSuspended {
    pub sentinel_project: ProjectKey,
    pub worker_id: crate::ids::WorkerId,
    pub tenant_id: TenantId,
    pub suspended_at: u64,
    pub reason: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExternalWorkerReactivated {
    pub sentinel_project: ProjectKey,
    pub worker_id: crate::ids::WorkerId,
    pub tenant_id: TenantId,
    pub reactivated_at: u64,
}

/// RFC 014 / issue #670 G1+G2: parent→child subagent spawn fact.
///
/// Emitted once per `TaskService::spawn_subagent` call when the
/// orchestrator executes a `spawn_subagent` proposal from the LLM. The
/// event captures the LLM's **intent** — the sub-goal the parent
/// delegated, and the role it delegated to — in addition to the
/// parent/child linkage.
///
/// `goal` and `role` are required in the domain contract but carry
/// `#[serde(default)]` so older event-log entries (pre-G2, where the
/// fields did not exist) still deserialise cleanly as empty strings.
/// New emitters MUST populate them; an empty string post-G2 indicates
/// the execute layer dropped the LLM proposal context and should be
/// treated as a bug (see G2 in `#670`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubagentSpawned {
    pub project: ProjectKey,
    pub parent_run_id: RunId,
    pub parent_task_id: Option<TaskId>,
    pub child_task_id: TaskId,
    pub child_session_id: SessionId,
    pub child_run_id: Option<RunId>,
    /// Sub-goal the parent run delegated, taken verbatim from the
    /// `ActionProposal.tool_args["goal"]` string the LLM emitted.
    /// Empty string when the event was appended before G2 shipped
    /// (`#670`) — operators should treat such rows as legacy audit
    /// records with no delegated-goal context.
    #[serde(default)]
    pub goal: String,
    /// Agent role the parent delegated to. Mirrors
    /// `ActionProposal.tool_name` when `action_type=SpawnSubagent`.
    /// Typically one of `executor`, `researcher`, `reviewer`,
    /// `generic`; the execute layer validates against the known-role
    /// allow-list before emitting. Empty for pre-G2 events.
    #[serde(default)]
    pub role: String,
    /// Optional freeform context the parent threaded into the spawn.
    ///
    /// (#775) Used to carry parent-side learning into the child's
    /// first DECIDE prompt — typically a previous-attempt mistake to
    /// avoid, or a workspace path / credential the parent already
    /// resolved. The orchestrator surfaces this verbatim under a
    /// `## Parent context` section in the child's user message; it
    /// is NOT meant to carry the goal itself (the goal goes in
    /// `goal`).
    ///
    /// `#[serde(default, skip_serializing_if = "Option::is_none")]`
    /// keeps wire compatibility with pre-#775 events: replayed
    /// events without this field deserialise as `None`, and new
    /// spawns without parent context don't bloat the event row.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_context: Option<String>,
}

/// Recovery attempt fact per RFC 002.
///
/// At least one of `run_id` or `task_id` MUST be present — a targetless
/// recovery event has no semantic meaning and indicates a caller bug.
/// Callers should assert `has_target()` before appending this event.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecoveryAttempted {
    pub project: ProjectKey,
    pub run_id: Option<RunId>,
    pub task_id: Option<TaskId>,
    pub reason: String,
    /// RFC 020 Track 1: the cairn-app boot that originated this recovery sweep.
    /// `None` for legacy FF-authored recovery events (task lease expiry etc.)
    /// so existing callers keep deserialising cleanly.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub boot_id: Option<String>,
}

impl RecoveryAttempted {
    /// Returns `true` when the event targets at least one recoverable entity.
    ///
    /// RFC 002 requires recovery events to be anchored to a run or task.
    /// A `false` return indicates a malformed event (both fields absent).
    pub fn has_target(&self) -> bool {
        self.run_id.is_some() || self.task_id.is_some()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignalIngested {
    pub project: ProjectKey,
    pub signal_id: SignalId,
    pub source: String,
    pub payload: serde_json::Value,
    pub timestamp_ms: u64,
}

/// Recovery completion fact per RFC 002.
///
/// At least one of `run_id` or `task_id` MUST be present — a targetless
/// recovery event has no semantic meaning and indicates a caller bug.
/// Callers should assert `has_target()` before appending this event.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecoveryCompleted {
    pub project: ProjectKey,
    pub run_id: Option<RunId>,
    pub task_id: Option<TaskId>,
    pub recovered: bool,
    /// RFC 020 Track 1: the cairn-app boot that originated this recovery sweep.
    /// `None` for legacy FF-authored recovery events so pre-RFC-020 events
    /// still round-trip through serde.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub boot_id: Option<String>,
}

impl RecoveryCompleted {
    /// Returns `true` when the event targets at least one recoverable entity.
    ///
    /// RFC 002 requires recovery events to be anchored to a run or task.
    /// A `false` return indicates a malformed event (both fields absent).
    pub fn has_target(&self) -> bool {
        self.run_id.is_some() || self.task_id.is_some()
    }
}

/// RFC 020 Track 4 — once-per-boot recovery audit summary.
///
/// Emitted exactly once at the end of `RecoveryService::recover_all` with
/// per-branch counts. The `boot_id` correlates this summary with the
/// `RecoveryAttempted`/`RecoveryCompleted` pairs emitted during the same
/// sweep, giving operators a single wire event to observe startup recovery
/// cost without re-aggregating the stream.
///
/// Branch counts that Track 4 cannot populate directly (sandbox, graph,
/// memory, trigger, webhook dedup — each owned by a sibling recovery
/// service) default to 0 and will be filled in by their respective tracks.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecoverySummaryEmitted {
    /// Tenant-scoped sentinel project (no per-project recovery summary in v1).
    pub sentinel_project: ProjectKey,
    /// Unique identifier for this cairn-app boot.
    pub boot_id: String,
    pub recovered_runs: u32,
    pub recovered_tasks: u32,
    pub recovered_sandboxes: u32,
    pub preserved_sandboxes: u32,
    pub orphaned_sandboxes_cleaned: u32,
    pub decision_cache_entries: u32,
    pub stale_pending_cleared: u32,
    pub tool_result_cache_entries: u32,
    pub memory_projection_entries: u32,
    pub graph_nodes_recovered: u32,
    pub graph_edges_recovered: u32,
    pub webhook_dedup_entries: u32,
    pub trigger_projections: u32,
    /// Wall-clock ms from process start to recovery completion.
    pub startup_ms: u64,
    /// Unix-ms timestamp when the summary was emitted.
    pub summary_at_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct UserMessageAppended {
    pub project: ProjectKey,
    pub session_id: SessionId,
    pub run_id: RunId,
    /// The user's message text. Empty string when used as a bare signal
    /// (backward-compatible — `#[serde(default)]` on old events).
    #[serde(default)]
    pub content: String,
    /// Optional sequence number within the session for stable ordering.
    #[serde(default)]
    pub sequence: u64,
    /// Unix milliseconds when the message was appended.
    #[serde(default)]
    pub appended_at_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct IngestJobStarted {
    pub project: ProjectKey,
    pub job_id: IngestJobId,
    pub source_id: Option<crate::ids::SourceId>,
    pub document_count: u32,
    pub started_at: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct IngestJobCompleted {
    pub project: ProjectKey,
    pub job_id: IngestJobId,
    pub success: bool,
    pub error_message: Option<String>,
    pub completed_at: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvalRunStarted {
    pub project: ProjectKey,
    pub eval_run_id: EvalRunId,
    pub subject_kind: String,
    pub evaluator_type: String,
    pub started_at: u64,
    /// Prompt linkage — populated by the API eval surface so runs can be
    /// reconstructed from the event log on restart.
    #[serde(default)]
    pub prompt_asset_id: Option<PromptAssetId>,
    #[serde(default)]
    pub prompt_version_id: Option<PromptVersionId>,
    #[serde(default)]
    pub prompt_release_id: Option<PromptReleaseId>,
    #[serde(default)]
    pub created_by: Option<OperatorId>,
    /// Dataset binding captured at run-create time.
    ///
    /// Persisted on `EvalRunStarted` so `replay_evals` can restore the
    /// dataset linkage after a restart — the in-memory eval service
    /// previously lost this binding because only the run-create path
    /// wrote to it. Defaulted to `None` for backward compatibility with
    /// pre-#220 event log entries.
    #[serde(default)]
    pub dataset_id: Option<String>,
    /// Rubric id attached at run-create time (issue #223). `#[serde(default)]`
    /// for backward-compat with pre-#223 event-log entries.
    #[serde(default)]
    pub rubric_id: Option<String>,
    /// Baseline id attached at run-create time (issue #223).
    #[serde(default)]
    pub baseline_id: Option<String>,
}

/// Emitted when an eval run is soft-deleted (archived). The run record is
/// preserved for audit/history so scorecard/matrix views stay intact; list
/// endpoints filter it out by default. Mirrors `WorkspaceArchived` (#218).
/// Issue #244.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvalRunArchived {
    pub project: ProjectKey,
    pub eval_run_id: EvalRunId,
    pub archived_at: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvalRunCompleted {
    pub project: ProjectKey,
    pub eval_run_id: EvalRunId,
    pub success: bool,
    pub error_message: Option<String>,
    /// Node ID of the subject being evaluated (e.g. prompt_release_id).
    pub subject_node_id: Option<String>,
    pub completed_at: u64,
}

/// RFC-025 Phase 1 (#435): score recorded for an eval run.
///
/// Emitted by `score_eval_run_handler` when an operator posts metrics to
/// `POST /v1/evals/runs/:id/score`. The full `EvalMetrics` payload is
/// carried so the projection can surface each canonical metric field
/// (task_success_rate, latency_p50_ms, etc.) after a restart. Without this
/// event, metrics were only held in the in-process `EvalRunService` and
/// silently vanished across a reboot (RFC-025 §#435).
///
/// `EvalMetrics` holds `Option<f64>` fields, so `Eq` cannot be derived.
/// Follows the `OutcomeRecorded` pattern: hand-rolled `PartialEq`/`Eq`
/// that hashes NaN deterministically via `to_bits()` so the enum-wide
/// `#[derive(Eq)]` on `RuntimeEvent` stays valid.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EvalRunScored {
    pub project: ProjectKey,
    pub eval_run_id: EvalRunId,
    /// Full metrics block captured at scoring time. Stored verbatim so
    /// projection reads can reconstruct every canonical field.
    pub metrics: crate::evals::EvalMetrics,
    pub recorded_at_ms: u64,
}

impl PartialEq for EvalRunScored {
    fn eq(&self, other: &Self) -> bool {
        fn opt_bits(a: Option<f64>, b: Option<f64>) -> bool {
            match (a, b) {
                (Some(x), Some(y)) => x.to_bits() == y.to_bits(),
                (None, None) => true,
                _ => false,
            }
        }
        self.project == other.project
            && self.eval_run_id == other.eval_run_id
            && self.recorded_at_ms == other.recorded_at_ms
            && opt_bits(
                self.metrics.task_success_rate,
                other.metrics.task_success_rate,
            )
            && self.metrics.latency_p50_ms == other.metrics.latency_p50_ms
            && self.metrics.latency_p99_ms == other.metrics.latency_p99_ms
            && opt_bits(self.metrics.cost_per_run, other.metrics.cost_per_run)
            && opt_bits(
                self.metrics.policy_pass_rate,
                other.metrics.policy_pass_rate,
            )
            && opt_bits(
                self.metrics.retrieval_hit_at_k,
                other.metrics.retrieval_hit_at_k,
            )
            && opt_bits(
                self.metrics.citation_coverage,
                other.metrics.citation_coverage,
            )
            && opt_bits(
                self.metrics.source_diversity,
                other.metrics.source_diversity,
            )
            && self.metrics.retrieval_latency_ms == other.metrics.retrieval_latency_ms
            && opt_bits(self.metrics.retrieval_cost, other.metrics.retrieval_cost)
    }
}

impl Eq for EvalRunScored {}

/// RFC-025 Phase 1 (#435): rubric-scored verdict recorded for an eval run.
///
/// Emitted by `score_eval_rubric_handler` when an operator posts
/// `{rubric_id, actual_outputs}` to `POST /v1/evals/runs/:id/rubric-score`.
/// Carries the rubric id, the per-dimension weighted scores, and the
/// overall weighted aggregate so the projection can rebuild the verdict
/// after a restart without re-scoring against the dataset.
///
/// `f64` values use the `OutcomeRecorded` pattern for `PartialEq`/`Eq`
/// so NaN round-trips deterministically.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EvalRubricScored {
    pub project: ProjectKey,
    pub eval_run_id: EvalRunId,
    pub rubric_id: String,
    /// Per-dimension `(dimension_name, weighted_score)` pairs, in the
    /// order the rubric evaluated them.
    pub dimension_scores: Vec<(String, f64)>,
    /// Overall weighted score in `[0.0, 1.0]`.
    pub overall: f64,
    pub recorded_at_ms: u64,
}

impl PartialEq for EvalRubricScored {
    fn eq(&self, other: &Self) -> bool {
        self.project == other.project
            && self.eval_run_id == other.eval_run_id
            && self.rubric_id == other.rubric_id
            && self.recorded_at_ms == other.recorded_at_ms
            && self.overall.to_bits() == other.overall.to_bits()
            && self.dimension_scores.len() == other.dimension_scores.len()
            && self
                .dimension_scores
                .iter()
                .zip(other.dimension_scores.iter())
                .all(|((an, av), (bn, bv))| an == bn && av.to_bits() == bv.to_bits())
    }
}

impl Eq for EvalRubricScored {}

/// Actual outcome classification for an agent execution.
///
/// Part of the evaluator–optimizer feedback loop: agents record predicted
/// confidence before execution and actual outcome after, enabling
/// self-correction of confidence calibration over time.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActualOutcome {
    Success,
    Failure,
    Partial,
}

/// Outcome recorded after an agent run completes.
///
/// Links a run to its predicted confidence and actual result, forming the
/// feedback signal for confidence calibration and evaluator tuning.
///
/// **`predicted_confidence` contract:** expected to be finite and in
/// `[0.0, 1.0]`. Storage is raw `f64` because the value originates in an LLM
/// response; the `PartialEq`/`Eq` impls below use `f64::to_bits` so that a
/// stray `NaN` round-trips deterministically (two `NaN`s with identical bit
/// patterns compare equal, respecting `Eq`'s reflexivity rule).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OutcomeRecorded {
    pub project: ProjectKey,
    pub outcome_id: OutcomeId,
    pub run_id: RunId,
    /// Agent type that produced this outcome (e.g. "code_review", "research").
    pub agent_type: String,
    /// Confidence the agent predicted before execution [0.0, 1.0].
    pub predicted_confidence: f64,
    /// What actually happened.
    pub actual_outcome: ActualOutcome,
    pub recorded_at: u64,
}

impl PartialEq for OutcomeRecorded {
    fn eq(&self, other: &Self) -> bool {
        self.project == other.project
            && self.outcome_id == other.outcome_id
            && self.run_id == other.run_id
            && self.agent_type == other.agent_type
            && self.predicted_confidence.to_bits() == other.predicted_confidence.to_bits()
            && self.actual_outcome == other.actual_outcome
            && self.recorded_at == other.recorded_at
    }
}

impl Eq for OutcomeRecorded {}

/// A tenant-scoped scheduled task was registered.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScheduledTaskCreated {
    pub tenant_id: TenantId,
    pub scheduled_task_id: ScheduledTaskId,
    pub name: String,
    pub cron_expression: String,
    pub next_run_at: Option<u64>,
    pub created_at: u64,
}

// ── Plan review events (RFC 018) ─────────────────────────────────────────────

/// A Plan-mode run produced a plan artifact via `<proposed_plan>`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanProposed {
    pub project: ProjectKey,
    pub plan_run_id: RunId,
    pub session_id: SessionId,
    pub plan_markdown: String,
    pub proposed_at: u64,
}

/// An operator approved the plan artifact. Next step: create an Execute-mode run.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanApproved {
    pub project: ProjectKey,
    pub plan_run_id: RunId,
    pub approved_by: OperatorId,
    pub reviewer_comments: Option<String>,
    pub approved_at: u64,
}

/// An operator rejected the plan artifact. No execution run will be created.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanRejected {
    pub project: ProjectKey,
    pub plan_run_id: RunId,
    pub rejected_by: OperatorId,
    pub reason: String,
    pub rejected_at: u64,
}

/// An operator requested plan revision. A new Plan-mode run has been created.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanRevisionRequested {
    pub project: ProjectKey,
    pub original_plan_run_id: RunId,
    pub new_plan_run_id: RunId,
    pub reviewer_comments: String,
    pub requested_at: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PromptAssetCreated {
    pub project: ProjectKey,
    pub prompt_asset_id: PromptAssetId,
    pub name: String,
    pub kind: String,
    pub created_at: u64,
    /// RFC 006: workspace scope — prompt assets belong to a workspace, not a project.
    /// Extracted from `project.workspace_id` at creation time so downstream projections
    /// can scope queries at workspace level without re-deriving from the full project key.
    #[serde(default = "crate::ids::empty_workspace_id")]
    pub workspace_id: WorkspaceId,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PromptVersionCreated {
    pub project: ProjectKey,
    pub prompt_version_id: PromptVersionId,
    pub prompt_asset_id: PromptAssetId,
    pub content_hash: String,
    pub created_at: u64,
    /// RFC 006: workspace scope — prompt versions inherit workspace from the
    /// owning asset. Extracted from `project.workspace_id` at creation time
    /// so projections can scope at workspace level without re-deriving.
    #[serde(default = "crate::ids::empty_workspace_id")]
    pub workspace_id: WorkspaceId,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PromptReleaseCreated {
    pub project: ProjectKey,
    pub prompt_release_id: PromptReleaseId,
    pub prompt_asset_id: PromptAssetId,
    pub prompt_version_id: PromptVersionId,
    pub created_at: u64,
    /// RFC 006: optional human-readable tag for this release (e.g. "v1.2-beta").
    #[serde(default)]
    pub release_tag: Option<String>,
    /// RFC 006: operator or service account that authored this release.
    #[serde(default)]
    pub created_by: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PromptReleaseTransitioned {
    pub project: ProjectKey,
    pub prompt_release_id: PromptReleaseId,
    pub from_state: String,
    pub to_state: String,
    pub transitioned_at: u64,
    /// RFC 006: actor (operator id or service account) that triggered the transition.
    #[serde(default)]
    pub actor: Option<String>,
    /// RFC 006: free-text reason supplied at transition time (e.g. "approved by QA").
    #[serde(default)]
    pub reason: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApprovalPolicyCreated {
    pub project: ProjectKey,
    pub policy_id: String,
    pub tenant_id: TenantId,
    pub name: String,
    pub required_approvers: u32,
    pub allowed_approver_roles: Vec<crate::tenancy::WorkspaceRole>,
    pub auto_approve_after_ms: Option<u64>,
    pub auto_reject_after_ms: Option<u64>,
    pub created_at_ms: u64,
}

/// RFC 001: emitted when a partial rollout (percentage-based traffic split) is started.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PromptRolloutStarted {
    pub project: ProjectKey,
    pub prompt_release_id: PromptReleaseId,
    pub percent: u8,
    pub started_at: u64,
    /// Alias for prompt_release_id used by operator views.
    #[serde(default)]
    pub release_id: Option<PromptReleaseId>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TenantCreated {
    pub project: ProjectKey,
    pub tenant_id: TenantId,
    pub name: String,
    pub created_at: u64,
}

/// RFC 026 PR-A2: tenant-level PATCH edit. Only `name` is mutable
/// today — the `tenants` projection table carries `name`, `created_at`,
/// `updated_at` and nothing else (`crates/cairn-store/migrations/V017`).
///
/// `Option<String>` uses PATCH semantics: `None` leaves the field
/// alone, `Some(value)` overwrites. Every backend's applier COALESCEs
/// or `if let Some` so a future `metadata: Option<...>` field can be
/// added without breaking replay of events emitted before the column
/// existed.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TenantUpdated {
    pub project: ProjectKey,
    pub tenant_id: TenantId,
    #[serde(default)]
    pub name: Option<String>,
    pub updated_by: String,
    pub updated_at_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceCreated {
    pub project: ProjectKey,
    pub workspace_id: WorkspaceId,
    pub tenant_id: TenantId,
    pub name: String,
    pub created_at: u64,
}

/// Emitted when a workspace is soft-deleted (archived). The workspace record
/// is preserved for audit/history; list endpoints filter it out by default.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceArchived {
    pub project: ProjectKey,
    pub workspace_id: WorkspaceId,
    pub tenant_id: TenantId,
    pub archived_at: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectCreated {
    pub project: ProjectKey,
    pub name: String,
    pub created_at: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RouteDecisionMade {
    pub project: ProjectKey,
    pub route_decision_id: RouteDecisionId,
    pub operation_kind: crate::providers::OperationKind,
    pub selected_provider_binding_id: Option<ProviderBindingId>,
    pub final_status: crate::providers::RouteDecisionStatus,
    pub attempt_count: u16,
    pub fallback_used: bool,
    pub decided_at: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderCallCompleted {
    pub project: ProjectKey,
    pub provider_call_id: ProviderCallId,
    pub route_decision_id: RouteDecisionId,
    pub route_attempt_id: RouteAttemptId,
    pub provider_binding_id: ProviderBindingId,
    pub provider_connection_id: ProviderConnectionId,
    pub provider_model_id: ProviderModelId,
    pub operation_kind: crate::providers::OperationKind,
    pub status: crate::providers::ProviderCallStatus,
    pub latency_ms: Option<u64>,
    pub input_tokens: Option<u32>,
    pub output_tokens: Option<u32>,
    pub cost_micros: Option<u64>,
    pub completed_at: u64,
    /// Session context for LLM observability trace derivation.
    #[serde(default)]
    pub session_id: Option<SessionId>,
    /// Run context for LLM observability trace derivation.
    #[serde(default)]
    pub run_id: Option<RunId>,
    /// Provider error class (None on success).
    #[serde(default)]
    pub error_class: Option<crate::providers::ProviderCallErrorClass>,
    /// Raw error text from the provider (None on success).
    #[serde(default)]
    pub raw_error_message: Option<String>,
    /// Retry attempt index (0 = first attempt).
    #[serde(default)]
    pub retry_count: u8,
    /// Task that triggered this provider call, if any.
    #[serde(default)]
    pub task_id: Option<TaskId>,
    /// Prompt release being executed at the time of the call.
    #[serde(default)]
    pub prompt_release_id: Option<PromptReleaseId>,
    /// Position in the fallback chain (0 = primary, 1 = first fallback, …).
    #[serde(default)]
    pub fallback_position: u32,
    /// Unix epoch ms when the call was dispatched to the provider.
    #[serde(default)]
    pub started_at: u64,
    /// Unix epoch ms when the provider response was received.
    #[serde(default)]
    pub finished_at: u64,
}

/// Issue #668: post-redaction LLM round-trip body.
///
/// Emitted once per successful LLM provider call alongside
/// `ProviderCallCompleted`. Where `ProviderCallCompleted` / `LlmCallTrace`
/// carry only metadata (tokens, latency, cost, model id), this event
/// carries the actual prompt + response text. Operators use the
/// persisted body to debug why the LLM made a specific decision —
/// answering "what did the model see?" and "what did it say?".
///
/// **Redaction contract:** all free-text fields (`system_prompt`,
/// `messages_json`, `response_text`) are redacted via
/// `cairn_providers::redact::redact_secrets` BEFORE being placed in the
/// event. Consumers can assume API keys, bearer tokens, and
/// provider-key literals have been stripped. The `tool_calls_json`
/// field is serialised directly from the model's structured output and
/// is redacted the same way.
///
/// **Provenance:** the `trace_id` matches the `provider_call_id` of
/// the sibling `ProviderCallCompleted` event for the same call, so
/// operators can join the two by id.
///
/// **Size:** body fields can be tens of kilobytes for long prompts +
/// reasoning chains. The projection applier caps individual field
/// length (`CAIRN_LLM_TRACE_MAX_FIELD_BYTES`, default 256 KiB) and
/// truncates over-long fields with a `[TRUNCATED]` marker so the
/// event log stays bounded.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LlmCompletionRecorded {
    pub project: ProjectKey,
    /// Matches `ProviderCallCompleted.provider_call_id` for the sibling
    /// metadata event, so the two can be joined for operator queries.
    pub trace_id: String,
    /// Session the call belongs to.
    pub session_id: crate::ids::SessionId,
    /// Run the call belongs to. `None` for out-of-run calls (rare; kept
    /// symmetric with `ProviderCallCompleted.run_id`).
    #[serde(default)]
    pub run_id: Option<crate::ids::RunId>,
    /// Model identifier actually used (resolved post-routing).
    pub model_id: String,
    /// Full system prompt the LLM received, post-redaction.
    pub system_prompt: String,
    /// JSON-serialised `Vec<Message>` (role + content) sent to the
    /// provider, post-redaction. Serialised rather than typed so the
    /// schema evolves with the provider's message shape without
    /// domain-layer migrations.
    pub messages_json: String,
    /// Free-text response from the provider, post-redaction. Empty
    /// when the model went straight to native tool calls without any
    /// prose.
    pub response_text: String,
    /// JSON-serialised `Vec<ToolCall>` the LLM proposed, post-redaction.
    /// Empty JSON array for legacy text-parsing responses. Structured
    /// tool calls are the preferred surface.
    pub tool_calls_json: String,
    /// JSON-serialised `Vec<ToolDef>` the orchestrator shipped TO the
    /// provider in the `tools[]` array of the chat-completion request.
    /// Post-redaction. Empty JSON array when no native tools were
    /// advertised (legacy text-only path).
    ///
    /// **Why this field exists.** Dogfood R7 (2026-05-06) hit a
    /// diagnostic wall: the parent run emitted the same
    /// `spawn_subagent` call five times in a row, and the operator
    /// trace surfaced `tool_calls_json` (what the model emitted) and
    /// `messages_json` (what it saw) but NOT the `tools[]` array the
    /// request shipped with. Debugging "did the model have
    /// `complete_run` available when it chose to spawn again?"
    /// required re-reading the orchestrator source rather than
    /// checking the trace. Persisting the exact `tools[]` array
    /// closes that observability gap.
    ///
    /// Back-compat: `#[serde(default = "default_empty_json_array")]`
    /// makes pre-fix events (replayed from the log) deserialise with
    /// `"[]"` — a valid JSON array — instead of `""`. This keeps the
    /// API contract stable (consumers can always `JSON.parse` the
    /// field) and matches the SQL column `DEFAULT '[]'` so the
    /// projection applier can bind the field verbatim without a
    /// branch on empty strings.
    #[serde(default = "default_empty_json_array")]
    pub tool_defs_json: String,
    /// Unix epoch ms when the body was recorded.
    pub recorded_at_ms: u64,
}

/// #789 — compacted per-iteration reasoning record. Operators get a
/// post-mortem trajectory + a live "what is this agent thinking right
/// now" view from this; without it, diagnosing a stuck-in-loop run
/// requires fetching every per-call LLM body trace and reading them
/// in order, which is expensive and only works AFTER the run is
/// killed.
///
/// This event is the smaller cousin of `LlmCompletionRecorded`. The
/// full body is still captured by that event behind
/// `CAIRN_LLM_TRACE_BODIES_ENABLED`; this one extracts only the
/// signals an operator scanning a fleet view actually reads:
///
/// - `reasoning_compact`: the model's chain-of-thought, truncated to
///   ~1 KiB.
/// - `proposed_action`: the highest-confidence proposal.
/// - `user_message_delta`: what's NEW in the rendered step history
///   since the prior iteration. Empty on iteration 0.
/// - `confidence`: the model's calibrated_confidence on the top
///   proposal.
///
/// Run-keyed (`primary_entity_ref` returns `RuntimeEntityRef::Run`)
/// so `read_by_entity(Run(id))` returns this run's full trajectory
/// without a session-wide filter.
// `confidence` is f64; uses the `OutcomeRecorded` pattern below to
// implement `PartialEq`/`Eq` via `f64::to_bits` so two events with
// the same bit pattern (including NaN) compare equal — needed
// because the `RuntimeEvent` enum derives `Eq`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RunReasoningStep {
    pub project: ProjectKey,
    pub run_id: crate::ids::RunId,
    pub session_id: crate::ids::SessionId,
    pub iteration: u32,
    pub recorded_at_ms: u64,
    /// Model identifier actually used for this iteration's DECIDE.
    pub model_id: String,
    /// Truncated chain-of-thought from the model's free-text response,
    /// post-redaction. Bounded to ~1 KiB (head + tail concat with a
    /// `…[truncated]…` marker if cut). NOT a substitute for
    /// `LlmCompletionRecorded.response_text` — that holds the full
    /// payload when bodies are enabled.
    pub reasoning_compact: String,
    /// The single highest-confidence proposed action for this
    /// iteration. Multi-proposal cases (LLMs returning >1 tool_call
    /// in one response — e.g. parallel inspect-then-act batches) are
    /// uncommon and operators rarely care about runner-up proposals;
    /// if they do, the full proposal array lives on the LLM body
    /// trace. See `proposal_count` below to detect multi-proposal
    /// iterations at a glance.
    pub proposed_action: ProposedActionSummary,
    /// #805: total number of proposals the LLM emitted on this
    /// iteration. For most iterations this is `1` and `proposed_action`
    /// is the only proposal. When `> 1` the LLM returned multiple
    /// tool_calls in one response (parallel batch); `proposed_action`
    /// shows only the top-1, and the operator can drill into
    /// `LlmCompletionRecorded.response_text` for the full set.
    /// Surfacing the count here lets the trajectory UI flag
    /// multi-proposal iterations without forcing a body-trace lookup.
    /// Defaults to `1` on serde-replay of pre-#805 events.
    #[serde(default = "default_proposal_count")]
    pub proposal_count: u32,
    /// Snapshot of the full rendered `## Step history` section of
    /// the user message at this iteration. The trajectory consumer
    /// computes the iteration-to-iteration delta at read time by
    /// diffing this against the prior step's snapshot — which makes
    /// the diff correct for additive histories (Gemini review on
    /// PR #794). Bounded to ~3 KiB via truncation; runs that have
    /// accumulated more than that get a `…[truncated]` marker.
    pub step_history_snapshot: String,
    /// Calibrated confidence the model assigned to the top proposal,
    /// in [0.0, 1.0]. From `DecideOutput.calibrated_confidence`.
    pub confidence: f64,
}

/// #789 — compacted shape for the top-1 proposed action recorded in a
/// `RunReasoningStep`. Forward-compat: an `Other` variant catches any
/// new `ActionType` introduced upstream so the projection apply
/// doesn't need a synchronized release with the orchestrator.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ProposedActionSummary {
    /// `invoke_tool` proposal. `args_preview` is the JSON-serialised
    /// args truncated to ~120 chars.
    ToolCall {
        tool_name: String,
        args_preview: String,
    },
    /// `complete_run` proposal. `final_answer_preview` is the first
    /// ~200 chars of the final answer text.
    CompleteRun { final_answer_preview: String },
    /// `spawn_subagent` proposal. `goal_preview` is the first ~120
    /// chars of the child goal.
    SpawnSubagent { role: String, goal_preview: String },
    /// `escalate_to_operator` proposal. `reason_preview` is the
    /// first ~120 chars of the reason.
    EscalateToOperator { reason_preview: String },
    /// Any other / future ActionType. `kind` carries the raw
    /// stringified action type for forward-compat.
    Other { action_type: String },
}

impl PartialEq for RunReasoningStep {
    fn eq(&self, other: &Self) -> bool {
        self.project == other.project
            && self.run_id == other.run_id
            && self.session_id == other.session_id
            && self.iteration == other.iteration
            && self.recorded_at_ms == other.recorded_at_ms
            && self.model_id == other.model_id
            && self.reasoning_compact == other.reasoning_compact
            && self.proposed_action == other.proposed_action
            && self.proposal_count == other.proposal_count
            && self.step_history_snapshot == other.step_history_snapshot
            && self.confidence.to_bits() == other.confidence.to_bits()
    }
}

impl Eq for RunReasoningStep {}

impl Eq for ProposedActionSummary {}

/// A soul patch has been proposed and is awaiting operator review.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SoulPatchProposed {
    pub project: ProjectKey,
    pub patch_id: String,
    pub patch_content: String,
    pub requires_approval: bool,
    pub proposed_at: u64,
}

/// An approved soul patch has been applied to the document.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SoulPatchApplied {
    pub project: ProjectKey,
    pub patch_id: String,
    pub new_version: u32,
    pub applied_at: u64,
}
/// GAP-006: session-level accumulated cost delta from a completed provider call.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionCostUpdated {
    pub project: ProjectKey,
    pub session_id: crate::ids::SessionId,
    pub tenant_id: crate::ids::TenantId,
    /// Cost delta from this specific provider call, in USD micros.
    pub delta_cost_micros: u64,
    /// Input tokens consumed by this call.
    pub delta_tokens_in: u64,
    /// Output tokens produced by this call.
    pub delta_tokens_out: u64,
    /// Provider call that produced this cost update.
    pub provider_call_id: String,
    pub updated_at_ms: u64,
}

/// Run-level accumulated cost updated after a provider call.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunCostUpdated {
    pub project: ProjectKey,
    pub run_id: RunId,
    pub delta_cost_micros: u64,
    pub delta_tokens_in: u64,
    pub delta_tokens_out: u64,
    pub provider_call_id: String,
    pub updated_at_ms: u64,
    #[serde(default)]
    pub session_id: Option<crate::ids::SessionId>,
    #[serde(default)]
    pub tenant_id: Option<crate::ids::TenantId>,
}

/// GAP-006: tenant-level spend alert triggered.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SpendAlertTriggered {
    pub project: ProjectKey,
    pub alert_id: String,
    pub tenant_id: crate::ids::TenantId,
    pub session_id: crate::ids::SessionId,
    /// Threshold that was crossed, in USD micros.
    pub threshold_micros: u64,
    /// Session total cost at alert time, in USD micros.
    pub current_micros: u64,
    pub triggered_at_ms: u64,
}

// ── New event structs for extended service coverage ─────────────────────────

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderBudgetSet {
    pub tenant_id: crate::ids::TenantId,
    pub budget_id: String,
    pub period: crate::providers::ProviderBudgetPeriod,
    pub limit_micros: u64,
    pub alert_threshold_percent: Option<u32>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChannelCreated {
    pub channel_id: crate::ids::ChannelId,
    pub project: crate::tenancy::ProjectKey,
    pub name: String,
    pub capacity: u32,
    pub created_at_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChannelMessageSent {
    pub channel_id: crate::ids::ChannelId,
    pub project: crate::tenancy::ProjectKey,
    pub message_id: String,
    pub sender_id: String,
    pub body: String,
    pub sent_at_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChannelMessageConsumed {
    pub channel_id: crate::ids::ChannelId,
    pub project: crate::tenancy::ProjectKey,
    pub message_id: String,
    pub consumed_by: String,
    pub consumed_at_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DefaultSettingSet {
    pub scope: crate::tenancy::Scope,
    pub scope_id: String,
    pub key: String,
    pub value: serde_json::Value,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DefaultSettingCleared {
    pub scope: crate::tenancy::Scope,
    pub scope_id: String,
    pub key: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LicenseActivated {
    pub tenant_id: crate::ids::TenantId,
    pub license_id: String,
    pub tier: crate::commercial::ProductTier,
    pub valid_from_ms: u64,
    pub valid_until_ms: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EntitlementOverrideSet {
    pub tenant_id: crate::ids::TenantId,
    pub feature: String,
    pub allowed: bool,
    pub reason: Option<String>,
    pub set_at_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NotificationPreferenceSet {
    pub tenant_id: crate::ids::TenantId,
    pub operator_id: String,
    pub event_types: Vec<String>,
    pub channels: Vec<crate::notification_prefs::NotificationChannel>,
    pub set_at_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NotificationSent {
    pub record_id: String,
    pub tenant_id: crate::ids::TenantId,
    pub operator_id: String,
    pub event_type: String,
    pub channel_kind: String,
    pub channel_target: String,
    pub payload: serde_json::Value,
    pub sent_at_ms: u64,
    pub delivered: bool,
    pub delivery_error: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderPoolCreated {
    pub pool_id: String,
    pub tenant_id: crate::ids::TenantId,
    pub max_connections: u32,
    pub created_at_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderPoolConnectionAdded {
    pub pool_id: String,
    pub tenant_id: crate::ids::TenantId,
    pub connection_id: ProviderConnectionId,
    pub added_at_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderPoolConnectionRemoved {
    pub pool_id: String,
    pub tenant_id: crate::ids::TenantId,
    pub connection_id: ProviderConnectionId,
    pub removed_at_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TenantQuotaSet {
    pub tenant_id: crate::ids::TenantId,
    pub max_concurrent_runs: u32,
    pub max_sessions_per_hour: u32,
    pub max_tasks_per_run: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TenantQuotaViolated {
    pub tenant_id: crate::ids::TenantId,
    pub quota_type: String,
    pub current: u32,
    pub limit: u32,
    pub occurred_at_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetentionPolicySet {
    pub tenant_id: crate::ids::TenantId,
    pub policy_id: String,
    pub full_history_days: u32,
    pub current_state_days: u32,
    pub max_events_per_entity: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunCostAlertSet {
    pub run_id: RunId,
    pub tenant_id: crate::ids::TenantId,
    pub threshold_micros: u64,
    pub set_at_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunCostAlertTriggered {
    pub run_id: RunId,
    pub tenant_id: crate::ids::TenantId,
    pub threshold_micros: u64,
    pub actual_cost_micros: u64,
    pub triggered_at_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceMemberAdded {
    pub workspace_key: crate::tenancy::WorkspaceKey,
    pub member_id: crate::ids::OperatorId,
    pub role: crate::tenancy::WorkspaceRole,
    pub added_at_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceMemberRemoved {
    pub workspace_key: crate::tenancy::WorkspaceKey,
    pub member_id: crate::ids::OperatorId,
    pub removed_at_ms: u64,
}

// ── Second-wave event structs ────────────────────────────────────────────────

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApprovalDelegated {
    pub approval_id: ApprovalId,
    pub delegated_to: String,
    pub delegated_at_ms: u64,
    /// Monotonic per-delegation identifier minted by the runtime service
    /// (`approval_impl::next_delegation_id`) at emit time. Included in
    /// the projection PK so two delegations that share `(approval_id,
    /// delegated_to, delegated_at_ms)` — the same delegator asked twice
    /// in the same millisecond — both survive as distinct audit rows.
    ///
    /// `#[serde(default)]` is retained for backward-compatible
    /// deserialization because pre-Phase-2a.2 fixtures/event-log entries
    /// do not carry this field. Legacy events therefore deserialize with
    /// an empty `delegation_id`; the pg/sqlite + in-memory projections
    /// insert that empty string as-is under the PK `(approval_id,
    /// delegation_id)`. Pre-v0.1.0 there are no persisted ApprovalDelegated
    /// events so no legacy collapse can happen in production; the
    /// `#[serde(default)]` exists for fixture / replay safety only.
    #[serde(default)]
    pub delegation_id: String,
}

/// Audit log entry event — carries only Eq-able fields; metadata is in the projection.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuditLogEntryRecorded {
    pub entry_id: String,
    pub tenant_id: TenantId,
    pub actor_id: String,
    pub action: String,
    pub resource_type: String,
    pub resource_id: String,
    pub outcome: crate::audit::AuditOutcome,
    pub occurred_at_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckpointStrategySet {
    pub strategy_id: String,
    pub description: String,
    pub set_at_ms: u64,
    #[serde(default)]
    pub run_id: Option<RunId>,
    #[serde(default)]
    pub interval_ms: u64,
    #[serde(default)]
    pub max_checkpoints: u32,
    #[serde(default)]
    pub trigger_on_task_complete: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CredentialKeyRotated {
    pub tenant_id: TenantId,
    pub rotation_id: String,
    pub old_key_id: String,
    pub new_key_id: String,
    pub credential_ids_rotated: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CredentialRevoked {
    pub tenant_id: TenantId,
    pub credential_id: crate::ids::CredentialId,
    pub revoked_at_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CredentialStored {
    pub tenant_id: TenantId,
    pub credential_id: crate::ids::CredentialId,
    pub provider_id: String,
    pub encrypted_value: Vec<u8>,
    pub key_id: Option<String>,
    pub key_version: Option<String>,
    pub encrypted_at_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvalBaselineLocked {
    pub baseline_id: String,
    pub locked_at_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvalBaselineSet {
    pub baseline_id: String,
    pub metric: String,
    pub value: String,
    pub set_at_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvalDatasetCreated {
    pub dataset_id: String,
    pub name: String,
    pub created_at_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvalDatasetEntryAdded {
    pub dataset_id: String,
    pub entry_id: String,
    pub added_at_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvalRubricCreated {
    pub rubric_id: String,
    pub name: String,
    pub created_at_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventLogCompacted {
    pub up_to_position: u64,
    pub compacted_at_ms: u64,
    #[serde(default = "crate::ids::empty_tenant_id")]
    pub tenant_id: TenantId,
    #[serde(default)]
    pub events_before: u64,
    #[serde(default)]
    pub events_after: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GuardrailPolicyCreated {
    pub tenant_id: TenantId,
    pub policy_id: String,
    pub name: String,
    pub rules: Vec<crate::policy::GuardrailRule>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GuardrailPolicyEvaluated {
    pub tenant_id: TenantId,
    pub policy_id: String,
    pub subject_type: crate::policy::GuardrailSubjectType,
    pub subject_id: Option<String>,
    pub action: String,
    pub decision: crate::policy::GuardrailDecisionKind,
    pub reason: Option<String>,
    pub evaluated_at_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperatorIntervention {
    pub action: String,
    #[serde(default)]
    pub run_id: Option<RunId>,
    #[serde(default = "crate::ids::empty_tenant_id")]
    pub tenant_id: TenantId,
    #[serde(default)]
    pub reason: String,
    #[serde(default)]
    pub intervened_at_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperatorProfileCreated {
    pub tenant_id: TenantId,
    pub profile_id: crate::ids::OperatorId,
    pub display_name: String,
    pub email: String,
    pub role: crate::tenancy::WorkspaceRole,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperatorProfileUpdated {
    pub tenant_id: TenantId,
    pub profile_id: crate::ids::OperatorId,
    pub display_name: Option<String>,
    pub email: Option<String>,
    /// RFC 026 PR-A2: operator `WorkspaceRole` is mutable from the
    /// admin PATCH. `None` leaves the stored role alone; `Some(role)`
    /// replaces it. `#[serde(default)]` keeps pre-A2 events (which
    /// omitted the field) replayable — they deserialize as `None` and
    /// therefore no-op on the role column.
    #[serde(default)]
    pub role: Option<crate::tenancy::WorkspaceRole>,
}

/// RFC 026 PR-A0: tenant-admin role granted to an operator.
///
/// Upserts one row in `operator_tenant_roles` keyed on
/// `(tenant_id, operator_id)`. Replay of a later grant over an earlier one
/// wins — the role string + `granted_by` + `granted_at_ms` update, and
/// `revoked_at_ms` / `revoked_by` are cleared so a re-grant supersedes an
/// earlier revocation.
///
/// `granted_by` is the authenticated principal id that approved the
/// grant: operator id for a tenant-admin delegating, or the string
/// `"system"` for a `CAIRN_ADMIN_TOKEN` promotion, or
/// `"upgrade-backfill"` for events emitted by the V066 migration.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TenantRoleGranted {
    pub tenant_id: TenantId,
    pub operator_id: crate::ids::OperatorId,
    pub role: crate::tenancy::TenantRole,
    pub granted_by: String,
    pub at_ms: u64,
}

/// RFC 026 PR-A0: tenant-admin role revoked from an operator.
///
/// Updates the `operator_tenant_roles` row keyed on
/// `(tenant_id, operator_id)` to record the revocation. The row is NOT
/// deleted — `revoked_at_ms` + `revoked_by` are set so the audit trail
/// survives, and a subsequent `TenantRoleGranted` re-grants by upserting
/// a fresh role and clearing `revoked_at_ms`.
///
/// Applying `TenantRoleRevoked` against a non-existent row is a no-op
/// (replay-safe — the projection keeps idempotency even if revocation is
/// replayed before the grant).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TenantRoleRevoked {
    pub tenant_id: TenantId,
    pub operator_id: crate::ids::OperatorId,
    pub revoked_by: String,
    pub at_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PauseScheduled {
    pub task_id: crate::ids::TaskId,
    pub resume_at_ms: u64,
    #[serde(default)]
    pub run_id: Option<RunId>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PermissionDecisionRecorded {
    pub decision_id: String,
    pub principal: String,
    pub action: String,
    pub resource: String,
    pub allowed: bool,
    pub recorded_at_ms: u64,
    #[serde(default)]
    pub invocation_id: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderBindingCreated {
    pub project: crate::tenancy::ProjectKey,
    pub provider_binding_id: crate::ids::ProviderBindingId,
    pub provider_connection_id: crate::ids::ProviderConnectionId,
    pub provider_model_id: crate::ids::ProviderModelId,
    pub operation_kind: crate::providers::OperationKind,
    pub settings: crate::providers::ProviderBindingSettings,
    pub policy_id: Option<String>,
    pub active: bool,
    pub created_at: u64,
    pub estimated_cost_micros: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderBindingStateChanged {
    pub project: crate::tenancy::ProjectKey,
    pub provider_binding_id: crate::ids::ProviderBindingId,
    pub active: bool,
    pub changed_at: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderBudgetAlertTriggered {
    pub budget_id: String,
    pub current_micros: u64,
    pub limit_micros: u64,
    pub triggered_at_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderBudgetExceeded {
    pub budget_id: String,
    pub exceeded_by_micros: u64,
    pub exceeded_at_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderConnectionRegistered {
    pub tenant: crate::tenancy::TenantKey,
    pub provider_connection_id: crate::ids::ProviderConnectionId,
    pub provider_family: String,
    pub adapter_type: String,
    /// Model identifiers served through this connection.
    #[serde(default)]
    pub supported_models: Vec<String>,
    pub status: crate::providers::ProviderConnectionStatus,
    pub registered_at: u64,
}

/// Projection directive emitted when the operator deletes a provider
/// connection. The in-memory projection hard-removes the row so the
/// `provider_connection_id` is free to be re-used immediately. Prior
/// `ProviderConnectionRegistered` events for the same ID are preserved
/// in the event log for audit. F40.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderConnectionDeleted {
    pub tenant: crate::tenancy::TenantKey,
    pub provider_connection_id: crate::ids::ProviderConnectionId,
    pub deleted_at: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderHealthChecked {
    pub tenant_id: TenantId,
    pub connection_id: crate::ids::ProviderConnectionId,
    pub status: crate::providers::ProviderHealthStatus,
    pub latency_ms: Option<u64>,
    pub checked_at_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderHealthScheduleSet {
    pub schedule_id: String,
    pub connection_id: crate::ids::ProviderConnectionId,
    pub tenant_id: TenantId,
    pub interval_ms: u64,
    pub enabled: bool,
    pub set_at_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderHealthScheduleTriggered {
    pub schedule_id: String,
    pub connection_id: crate::ids::ProviderConnectionId,
    pub tenant_id: TenantId,
    pub triggered_at_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderMarkedDegraded {
    pub tenant_id: TenantId,
    pub connection_id: crate::ids::ProviderConnectionId,
    pub reason: String,
    pub marked_at_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderModelRegistered {
    pub tenant_id: TenantId,
    pub connection_id: ProviderConnectionId,
    pub model_id: String,
    /// Serialized capabilities — stored as JSON string to maintain Eq on RuntimeEvent.
    pub capabilities_json: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderRecovered {
    pub tenant_id: TenantId,
    pub connection_id: crate::ids::ProviderConnectionId,
    pub recovered_at_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderRetryPolicySet {
    pub connection_id: crate::ids::ProviderConnectionId,
    pub tenant_id: crate::ids::TenantId,
    pub policy: crate::providers::RetryPolicy,
    pub set_at_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecoveryEscalated {
    pub task_id: crate::ids::TaskId,
    pub reason: String,
    pub escalated_at_ms: u64,
    #[serde(default)]
    pub run_id: Option<RunId>,
    #[serde(default)]
    pub last_error: Option<String>,
    #[serde(default)]
    pub attempt_count: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceShareRevoked {
    pub share_id: String,
    pub revoked_at_ms: u64,
    #[serde(default = "crate::ids::empty_tenant_id")]
    pub tenant_id: TenantId,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceShared {
    pub share_id: String,
    pub resource_type: String,
    #[serde(default)]
    pub grantee: String,
    pub shared_at_ms: u64,
    #[serde(default = "crate::ids::empty_tenant_id")]
    pub tenant_id: TenantId,
    #[serde(default = "crate::ids::empty_workspace_id")]
    pub source_workspace_id: WorkspaceId,
    #[serde(default = "crate::ids::empty_workspace_id")]
    pub target_workspace_id: WorkspaceId,
    #[serde(default)]
    pub resource_id: String,
    #[serde(default)]
    pub permissions: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoutePolicyCreated {
    pub tenant_id: TenantId,
    pub policy_id: String,
    pub name: String,
    pub rules: Vec<crate::providers::RoutePolicyRule>,
    /// Whether the policy is active at creation time (default: true).
    #[serde(default = "crate::events::default_true")]
    pub enabled: bool,
}

fn default_true() -> bool {
    true
}

/// Serde default for `LlmCompletionRecorded.tool_defs_json` — pre-fix
/// events replayed from the log deserialise with `"[]"` (a valid
/// JSON array) rather than `""` (invalid JSON). Keeps the API
/// contract stable for legacy traces and matches the SQL column
/// `DEFAULT '[]'` so the projection appliers don't need a
/// normalisation branch.
fn default_empty_json_array() -> String {
    "[]".to_owned()
}

/// #805: serde default for `RunReasoningStep.proposal_count` so
/// pre-#805 event-log entries replay as single-proposal iterations.
/// The pre-#805 emitter only ever wrote one `proposed_action`, so
/// counting them as 1 is the historically-correct value.
fn default_proposal_count() -> u32 {
    1
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoutePolicyUpdated {
    pub policy_id: String,
    pub updated_at_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunSlaBreached {
    pub run_id: RunId,
    pub tenant_id: TenantId,
    pub elapsed_ms: u64,
    pub target_ms: u64,
    pub breached_at_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunSlaSet {
    pub run_id: RunId,
    pub tenant_id: TenantId,
    pub target_completion_ms: u64,
    pub alert_at_percent: u8,
    pub set_at_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignalRouted {
    pub project: crate::tenancy::ProjectKey,
    pub signal_id: crate::ids::SignalId,
    pub subscription_id: String,
    pub delivered_at_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignalSubscriptionCreated {
    pub project: crate::tenancy::ProjectKey,
    pub subscription_id: String,
    pub signal_kind: String,
    pub target_run_id: Option<crate::ids::RunId>,
    pub target_mailbox_id: Option<String>,
    pub filter_expression: Option<String>,
    #[serde(default)]
    pub created_at_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TriggerSkipReason {
    ConditionMismatch,
    ChainTooDeep,
    AlreadyFired,
    MissingRequiredField { field: String },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TriggerSuspensionReason {
    RateLimitExceeded,
    BudgetExceeded,
    RepeatedFailures { failure_count: u32 },
    OperatorPaused,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TriggerCreated {
    pub project: ProjectKey,
    pub trigger_id: TriggerId,
    pub name: String,
    pub description: Option<String>,
    pub signal_type: String,
    pub plugin_id: Option<String>,
    pub conditions: Vec<serde_json::Value>,
    pub run_template_id: RunTemplateId,
    pub max_per_minute: u32,
    pub max_burst: u32,
    pub max_chain_depth: u8,
    pub created_by: OperatorId,
    pub created_at: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TriggerEnabled {
    pub project: ProjectKey,
    pub trigger_id: TriggerId,
    pub by: OperatorId,
    pub at: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TriggerDisabled {
    pub project: ProjectKey,
    pub trigger_id: TriggerId,
    pub by: OperatorId,
    pub reason: Option<String>,
    pub at: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TriggerSuspended {
    pub project: ProjectKey,
    pub trigger_id: TriggerId,
    pub reason: TriggerSuspensionReason,
    pub at: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TriggerResumed {
    pub project: ProjectKey,
    pub trigger_id: TriggerId,
    pub at: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TriggerDeleted {
    pub project: ProjectKey,
    pub trigger_id: TriggerId,
    pub by: OperatorId,
    pub at: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TriggerFired {
    pub project: ProjectKey,
    pub trigger_id: TriggerId,
    pub signal_id: SignalId,
    pub signal_type: String,
    pub run_id: RunId,
    pub chain_depth: u8,
    pub fired_at: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TriggerSkipped {
    pub project: ProjectKey,
    pub trigger_id: TriggerId,
    pub signal_id: SignalId,
    pub reason: TriggerSkipReason,
    pub skipped_at: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TriggerDenied {
    pub project: ProjectKey,
    pub trigger_id: TriggerId,
    pub signal_id: SignalId,
    pub decision_id: DecisionId,
    pub reason: String,
    pub denied_at: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TriggerRateLimited {
    pub project: ProjectKey,
    pub trigger_id: TriggerId,
    pub signal_id: SignalId,
    pub bucket_remaining: u32,
    pub bucket_capacity: u32,
    pub rate_limited_at: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TriggerPendingApproval {
    pub project: ProjectKey,
    pub trigger_id: TriggerId,
    pub signal_id: SignalId,
    pub approval_id: ApprovalId,
    pub pending_at: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RunTemplateCreated {
    pub project: ProjectKey,
    pub template_id: RunTemplateId,
    pub name: String,
    pub description: Option<String>,
    pub default_mode: crate::decisions::RunMode,
    pub system_prompt: String,
    pub initial_user_message: Option<String>,
    pub plugin_allowlist: Option<Vec<String>>,
    pub tool_allowlist: Option<Vec<String>>,
    pub budget_max_tokens: Option<u64>,
    pub budget_max_wall_clock_ms: Option<u64>,
    pub budget_max_iterations: Option<u32>,
    pub budget_exploration_budget_share: Option<f32>,
    pub sandbox_hint: Option<String>,
    pub required_fields: Vec<String>,
    pub created_by: OperatorId,
    pub created_at: u64,
}

impl Eq for RunTemplateCreated {}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunTemplateDeleted {
    pub project: ProjectKey,
    pub template_id: RunTemplateId,
    pub by: OperatorId,
    pub at: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotCreated {
    pub snapshot_id: String,
    pub created_at_ms: u64,
    #[serde(default = "crate::ids::empty_tenant_id")]
    pub tenant_id: TenantId,
    #[serde(default)]
    pub event_position: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskDependencyAdded {
    pub task_id: crate::ids::TaskId,
    pub depends_on: crate::ids::TaskId,
    pub added_at_ms: u64,
    /// Alias for task_id (the dependent task).
    #[serde(default = "crate::ids::empty_task_id")]
    pub dependent_task_id: crate::ids::TaskId,
    /// Alias for depends_on (the prerequisite task).
    #[serde(default = "crate::ids::empty_task_id")]
    pub depends_on_task_id: crate::ids::TaskId,
    /// Edge kind forwarded to FF. Default `SuccessOnly` so pre-0.2
    /// event-log entries deserialise.
    #[serde(default)]
    pub dependency_kind: crate::task_dependencies::DependencyKind,
    /// Opaque caller-supplied reference stored on the FF edge. `None`
    /// for pre-0.2 event-log entries and for callers that don't supply
    /// one.
    #[serde(default)]
    pub data_passing_ref: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskDependencyResolved {
    pub task_id: crate::ids::TaskId,
    pub prerequisite_id: crate::ids::TaskId,
    pub resolved_at_ms: u64,
    #[serde(default = "crate::ids::empty_task_id")]
    pub dependent_task_id: crate::ids::TaskId,
    #[serde(default = "crate::ids::empty_task_id")]
    pub depends_on_task_id: crate::ids::TaskId,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskLeaseExpired {
    pub task_id: crate::ids::TaskId,
    pub expired_at_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskPriorityChanged {
    pub task_id: crate::ids::TaskId,
    pub new_priority: u32,
    pub changed_at_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolInvocationProgressUpdated {
    pub invocation_id: ToolInvocationId,
    pub progress_pct: u8,
    pub message: Option<String>,
    pub updated_at_ms: u64,
}

/// Durable record of a single decision evaluation.
///
/// Persisting this event lets cairn-app rebuild the RFC 019 decision cache
/// on startup (RFC 020 §"Decision Cache Survival"). The payload carries
/// only the fields needed to reconstruct a cache entry plus an opaque
/// `event_json` blob with the full `cairn_domain::decisions::DecisionEvent`
/// so `GET /v1/decisions/{id}` can serve the reasoning chain post-replay.
///
/// `event_json` is stored as a JSON string rather than a typed structure
/// so the event log stays portable (SQLite TEXT / Postgres TEXT, no JSONB
/// operators) and to sidestep the fact that the richer `DecisionEvent`
/// enum does not implement `Eq` (its `CostEstimate` carries `f64`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DecisionRecorded {
    pub project: ProjectKey,
    pub decision_id: DecisionId,
    pub decision_key: crate::decisions::DecisionKey,
    pub outcome: crate::decisions::DecisionOutcome,
    /// `true` when the decision was written to the cache at step 7. Replay
    /// skips events where `cached == false` (never-cache policies,
    /// `cache_write: skip`).
    pub cached: bool,
    /// Cache TTL expiry in epoch-ms. `0` when `cached == false`.
    pub expires_at: u64,
    pub decided_at: u64,
    /// Full `DecisionEvent::DecisionRecorded` serialized via `serde_json`.
    /// Used by `get_decision` after replay so the reasoning chain is
    /// preserved across restarts.
    pub event_json: String,
}

/// Emitted once at the end of startup replay for the decision cache.
///
/// Counts how many cached decisions survived the restart and how many
/// were dropped because their TTL had already elapsed. RFC 020
/// §"Decision Cache Survival" requires this as audit-trail evidence
/// that the cache rebuild ran.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DecisionCacheWarmup {
    pub cached: u32,
    pub expired_and_dropped: u32,
    pub warmed_at: u64,
}

/// F47 PR2: annotates a completed run with the LLM's free-text summary
/// and the extractor-produced `CompletionVerification` sidecar.
///
/// Emitted after `runs.complete` has flipped the run to the terminal
/// state and after the orchestrator loop has returned
/// `LoopTermination::Completed`. Does not drive state transitions —
/// projections use it to populate nullable `completion_summary` /
/// `completion_verification_json` columns so the evidence survives
/// past the SSE `orchestrate_finished` frame.
///
/// Per-field `#[serde(default)]` keeps event logs written before this
/// variant existed deserialising cleanly: a legacy log simply has no
/// `RunCompletionAnnotated` entries, so every projected run shows
/// `completion: None` at the REST boundary.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunCompletionAnnotated {
    pub project: crate::tenancy::ProjectKey,
    pub session_id: crate::ids::SessionId,
    pub run_id: crate::ids::RunId,
    pub summary: String,
    #[serde(default)]
    pub verification: crate::orchestrator::CompletionVerification,
    pub occurred_at_ms: u64,
}

/// RFC 032 §2.3 event payload. Emitted when a run's
/// [`crate::completion_contracts::CompletionContract`] resolves —
/// explicitly on create / spawn, inferred from goal text at first
/// orchestrate, or re-inferred after a goal change.
///
/// PR-2 lands the struct. PR-4 wires emission at the orchestrate
/// handler. PR-2's projection-registry entry marks this `Ephemeral`
/// (SSE + trajectory only, no dedicated read-model table); Phase 2
/// may promote to `Projected` against a `completion_contracts`
/// table if operator dashboards need structured queries on the
/// resolved contract.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompletionContractResolved {
    pub project: crate::tenancy::ProjectKey,
    pub session_id: crate::ids::SessionId,
    pub run_id: crate::ids::RunId,
    /// The resolved contract. Serializes with its
    /// `#[serde(tag = "kind")]` discriminator; deserialization
    /// validates every typed newtype inside (RelPath, BoundedRegex,
    /// ContractSchema), so a malformed contract cannot reach the
    /// event log.
    pub contract: crate::completion_contracts::CompletionContract,
    /// How this contract arrived — explicit declaration, inference,
    /// or re-inference on goal change. Operator timeline uses this
    /// to distinguish "we inferred this" from "you declared this".
    pub source: crate::completion_contracts::ContractSource,
    /// Short hash of the goal text that triggered resolution. Used
    /// by PR-4's re-inference guard to decide whether the goal has
    /// changed since the last resolution. 16 hex chars = first 8
    /// bytes of a sha256 over the goal string.
    pub goal_hash: String,
    pub occurred_at_ms: u64,
}

/// F64: outcome of a single terminal-write recovery loop (the cairn-side
/// bridge for the FF#371 dual-door deadlock). Emitted once per complete/
/// fail/cancel call that enters the recovery loop, regardless of whether
/// the loop recovers or times out. Absent for the hot path (no recovery
/// needed) — the field on `RunRecord` stays `None` for normal runs.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TerminalRecoveryAttempted {
    pub project: crate::tenancy::ProjectKey,
    pub run_id: crate::ids::RunId,
    /// Which terminal FCALL the recovery loop was wrapping: `"complete"`,
    /// `"fail"`, or `"cancel"`.
    pub fcall: String,
    /// Number of re-claim + retry attempts the loop made. `>= 1`.
    pub attempts: u32,
    /// Wall-clock milliseconds spent inside the recovery loop (sum of
    /// backoff sleeps + FCALL round-trips).
    pub wall_time_ms: u64,
    /// Machine-readable recovery result. Kept as `String` (not a typed
    /// enum) so the audit log can carry less-common post-mortem
    /// shapes without a schema migration. Currently emitted values:
    ///
    /// * `"recovered"` — a retry inside the loop succeeded; the run
    ///   completed normally.
    /// * `"deadlocked"` — the backoff schedule exhausted and the F62
    ///   `TerminalWriteDeadlock` fallback fired.
    /// * `"non_transient_retry_error"` — the re-claim succeeded but
    ///   the terminal FCALL retry returned a non-transient error
    ///   (e.g. `NotFound`, permanent `Conflict`, `Validation`).
    /// * `"non_transient_reclaim_error"` — the re-claim itself
    ///   returned a non-transient error; the loop short-circuited.
    ///
    /// Operator dashboards should surface `"recovered"` and
    /// `"deadlocked"` prominently and treat the two non-transient
    /// strings as audit-only diagnostics.
    pub outcome: String,
    /// Wall-clock ms when the loop finished. Lets operator dashboards
    /// plot recovery incidents over time.
    pub occurred_at_ms: u64,
}

// ── F65: orchestrator session redesign (PR-1 foundation) ─────────────────────
//
// These event variants persist the observable milestones of an orchestrated
// session. PR-1 adds the shapes only; PR-2 begins wiring projection writers,
// PR-3 emits them from the circuit breaker path, PR-6 wires the summarizer.
//
// Every struct is `#[derive(..., Serialize, Deserialize)]` to match the
// existing convention. New fields here (and on existing extended shapes)
// carry `#[serde(default)]` so legacy event logs continue to replay cleanly.

/// F65: a new session attempt started.
///
/// Emitted each time the orchestrator begins executing a session — on
/// initial start and on every retry up to `max_attempts`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionAttemptStarted {
    pub project: crate::tenancy::ProjectKey,
    pub session_id: crate::ids::SessionId,
    pub root_run_id: crate::ids::RunId,
    /// 1-based attempt number within the session.
    pub attempt_number: u32,
    /// Configured max attempts at the time this attempt started. Captured on
    /// each attempt so log replay is resilient to later config changes.
    pub max_attempts: u32,
    pub at_ms: u64,
}

/// F65: a session attempt finished (terminal or retrying).
///
/// `outcome_kind` mirrors the `TerminationReason` discriminator as a string so
/// the event log remains stable even if the enum grows; the rich outcome is
/// emitted via [`SessionOutcomeEmitted`] when the entire session closes.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionAttemptCompleted {
    pub project: crate::tenancy::ProjectKey,
    pub session_id: crate::ids::SessionId,
    pub root_run_id: crate::ids::RunId,
    /// Machine-readable discriminator of the [`crate::session_orchestration::TerminationReason`].
    /// Example values: `complete_run`, `circuit_breaker_tripped`, `lease_lost`,
    /// `provider_error`, `operator_cancel`, `crashed`.
    pub outcome_kind: String,
    pub at_ms: u64,
}

/// F65: a circuit breaker fired.
///
/// May or may not cause the session attempt to terminate — the orchestrator
/// decides based on the breaker kind and session policy. When it does, the
/// trip also appears inside [`SessionOutcomeEmitted`] via `TerminationReason`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CircuitBreakerTripped {
    pub project: crate::tenancy::ProjectKey,
    pub session_id: crate::ids::SessionId,
    pub run_id: crate::ids::RunId,
    pub trip: crate::session_orchestration::CircuitBreakerTrip,
    pub at_ms: u64,
}

/// F65: operator-warning-level budget notification.
///
/// Emitted before a breaker actually trips when a configurable warning
/// threshold is crossed (e.g. 80 % of the token cap). Non-terminal: the
/// session continues to run. PR-3 wires the emission; PR-1 defines the shape.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BudgetThresholdCrossed {
    pub project: crate::tenancy::ProjectKey,
    pub session_id: crate::ids::SessionId,
    pub run_id: crate::ids::RunId,
    /// Which breaker the threshold relates to (see [`crate::session_orchestration::BreakerKind`]).
    pub which_breaker: crate::session_orchestration::BreakerKind,
    pub measured: u64,
    pub limit: u64,
    /// Fraction of the limit that was measured, expressed in **basis points**
    /// (0-10_000 spans 0 %–100 %). Integer storage keeps the event `Eq`-able
    /// and avoids NaN-flavoured equality issues on replay.
    pub ratio_bps: u32,
    pub at_ms: u64,
}

/// F65: a checkpoint was persisted by the orchestrator.
///
/// The body of the checkpoint lives outside the event payload (in the
/// checkpoint projection); the event carries only identity + iteration so
/// consumers can correlate without loading large blobs.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckpointPersisted {
    pub project: crate::tenancy::ProjectKey,
    pub checkpoint_id: crate::ids::CheckpointId,
    pub session_id: crate::ids::SessionId,
    pub root_run_id: crate::ids::RunId,
    pub iteration: u32,
    pub at_ms: u64,
}

/// F65: a workspace filesystem snapshot was created.
///
/// The event intentionally does **not** carry `snapshot_path` — that is host-
/// local filesystem detail that belongs to the workspace projection, not the
/// portable event log. Readers who need the path resolve it via
/// [`crate::session_orchestration::WorkspaceSnapshot`].
///
/// `bytes`, `reflink_used`, and `parent_snapshot_id` ARE carried on the
/// event so a fresh log replay from an empty projection store rebuilds
/// the `workspace_snapshots` row with the same metadata the live writer
/// produced — satisfying the CLAUDE.md "all state derives from the
/// event log" invariant. See #482 for the gap this closes. Before this
/// change, the projection inserted the row with `bytes=0` /
/// `reflink_used=false` / `parent_snapshot_id=NULL` and waited for an
/// out-of-band `WorkspaceSnapshotWriter::stamp_metadata` call to fill
/// them — a call that never fires during replay.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceSnapshotCreated {
    pub project: crate::tenancy::ProjectKey,
    pub snapshot_id: crate::ids::WorkspaceSnapshotId,
    pub workspace_id: crate::ids::WorkspaceId,
    pub session_id: crate::ids::SessionId,
    pub at_ms: u64,
    /// Byte count of the persisted reflink snapshot directory at the
    /// moment it was finalised. `0` is a valid value (empty workspace);
    /// it means the live writer observed no files, not "data unknown".
    /// `#[serde(default)]` keeps the event deserialisable against older
    /// rows that pre-date #482 (0 is the documented legacy value).
    #[serde(default)]
    pub bytes: u64,
    /// `true` when the snapshot used a reflink (O(1) CoW) rather than
    /// a full copy. Tracks per-project backend capability so
    /// post-incident audit can correlate snapshot provenance with
    /// `SandboxBackendDegraded` events.
    #[serde(default)]
    pub reflink_used: bool,
    /// On the resume path, the snapshot the new one derives from.
    /// `None` for a fresh-provision snapshot. Rebuilds the snapshot
    /// lineage on replay (used by the F65 GC policy to defer reap
    /// until descendants are also reaped).
    #[serde(default)]
    pub parent_snapshot_id: Option<crate::ids::WorkspaceSnapshotId>,
}

/// F65: a workspace snapshot was reaped by the GC.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceSnapshotReaped {
    pub project: crate::tenancy::ProjectKey,
    pub snapshot_id: crate::ids::WorkspaceSnapshotId,
    pub at_ms: u64,
}

/// F65: the session's rich terminal outcome was emitted.
///
/// One per session (not per attempt). Downstream consumers (summarizer chain,
/// operator UI, next-session seeder) read this off the event log without
/// coordinating with in-flight services.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionOutcomeEmitted {
    pub project: crate::tenancy::ProjectKey,
    pub session_id: crate::ids::SessionId,
    pub root_run_id: crate::ids::RunId,
    pub outcome: crate::session_orchestration::SessionOutcome,
    pub at_ms: u64,
}

/// F65: the orchestrator made a high-level control-plane decision.
///
/// Used to record the "should I retry, checkpoint, stop?" signal the
/// orchestrator emits at end-of-attempt, separate from the attempt's
/// termination reason. `decision` is a short string tag (`retry`, `stop`,
/// `checkpoint_only`, `escalate`) to keep the payload shape stable across
/// future policy changes.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrchestratorDecisionMade {
    pub project: crate::tenancy::ProjectKey,
    pub session_id: crate::ids::SessionId,
    pub decision: String,
    pub at_ms: u64,
}

/// F65: the LLM-backed summarizer was unavailable at outcome time and the
/// orchestrator fell back to a deterministic summary. Surfaced to operators
/// so the provenance of `compacted_summary` is auditable.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SummarizerFallback {
    pub project: crate::tenancy::ProjectKey,
    pub session_id: crate::ids::SessionId,
    /// Short machine-readable reason code (e.g. `provider_unavailable`,
    /// `budget_exceeded`, `configuration_missing`).
    pub reason: String,
    pub at_ms: u64,
}

/// F65: the workspace backend picked a degraded mode (e.g. ext4 copy fallback
/// when overlayfs + reflink were unavailable) for the session's snapshots.
/// Operator-visible so capacity and performance regressions are explainable.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceBackendDegraded {
    pub project: crate::tenancy::ProjectKey,
    pub session_id: crate::ids::SessionId,
    /// Backend selected after degradation (e.g. `ext4_copy`).
    pub backend: String,
    /// Reason for the downgrade (e.g. `overlayfs_unavailable`,
    /// `reflink_unsupported_fs`).
    pub reason: String,
    pub at_ms: u64,
}

/// F65 PR-5 (#359): the crash-recovery sweep detected a dangling overlayfs
/// mount whose owning cairn-app process did not cleanly unmount it before
/// exiting, and successfully unmounted it via `umount2(MNT_DETACH)`.
///
/// Emitted exactly once per recovered mount so operators get a distinct
/// alerting surface for crash-derived state (orthogonal to
/// `WorkspaceBackendDegraded`, which signals per-snapshot FS quality
/// degradation). The bound `session_id` / `run_id` come from the recovery
/// registry sidecar; operators see which session's mount survived the
/// crash.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SandboxCrashRecovered {
    pub project: crate::tenancy::ProjectKey,
    pub session_id: crate::ids::SessionId,
    pub run_id: crate::ids::RunId,
    pub at_ms: u64,
}

// ── RFC 029 pluggable knowledge providers ──────────────────────────────────

/// Snapshot of the effective-capability data a plugin returned at its most
/// recent `initialize` handshake, stored per-project for visibility decisions
/// and capability-change audit. Intentionally minimal — full capability data
/// lives in the plugin host's process-state, not in the event log.
///
/// RFC 030 reuses this shape for both capability families. `auto_extract`
/// is meaningful only on memory-family snapshots (maps to
/// `MemoryProviderCapability.auto_extract`); knowledge-family snapshots
/// serialize `None`. Handled via `#[serde(default)]` so pre-RFC-030
/// payloads — all knowledge-family — replay unchanged.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolvedProviderSnapshot {
    /// Plugin-declared id, or `"cairn-default"` for the in-process default.
    pub provider_id: String,
    /// Whether the provider can accept `knowledge.ingest` / `memory.ingest`
    /// calls right now.
    pub ingest_capable: bool,
    /// Retrieval modes the provider currently supports (subset of
    /// `{"lexical_only", "vector_only", "hybrid"}`).
    pub retrieval_modes: Vec<String>,
    /// Provider-required scoring dimensions the provider surfaces right now.
    /// Values from `{"semantic_relevance", "lexical_relevance",
    /// "freshness_decay", "staleness_penalty", "recency_of_use"}`. Runtime-
    /// owned dimensions (graph_proximity, source_credibility, corroboration)
    /// are never included — the runtime always computes those post-hoc.
    pub scoring_dimensions_surfaced: Vec<String>,
    /// RFC 030: set to `Some(true)` for memory-family providers that
    /// auto-extract memories from conversation turns (mem0 post-turn-hook
    /// style). When true, the runtime suppresses the `memory_store` tool
    /// from the agent prompt — the agent does not call `memory.ingest`
    /// explicitly; the provider picks up context on its own. `None` on
    /// knowledge-family snapshots and on pre-RFC-030 payloads.
    #[serde(default)]
    pub auto_extract: Option<bool>,
}

/// Project configured or re-configured its knowledge provider. Upserts the
/// current-configuration row on `project_knowledge_providers`
/// (`kind = "configured"`).
///
/// RFC 030 adds `is_bootstrap: bool` — set to `true` exactly once per
/// project when `ProjectCreated` (or the V019 backfill sweep for pre-RFC-030
/// projects) emits the initial cairn-default binding. Operator-driven
/// re-configurations set it to `false`. Lets the operator UI + audit tooling
/// distinguish "default, never touched" from "deliberately set to
/// cairn-default after trying something else".
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct KnowledgeProviderConfigured {
    pub project: crate::tenancy::ProjectKey,
    pub provider_ref: crate::ids::ProviderRef,
    pub configured_by: crate::ids::OperatorId,
    /// `true` iff this is the initial bootstrap binding (`ProjectCreated`
    /// emission or V019 backfill). Default `false` on deserialisation so
    /// pre-RFC-030 events replay unchanged. Added by RFC 030.
    #[serde(default)]
    pub is_bootstrap: bool,
    pub at_ms: u64,
}

/// Query-time failure: the configured provider is unreachable, failed its
/// handshake, or has no credentials. Audit row on
/// `project_knowledge_providers` (`kind = "unavailable"`). Never upserts over
/// the `configured` row — pure insert for operator audit.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct KnowledgeProviderUnavailable {
    pub project: crate::tenancy::ProjectKey,
    pub provider_ref: crate::ids::ProviderRef,
    /// Free-form short reason string for operator UI.
    pub reason: String,
    pub at_ms: u64,
}

/// Plugin restart produced a handshake snapshot that differs from the
/// previous spawn (e.g., credentials changed → `ingest_capable` flipped, or
/// a scoring dimension moved between `surfaced` and `not_supported`). Audit
/// row on `project_knowledge_providers` (`kind = "capability_changed"`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct KnowledgeProviderCapabilityChanged {
    pub project: crate::tenancy::ProjectKey,
    pub provider_ref: crate::ids::ProviderRef,
    pub prior: ResolvedProviderSnapshot,
    pub current: ResolvedProviderSnapshot,
    pub at_ms: u64,
}

/// Knowledge-document ingest kicked off. Insert on `knowledge_ingest_jobs`
/// with `status = "submitted"`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct KnowledgeIngestSubmitted {
    pub project: crate::tenancy::ProjectKey,
    pub provider_ref: crate::ids::ProviderRef,
    pub document_id: crate::ids::KnowledgeDocumentId,
    /// Mirrors `cairn_memory::ingest::SourceType`; stored as the lowercase
    /// snake_case serde representation (e.g. `"markdown"`, `"plain_text"`).
    pub source_type: String,
    pub at_ms: u64,
}

/// Ingest refused before dispatch (typically because the resolved provider
/// declared `ingest_capable = false`). Insert on `knowledge_ingest_jobs`
/// with `status = "rejected"`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct KnowledgeIngestRejected {
    pub project: crate::tenancy::ProjectKey,
    pub provider_ref: crate::ids::ProviderRef,
    /// Free-form short reason string for operator UI.
    pub reason: String,
    pub at_ms: u64,
}

/// RFC 030 §Rollout: boot-time scan detected a family mismatch on a
/// project's provider slot. Emitted once per offending project per
/// boot; the scan is idempotent on the event log because the emission
/// is driven by the plugin host's handshake snapshot, not by a
/// deduplicating stream read.
///
/// `observed_family` is the family the plugin actually declared at its
/// most recent handshake; `configured_slot` is the slot the operator
/// assigned it via `PUT /knowledge-provider` or `PUT /memory-provider`.
/// A mismatch is an operator misconfigured a memory-only adapter
/// (e.g. mem0) on the knowledge slot — the runtime doesn't auto-fix
/// because the correct remediation is slot-specific.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct KnowledgeProviderFamilyMismatch {
    pub project: crate::tenancy::ProjectKey,
    pub provider_ref: crate::ids::ProviderRef,
    /// `"memory_provider"` — what the plugin claimed at handshake.
    /// Stored as the snake_case serde repr for cross-language
    /// consumers.
    pub observed_family: String,
    /// Always `"knowledge_provider"` for this variant. Kept explicit
    /// so operator tooling doesn't have to know which slot the event
    /// is about from the variant name alone.
    pub configured_slot: String,
    pub at_ms: u64,
}

/// Memory-slot twin of `KnowledgeProviderFamilyMismatch`.
/// `observed_family` is `"knowledge_provider"`, `configured_slot` is
/// `"memory_provider"`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemoryProviderFamilyMismatch {
    pub project: crate::tenancy::ProjectKey,
    pub provider_ref: crate::ids::ProviderRef,
    pub observed_family: String,
    pub configured_slot: String,
    pub at_ms: u64,
}

/// Ingest status transition reported by the provider (or generated by
/// cairn-default as it passes through its pipeline). Updates the row on
/// `knowledge_ingest_jobs` keyed by `(project, document_id)`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct KnowledgeIngestStatusUpdated {
    pub project: crate::tenancy::ProjectKey,
    pub provider_ref: crate::ids::ProviderRef,
    pub document_id: crate::ids::KnowledgeDocumentId,
    /// Mirrors `cairn_memory::ingest::IngestStatus`; stored as the
    /// snake_case serde representation (e.g. `"completed"`, `"failed"`).
    pub status: String,
    pub at_ms: u64,
}

// ── RFC 030: memory-provider lifecycle events ────────────────────────────
//
// Structural twins of the RFC 029 knowledge events, projected to a parallel
// pair of tables (`project_memory_providers`, `memory_ingest_jobs`). The
// types are distinct so code routing on capability family cannot
// accidentally feed a knowledge payload into a memory path and vice versa.
// See RFC 030 §Event-Sourcing Delta.

/// Project configured or re-configured its memory provider. Upserts the
/// current-configuration row on `project_memory_providers`
/// (`kind = "configured"`).
///
/// `is_bootstrap: bool` is set to `true` exactly once per project when
/// `ProjectCreated` (or the V019 backfill) emits the initial cairn-default
/// binding; operator-driven re-configurations set it to `false`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemoryProviderConfigured {
    pub project: crate::tenancy::ProjectKey,
    pub provider_ref: crate::ids::ProviderRef,
    pub configured_by: crate::ids::OperatorId,
    /// `true` iff this is the initial bootstrap binding. Default `false`
    /// on deserialisation so the projection handler treats older payloads
    /// (if any slip through the migration window) as operator-driven.
    #[serde(default)]
    pub is_bootstrap: bool,
    pub at_ms: u64,
}

/// Query-time failure: the configured memory provider is unreachable,
/// failed its handshake, or has no credentials. Audit row on
/// `project_memory_providers` (`kind = "unavailable"`). Never upserts over
/// the `configured` row.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemoryProviderUnavailable {
    pub project: crate::tenancy::ProjectKey,
    pub provider_ref: crate::ids::ProviderRef,
    /// Free-form short reason string for operator UI.
    pub reason: String,
    pub at_ms: u64,
}

/// Plugin restart produced a handshake snapshot that differs from the
/// previous spawn (e.g., `auto_extract` flipped, `ingest_capable` flipped,
/// a scoring dimension moved between `surfaced` and `not_supported`).
/// Audit row on `project_memory_providers` (`kind = "capability_changed"`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemoryProviderCapabilityChanged {
    pub project: crate::tenancy::ProjectKey,
    pub provider_ref: crate::ids::ProviderRef,
    pub prior: ResolvedProviderSnapshot,
    pub current: ResolvedProviderSnapshot,
    pub at_ms: u64,
}

/// Memory-document ingest kicked off (cairn-default or an explicit
/// `memory_store` call when the backend declares `auto_extract = false`).
/// Insert on `memory_ingest_jobs` with `status = "submitted"`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemoryIngestSubmitted {
    pub project: crate::tenancy::ProjectKey,
    pub provider_ref: crate::ids::ProviderRef,
    pub document_id: crate::ids::DocumentId,
    /// Mirrors `cairn_memory::ingest::SourceType` via its snake_case serde
    /// repr (e.g. `"plain_text"`, `"structured_json"`).
    pub source_type: String,
    pub at_ms: u64,
}

/// Ingest refused before dispatch (typically because the resolved memory
/// provider declared `auto_extract = true`, so `memory_store` was
/// suppressed, or `ingest_capable = false`). Insert on `memory_ingest_jobs`
/// with `status = "rejected"`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemoryIngestRejected {
    pub project: crate::tenancy::ProjectKey,
    pub provider_ref: crate::ids::ProviderRef,
    /// Free-form short reason string for operator UI.
    pub reason: String,
    pub at_ms: u64,
}

/// Ingest status transition reported by the memory provider. Updates the
/// row on `memory_ingest_jobs` keyed by `(project, document_id)`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemoryIngestStatusUpdated {
    pub project: crate::tenancy::ProjectKey,
    pub provider_ref: crate::ids::ProviderRef,
    pub document_id: crate::ids::DocumentId,
    /// Mirrors `cairn_memory::ingest::IngestStatus` via its snake_case
    /// serde repr (e.g. `"completed"`, `"failed"`).
    pub status: String,
    pub at_ms: u64,
}

// ── RFC 031 operator-defined agent roles ─────────────────────────────

/// An operator created or updated a per-project agent role. Upserts a
/// row on `project_agent_roles`; §D6 latest-wins semantic.
///
/// `shadows_builtin` is `Some("reviewer")` etc. when the role id
/// matches a built-in id (operator is shadowing the built-in per §D2);
/// `None` when the id is novel. The UI uses this to render `source =
/// custom_shadow` vs `custom`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentRoleDefined {
    pub project: crate::tenancy::ProjectKey,
    pub role: crate::agent_roles::AgentRole,
    #[serde(default)]
    pub shadows_builtin: Option<String>,
    pub defined_by: crate::ids::OperatorId,
    pub at_ms: u64,
}

/// An operator retracted a per-project agent role. Sets
/// `retracted_at` on the `project_agent_roles` row; subsequent
/// `resolve(&project, role_id)` calls fall through to the built-in
/// (if the id shadows one) or the generic role verbatim (§D7).
///
/// Running orchestrations are NOT interrupted (§D7) — they've
/// already resolved their role for the run. New runs after the
/// retract use the fallback.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentRoleRetracted {
    pub project: crate::tenancy::ProjectKey,
    pub role_id: String,
    pub retracted_by: crate::ids::OperatorId,
    pub at_ms: u64,
}

/// Observability advisory: the orchestrator's DECIDE-phase allowlist
/// filter found a tool id declared in `role.tools` that is not
/// currently registered in the tool registry. Emitted once per
/// `(run_id, role_id, tool_id)` per run; deduplication lives on the
/// run's `OrchestrationContext::declared_but_missing` HashSet
/// (`Arc<Mutex<...>>` so clones share state, per RFC 031 §Runtime
/// Resolution Delta).
///
/// Ephemeral — not projected. The missing tool is silently absent
/// from the tool set the LLM sees; this event is the operator-
/// facing signal.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolDeclaredButMissing {
    pub project: crate::tenancy::ProjectKey,
    pub run_id: crate::ids::RunId,
    pub role_id: String,
    pub tool_id: String,
    pub at_ms: u64,
}

#[cfg(test)]
mod tests {
    use super::{
        ApprovalRequested, EventEnvelope, EventSource, ExternalWorkerReported, RuntimeEvent,
        SessionCreated, TaskCreated, ToolInvocationFailed, ToolInvocationStarted,
        UserMessageAppended,
    };
    use crate::ids::{ApprovalId, CommandId, EventId, RunId, TaskId};
    use crate::policy::ExecutionClass;
    use crate::tenancy::{OwnershipKey, ProjectKey};
    use crate::tool_invocation::ToolInvocationTarget;
    use crate::workers::ExternalWorkerReport;

    #[test]
    fn runtime_event_envelope_carries_project_ownership() {
        let project = ProjectKey::new("tenant", "workspace", "project");
        let event = EventEnvelope::for_runtime_event(
            EventId::new("evt_1"),
            EventSource::Runtime,
            RuntimeEvent::SessionCreated(SessionCreated {
                project,
                session_id: "session_1".into(),
            }),
        )
        .with_correlation_id("corr_1");

        assert!(matches!(event.payload, RuntimeEvent::SessionCreated(_)));
        assert!(matches!(event.ownership, OwnershipKey::Project(_)));
    }

    #[test]
    fn event_envelope_builders_set_causation_and_correlation() {
        let event = EventEnvelope::new(
            EventId::new("evt_2"),
            EventSource::Runtime,
            ProjectKey::new("tenant", "workspace", "project"),
            RuntimeEvent::SessionCreated(SessionCreated {
                project: ProjectKey::new("tenant", "workspace", "project"),
                session_id: "session_2".into(),
            }),
        )
        .with_causation_id(CommandId::new("cmd_2"))
        .with_correlation_id("corr_2");

        assert_eq!(
            event.causation_id.as_ref().map(|id| id.as_str()),
            Some("cmd_2")
        );
        assert_eq!(event.correlation_id.as_deref(), Some("corr_2"));
    }

    #[test]
    fn runtime_event_envelope_carries_tool_invocation_payload() {
        let project = ProjectKey::new("tenant", "workspace", "project");
        let event = EventEnvelope {
            event_id: EventId::new("evt_tool_1"),
            source: EventSource::Runtime,
            ownership: OwnershipKey::Project(project.clone()),
            causation_id: None,
            correlation_id: Some("corr_tool_1".to_owned()),
            payload: RuntimeEvent::ToolInvocationStarted(ToolInvocationStarted {
                project,
                invocation_id: "inv_1".into(),
                session_id: Some("session_1".into()),
                run_id: Some("run_1".into()),
                task_id: Some("task_1".into()),
                target: ToolInvocationTarget::Plugin {
                    plugin_id: "com.example.git".to_owned(),
                    tool_name: "git.status".to_owned(),
                },
                execution_class: ExecutionClass::SandboxedProcess,
                prompt_release_id: None,
                requested_at_ms: 10,
                started_at_ms: 11,
                args_json: None,
            }),
        };

        assert!(matches!(
            event.payload,
            RuntimeEvent::ToolInvocationStarted(_)
        ));
    }

    #[test]
    fn runtime_event_envelope_carries_external_worker_payload() {
        let project = ProjectKey::new("tenant", "workspace", "project");
        let event = EventEnvelope {
            event_id: EventId::new("evt_worker_1"),
            source: EventSource::ExternalWorker {
                worker: "worker_1".to_owned(),
            },
            ownership: OwnershipKey::Project(project.clone()),
            causation_id: None,
            correlation_id: Some("corr_worker_1".to_owned()),
            payload: RuntimeEvent::ExternalWorkerReported(ExternalWorkerReported {
                report: ExternalWorkerReport {
                    project,
                    worker_id: "worker_1".into(),
                    run_id: Some("run_1".into()),
                    task_id: "task_1".into(),
                    lease_token: 3,
                    reported_at_ms: 99,
                    progress: None,
                    outcome: None,
                },
            }),
        };

        assert!(matches!(
            event.payload,
            RuntimeEvent::ExternalWorkerReported(_)
        ));
    }

    #[test]
    fn runtime_event_reports_project_and_primary_entity() {
        let event = RuntimeEvent::ToolInvocationFailed(ToolInvocationFailed {
            project: ProjectKey::new("tenant", "workspace", "project"),
            invocation_id: "inv_8".into(),
            task_id: Some("task_8".into()),
            tool_name: "fs.write".to_owned(),
            finished_at_ms: 14,
            outcome: crate::tool_invocation::ToolInvocationOutcomeKind::PermanentFailure,
            error_message: Some("bad input".to_owned()),
            output_preview: None,
        });

        assert_eq!(event.project().project_id.as_str(), "project");
        assert!(matches!(
            event.primary_entity_ref(),
            Some(crate::errors::RuntimeEntityRef::ToolInvocation { .. })
        ));
    }

    #[test]
    fn task_and_approval_events_already_carry_identity_for_enrichment() {
        let project = ProjectKey::new("tenant", "workspace", "project");
        let task_event = RuntimeEvent::TaskCreated(TaskCreated {
            project: project.clone(),
            task_id: TaskId::new("task_9"),
            parent_run_id: None,
            parent_task_id: None,
            prompt_release_id: None,
            session_id: None,
        });
        let approval_event = RuntimeEvent::ApprovalRequested(ApprovalRequested {
            project: project.clone(),
            approval_id: ApprovalId::new("approval_9"),
            run_id: None,
            task_id: Some(TaskId::new("task_9")),
            requirement: crate::policy::ApprovalRequirement::Required,
            title: None,
            description: None,
        });

        assert_eq!(task_event.project(), &project);
        assert_eq!(approval_event.project(), &project);
        assert!(matches!(
            task_event.primary_entity_ref(),
            Some(crate::errors::RuntimeEntityRef::Task { .. })
        ));
        assert!(matches!(
            approval_event.primary_entity_ref(),
            Some(crate::errors::RuntimeEntityRef::Approval { .. })
        ));
        assert_eq!(
            EventEnvelope::for_runtime_event(
                EventId::new("evt_task_9"),
                EventSource::Runtime,
                task_event
            )
            .project(),
            &project
        );
        assert_eq!(
            EventEnvelope::for_runtime_event(
                EventId::new("evt_approval_9"),
                EventSource::Runtime,
                approval_event
            )
            .project(),
            &project
        );
    }

    #[test]
    fn event_envelope_reports_project_and_primary_entity() {
        let event = EventEnvelope::for_runtime_event(
            EventId::new("evt_3"),
            EventSource::Runtime,
            RuntimeEvent::SessionCreated(SessionCreated {
                project: ProjectKey::new("tenant", "workspace", "project"),
                session_id: "session_3".into(),
            }),
        );

        assert_eq!(event.project().project_id.as_str(), "project");
        assert!(matches!(
            event.primary_entity_ref(),
            Some(crate::errors::RuntimeEntityRef::Session { .. })
        ));
    }

    #[test]
    fn user_message_appended_reports_project_and_run_entity() {
        let project = ProjectKey::new("tenant", "workspace", "project");
        let event = RuntimeEvent::UserMessageAppended(UserMessageAppended {
            project: project.clone(),
            session_id: "session_10".into(),
            run_id: RunId::new("run_10"),
            content: String::new(),
            sequence: 0,
            appended_at_ms: 0,
        });

        assert_eq!(event.project(), &project);
        assert!(matches!(
            event.primary_entity_ref(),
            Some(crate::errors::RuntimeEntityRef::Run { ref run_id }) if run_id.as_str() == "run_10"
        ));
    }

    // ── PR BP-1: tool-call approval foundation events ─────────────────────
    //
    // Strictly additive: these tests pin the JSON wire shape for the 4 new
    // variants + assert `project()` returns the carried `ProjectKey` and
    // `primary_entity_ref()` returns `None` (ToolCallId is not yet a
    // `RuntimeEntityRef` variant; that is PR BP-2+ scope).

    #[test]
    fn tool_call_proposed_roundtrips_and_reports_project() {
        let project = ProjectKey::new("t", "w", "p");
        let event = RuntimeEvent::ToolCallProposed(super::ToolCallProposed {
            project: project.clone(),
            call_id: crate::ids::ToolCallId::new("tc_1"),
            session_id: "sess_1".into(),
            run_id: RunId::new("run_1"),
            tool_name: "read_file".to_owned(),
            tool_args: serde_json::json!({"path": "/tmp/x"}),
            display_summary: "Read /tmp/x".to_owned(),
            match_policy: crate::approvals::ApprovalMatchPolicy::Exact,
            proposed_at_ms: 42,
        });
        assert_eq!(event.project(), &project);
        assert!(event.primary_entity_ref().is_none());
        let json = serde_json::to_string(&event).expect("serialize");
        let back: RuntimeEvent = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(event, back);
    }

    #[test]
    fn tool_call_approved_roundtrips_and_reports_project() {
        let project = ProjectKey::new("t", "w", "p");
        let event = RuntimeEvent::ToolCallApproved(super::ToolCallApproved {
            project: project.clone(),
            call_id: crate::ids::ToolCallId::new("tc_2"),
            session_id: "sess_2".into(),
            operator_id: crate::ids::OperatorId::new("op_1"),
            scope: crate::approvals::ApprovalScope::Session {
                match_policy: crate::approvals::ApprovalMatchPolicy::ProjectScopedPath {
                    project_root: "/w/p".to_owned(),
                },
            },
            approved_tool_args: Some(serde_json::json!({"path": "/w/p/file.rs"})),
            approved_at_ms: 1_000,
        });
        assert_eq!(event.project(), &project);
        assert!(event.primary_entity_ref().is_none());
        let json = serde_json::to_string(&event).expect("serialize");
        let back: RuntimeEvent = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(event, back);
    }

    #[test]
    fn tool_call_rejected_roundtrips_and_reports_project() {
        let project = ProjectKey::new("t", "w", "p");
        let event = RuntimeEvent::ToolCallRejected(super::ToolCallRejected {
            project: project.clone(),
            call_id: crate::ids::ToolCallId::new("tc_3"),
            session_id: "sess_3".into(),
            operator_id: crate::ids::OperatorId::new("op_1"),
            reason: Some("unsafe".to_owned()),
            rejected_at_ms: 2_000,
        });
        assert_eq!(event.project(), &project);
        assert!(event.primary_entity_ref().is_none());
        let json = serde_json::to_string(&event).expect("serialize");
        let back: RuntimeEvent = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(event, back);
    }

    #[test]
    fn tool_call_amended_roundtrips_and_reports_project() {
        let project = ProjectKey::new("t", "w", "p");
        let event = RuntimeEvent::ToolCallAmended(super::ToolCallAmended {
            project: project.clone(),
            call_id: crate::ids::ToolCallId::new("tc_4"),
            session_id: "sess_4".into(),
            operator_id: crate::ids::OperatorId::new("op_1"),
            new_tool_args: serde_json::json!({"path": "/tmp/y"}),
            amended_at_ms: 3_000,
        });
        assert_eq!(event.project(), &project);
        assert!(event.primary_entity_ref().is_none());
        let json = serde_json::to_string(&event).expect("serialize");
        let back: RuntimeEvent = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(event, back);
    }

    #[test]
    fn tool_call_events_use_snake_case_discriminator() {
        let event = RuntimeEvent::ToolCallProposed(super::ToolCallProposed {
            project: ProjectKey::new("t", "w", "p"),
            call_id: crate::ids::ToolCallId::new("tc_x"),
            session_id: "s".into(),
            run_id: RunId::new("r"),
            tool_name: "read_file".to_owned(),
            tool_args: serde_json::json!({}),
            display_summary: String::new(),
            match_policy: crate::approvals::ApprovalMatchPolicy::Exact,
            proposed_at_ms: 0,
        });
        let json = serde_json::to_string(&event).expect("serialize");
        assert!(
            json.contains("\"event\":\"tool_call_proposed\""),
            "expected snake_case event discriminator, got {json}"
        );
    }

    #[test]
    fn llm_completion_recorded_legacy_event_deserializes_tool_defs_as_valid_json_array() {
        // Pre-fix events on the event log don't carry `tool_defs_json`.
        // Gemini review on #703 caught that a naked `#[serde(default)]`
        // would resolve to `String::default()` == "" — which is NOT
        // valid JSON and contradicts both the SQL column `DEFAULT '[]'`
        // and the API-consumer expectation that `tool_defs_json` can
        // always be `JSON.parse`d.
        //
        // The fix: `#[serde(default = "default_empty_json_array")]`
        // returns `"[]"` so legacy events replay with a valid JSON
        // array. This test locks that in.
        let legacy_json = r#"{
            "project": {
                "tenant_id": "t",
                "workspace_id": "w",
                "project_id": "p"
            },
            "trace_id": "trace_legacy",
            "session_id": "sess_legacy",
            "run_id": null,
            "model_id": "claude-sonnet-4-5",
            "system_prompt": "",
            "messages_json": "[]",
            "response_text": "",
            "tool_calls_json": "[]",
            "recorded_at_ms": 0
        }"#;

        let parsed: super::LlmCompletionRecorded =
            serde_json::from_str(legacy_json).expect("legacy event must deserialize");
        assert_eq!(
            parsed.tool_defs_json, "[]",
            "missing tool_defs_json must default to the valid-JSON-array string `[]`, \
             not the invalid `\"\"`; got {:?}",
            parsed.tool_defs_json,
        );
        // Paranoia: confirm it parses as an empty JSON array.
        let parsed_array: serde_json::Value = serde_json::from_str(&parsed.tool_defs_json)
            .expect("tool_defs_json default must parse as JSON");
        assert!(
            parsed_array.as_array().is_some_and(|a| a.is_empty()),
            "default tool_defs_json must parse as an empty JSON array; got {parsed_array:?}",
        );
    }
}
