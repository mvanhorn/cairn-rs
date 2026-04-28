//! Compile-time projection-status registry for every `RuntimeEvent` variant.
//!
//! Each variant declares one of:
//!
//! - [`ProjectionStatus::Projected`] — backed by a read-model table updated
//!   synchronously inside the event-log insert transaction. Byte-equal
//!   reads across `InMemoryStore` ↔ `SqliteStore` are enforced for a
//!   representative subset of variants by `tests/projection_parity.rs`;
//!   pg-backend parity runs the same harness under
//!   `TEST_DATABASE_URL` in nightly CI.
//! - [`ProjectionStatus::Stubbed`] — currently `log_stub` in the Postgres
//!   and/or SQLite applier: the event commits to the event log but no
//!   projection table is written. Silent-read risk. New additions are
//!   rejected by `.githooks/pre-commit` and the `projection-stub-guard`
//!   CI job.
//! - [`ProjectionStatus::Ephemeral`] — intentionally not persisted into a
//!   read-model table. The event log itself is the audit trail; operator
//!   observability comes from SSE/metrics. Examples: circuit-breaker
//!   trips, summarizer-fallback notifications, sandbox crash-recovery
//!   audits.
//!
//! ## Safety rails
//!
//! - `build.rs` parses `crates/cairn-domain/src/events.rs` and refuses
//!   to compile if any `RuntimeEvent` variant is missing from
//!   [`REGISTRY`] or any registry entry references a variant that no
//!   longer exists.
//! - [`assert_no_stubs_for_persistent_backend`] returns the list of
//!   Stubbed variants, intended to be called by the application at boot
//!   on pg/sqlite backends. It is a pure reporting function; callers
//!   decide whether to log or fail.
//! - `.githooks/pre-commit` and the `projection-stub-guard` CI job
//!   reject commits/PRs that introduce new `log_stub(` sites in the
//!   pg/sqlite appliers.
//!
//! See `docs/design/rfcs/RFC-025-runtime-aggregate-backend-abstraction.md`
//! for the migration roadmap and severity history.

use crate::db::Backend;

/// Projection status for a single `RuntimeEvent` variant.
///
/// See crate-level docs for the contract behind each status.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProjectionStatus {
    /// Event has a real projection in every supported persistent backend.
    /// The optional `table` is the primary read-model table written to —
    /// informational only; parity is asserted by the test harness, not by
    /// the registry entry.
    Projected { table: Option<&'static str> },
    /// Event currently maps to `log_stub(...)` in at least one persistent
    /// backend applier. `tracking` is a short human-readable hint
    /// (issue/RFC reference) for the Phase 2a/2b migration.
    Stubbed { tracking: &'static str },
    /// Event deliberately has no projection table. The `reason` explains
    /// why (e.g. "operator observability via SSE + metrics, no read model").
    Ephemeral { reason: &'static str },
}

impl ProjectionStatus {
    /// `true` when the variant currently has a `log_stub` applier on at
    /// least one persistent backend.
    pub const fn is_stubbed(&self) -> bool {
        matches!(self, ProjectionStatus::Stubbed { .. })
    }

    /// `true` when the variant is declared as having a real projection.
    pub const fn is_projected(&self) -> bool {
        matches!(self, ProjectionStatus::Projected { .. })
    }

    /// `true` when the variant is declared as having no projection by design.
    pub const fn is_ephemeral(&self) -> bool {
        matches!(self, ProjectionStatus::Ephemeral { .. })
    }
}

/// One registry row. `variant` must match a `RuntimeEvent` variant name
/// verbatim; `build.rs` enforces that.
#[derive(Clone, Copy, Debug)]
pub struct ProjectionEntry {
    pub variant: &'static str,
    pub status: ProjectionStatus,
}

/// Registry error set. Both variants carry the concrete variant list so the
/// operator can act on the boot log without grepping the source.
#[derive(Debug)]
pub enum RegistryError {
    /// `assert_no_stubs_for_persistent_backend` found at least one Stubbed
    /// entry. Contains the list of stubbed variant names.
    StubbedVariantsPresent {
        backend: Backend,
        stubbed: Vec<&'static str>,
    },
}

impl std::fmt::Display for RegistryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RegistryError::StubbedVariantsPresent { backend, stubbed } => {
                // Ephemeral variants are a first-class non-projected
                // status — the error only surfaces Stubbed ones, which
                // are the silent-no-op hazard Phase 2a/2b will migrate.
                write!(
                    f,
                    "projection registry: {} RuntimeEvent variant(s) are still stubbed for \
                     backend {:?} (every variant on a persistent backend must be either \
                     Projected or Ephemeral per RFC-025; Stubbed is not a supported steady \
                     state): {}",
                    stubbed.len(),
                    backend,
                    stubbed.join(", ")
                )
            }
        }
    }
}

impl std::error::Error for RegistryError {}

/// Central registry. **Every** `RuntimeEvent` variant MUST appear here; the
/// `cairn-store` `build.rs` rejects the crate otherwise.
///
/// Classification source (2026-04-28):
/// - Variants handled by a real `INSERT`/`UPDATE` arm in
///   `src/pg/projections.rs` → `Projected`.
/// - Variants in pg's intentional no-op arm (`=> {}`) → `Ephemeral`.
/// - Variants in pg's `log_stub(...)` arm → `Stubbed`.
///
/// The sqlite applier carries a slightly wider stub set than pg (mostly
/// Trigger/RunSla/Snapshot/TaskDependency surfaces); the registry uses the
/// pg classification as canonical since pg is the production backend.
/// Phase 2a/2b close the pg stub gap and, as a side effect, the sqlite
/// gap.
pub const REGISTRY: &[ProjectionEntry] = &[
    // ── Projected (48) ────────────────────────────────────────────────────
    ProjectionEntry {
        variant: "ApprovalRequested",
        status: ProjectionStatus::Projected {
            table: Some("approvals"),
        },
    },
    ProjectionEntry {
        variant: "ApprovalResolved",
        status: ProjectionStatus::Projected {
            table: Some("approvals"),
        },
    },
    ProjectionEntry {
        variant: "CheckpointPersisted",
        status: ProjectionStatus::Projected {
            table: Some("checkpoints"),
        },
    },
    ProjectionEntry {
        variant: "CheckpointRecorded",
        status: ProjectionStatus::Projected {
            table: Some("checkpoints"),
        },
    },
    ProjectionEntry {
        variant: "CheckpointRestored",
        status: ProjectionStatus::Projected {
            table: Some("checkpoints"),
        },
    },
    ProjectionEntry {
        variant: "DecisionCacheWarmup",
        status: ProjectionStatus::Projected {
            table: Some("decision_cache_warmups"),
        },
    },
    ProjectionEntry {
        variant: "DecisionRecorded",
        status: ProjectionStatus::Projected {
            table: Some("decision_records"),
        },
    },
    ProjectionEntry {
        variant: "MailboxMessageAppended",
        status: ProjectionStatus::Projected {
            table: Some("mailbox_messages"),
        },
    },
    ProjectionEntry {
        variant: "ProjectCreated",
        status: ProjectionStatus::Projected {
            table: Some("projects"),
        },
    },
    ProjectionEntry {
        variant: "PromptAssetCreated",
        status: ProjectionStatus::Projected {
            table: Some("prompt_assets"),
        },
    },
    ProjectionEntry {
        variant: "PromptReleaseCreated",
        status: ProjectionStatus::Projected {
            table: Some("prompt_releases"),
        },
    },
    ProjectionEntry {
        variant: "PromptReleaseTransitioned",
        status: ProjectionStatus::Projected {
            table: Some("prompt_releases"),
        },
    },
    ProjectionEntry {
        variant: "PromptVersionCreated",
        status: ProjectionStatus::Projected {
            table: Some("prompt_versions"),
        },
    },
    ProjectionEntry {
        variant: "ProviderCallCompleted",
        status: ProjectionStatus::Projected {
            table: Some("provider_calls"),
        },
    },
    ProjectionEntry {
        variant: "RecoveryAttempted",
        status: ProjectionStatus::Projected {
            table: Some("recovery_attempts"),
        },
    },
    ProjectionEntry {
        variant: "RecoveryCompleted",
        status: ProjectionStatus::Projected {
            table: Some("recovery_completions"),
        },
    },
    ProjectionEntry {
        variant: "RecoverySummaryEmitted",
        status: ProjectionStatus::Projected {
            table: Some("recovery_summaries"),
        },
    },
    ProjectionEntry {
        variant: "RouteDecisionMade",
        status: ProjectionStatus::Projected {
            table: Some("route_decisions"),
        },
    },
    ProjectionEntry {
        variant: "RoutePolicyCreated",
        status: ProjectionStatus::Projected {
            table: Some("route_policies"),
        },
    },
    ProjectionEntry {
        variant: "RunCompletionAnnotated",
        status: ProjectionStatus::Projected {
            table: Some("runs"),
        },
    },
    ProjectionEntry {
        variant: "RunCreated",
        status: ProjectionStatus::Projected {
            table: Some("runs"),
        },
    },
    ProjectionEntry {
        variant: "RunStateChanged",
        status: ProjectionStatus::Projected {
            table: Some("runs"),
        },
    },
    ProjectionEntry {
        variant: "SessionAttemptStarted",
        status: ProjectionStatus::Projected {
            table: Some("sessions"),
        },
    },
    ProjectionEntry {
        variant: "SessionCostUpdated",
        status: ProjectionStatus::Projected {
            table: Some("session_costs"),
        },
    },
    ProjectionEntry {
        variant: "SessionCreated",
        status: ProjectionStatus::Projected {
            table: Some("sessions"),
        },
    },
    ProjectionEntry {
        variant: "SessionOutcomeEmitted",
        status: ProjectionStatus::Projected {
            table: Some("session_outcomes"),
        },
    },
    ProjectionEntry {
        variant: "SessionStateChanged",
        status: ProjectionStatus::Projected {
            table: Some("sessions"),
        },
    },
    ProjectionEntry {
        variant: "TaskCreated",
        status: ProjectionStatus::Projected {
            table: Some("tasks"),
        },
    },
    ProjectionEntry {
        variant: "TaskLeaseClaimed",
        status: ProjectionStatus::Projected {
            table: Some("tasks"),
        },
    },
    ProjectionEntry {
        variant: "TaskLeaseHeartbeated",
        status: ProjectionStatus::Projected {
            table: Some("tasks"),
        },
    },
    ProjectionEntry {
        variant: "TaskStateChanged",
        status: ProjectionStatus::Projected {
            table: Some("tasks"),
        },
    },
    ProjectionEntry {
        variant: "TenantCreated",
        status: ProjectionStatus::Projected {
            table: Some("tenants"),
        },
    },
    ProjectionEntry {
        variant: "TerminalRecoveryAttempted",
        status: ProjectionStatus::Projected {
            table: Some("runs"),
        },
    },
    ProjectionEntry {
        variant: "ToolCallAmended",
        status: ProjectionStatus::Projected {
            table: Some("tool_call_approvals"),
        },
    },
    ProjectionEntry {
        variant: "ToolCallApproved",
        status: ProjectionStatus::Projected {
            table: Some("tool_call_approvals"),
        },
    },
    ProjectionEntry {
        variant: "ToolCallProposed",
        status: ProjectionStatus::Projected {
            table: Some("tool_call_approvals"),
        },
    },
    ProjectionEntry {
        variant: "ToolCallRejected",
        status: ProjectionStatus::Projected {
            table: Some("tool_call_approvals"),
        },
    },
    ProjectionEntry {
        variant: "ToolInvocationCacheHit",
        status: ProjectionStatus::Projected {
            table: Some("tool_invocation_cache_hits"),
        },
    },
    ProjectionEntry {
        variant: "ToolInvocationCompleted",
        status: ProjectionStatus::Projected {
            table: Some("tool_invocations"),
        },
    },
    ProjectionEntry {
        variant: "ToolInvocationFailed",
        status: ProjectionStatus::Projected {
            table: Some("tool_invocations"),
        },
    },
    ProjectionEntry {
        variant: "ToolInvocationProgressUpdated",
        status: ProjectionStatus::Projected {
            table: Some("tool_invocation_progress"),
        },
    },
    ProjectionEntry {
        variant: "ToolInvocationStarted",
        status: ProjectionStatus::Projected {
            table: Some("tool_invocations"),
        },
    },
    ProjectionEntry {
        variant: "WorkspaceArchived",
        status: ProjectionStatus::Projected {
            table: Some("workspaces"),
        },
    },
    ProjectionEntry {
        variant: "WorkspaceCreated",
        status: ProjectionStatus::Projected {
            table: Some("workspaces"),
        },
    },
    ProjectionEntry {
        variant: "WorkspaceMemberAdded",
        status: ProjectionStatus::Projected {
            table: Some("workspace_members"),
        },
    },
    ProjectionEntry {
        variant: "WorkspaceMemberRemoved",
        status: ProjectionStatus::Projected {
            table: Some("workspace_members"),
        },
    },
    ProjectionEntry {
        variant: "WorkspaceSnapshotCreated",
        status: ProjectionStatus::Projected {
            table: Some("workspace_snapshots"),
        },
    },
    ProjectionEntry {
        variant: "WorkspaceSnapshotReaped",
        status: ProjectionStatus::Projected {
            table: Some("workspace_snapshots"),
        },
    },
    // ── Ephemeral (31) ────────────────────────────────────────────────────
    // Operator observability surfaces (SSE + metrics) with no durable read
    // model. The event log itself is the audit trail.
    ProjectionEntry {
        variant: "ApprovalPolicyCreated",
        status: ProjectionStatus::Ephemeral {
            reason: "RFC 005 approval policies — no durable table yet; service-layer in-memory registry until table ships",
        },
    },
    ProjectionEntry {
        variant: "BudgetThresholdCrossed",
        status: ProjectionStatus::Ephemeral {
            reason: "F65 observability: SSE + metrics only; no read-model table",
        },
    },
    ProjectionEntry {
        variant: "CircuitBreakerTripped",
        status: ProjectionStatus::Ephemeral {
            reason: "F65 observability: SSE + metrics only; breaker trip is visible via session_outcomes.termination_reason",
        },
    },
    ProjectionEntry {
        variant: "OrchestratorDecisionMade",
        status: ProjectionStatus::Ephemeral {
            reason: "F65 observability: SSE + metrics only; no read-model table",
        },
    },
    ProjectionEntry {
        variant: "PromptRolloutStarted",
        status: ProjectionStatus::Ephemeral {
            reason: "RFC 001 gradual rollout — state tracked via the prompt_releases projection",
        },
    },
    ProjectionEntry {
        variant: "RunSlaBreached",
        status: ProjectionStatus::Ephemeral {
            reason: "SLA breach surfaces via notifications + SSE; no dedicated table",
        },
    },
    ProjectionEntry {
        variant: "RunSlaSet",
        status: ProjectionStatus::Ephemeral {
            reason: "SLA set — policy-layer state, no dedicated runtime table",
        },
    },
    ProjectionEntry {
        variant: "RunTemplateCreated",
        status: ProjectionStatus::Ephemeral {
            reason: "Run templates — service-layer in-memory registry; event log is audit trail",
        },
    },
    ProjectionEntry {
        variant: "RunTemplateDeleted",
        status: ProjectionStatus::Ephemeral {
            reason: "Run templates — service-layer in-memory registry; event log is audit trail",
        },
    },
    ProjectionEntry {
        variant: "SandboxCrashRecovered",
        status: ProjectionStatus::Ephemeral {
            reason: "F65 PR-5 #359: crash-recovery umount sweep is observability-only (SSE + metrics)",
        },
    },
    ProjectionEntry {
        variant: "SessionAttemptCompleted",
        status: ProjectionStatus::Ephemeral {
            reason: "Visible via event log + subsequent SessionOutcomeEmitted row; no dedicated table",
        },
    },
    ProjectionEntry {
        variant: "SignalRouted",
        status: ProjectionStatus::Ephemeral {
            reason: "Signal routing — projection lives in cairn-signal service layer, not cairn-store",
        },
    },
    ProjectionEntry {
        variant: "SignalSubscriptionCreated",
        status: ProjectionStatus::Ephemeral {
            reason: "Signal subscription — projection lives in cairn-signal service layer",
        },
    },
    ProjectionEntry {
        variant: "SnapshotCreated",
        status: ProjectionStatus::Ephemeral {
            reason: "Workspace snapshot event predates F65 WorkspaceSnapshotCreated; retained for back-compat",
        },
    },
    ProjectionEntry {
        variant: "SummarizerFallback",
        status: ProjectionStatus::Ephemeral {
            reason: "F65 observability: SSE + metrics only; no read-model table",
        },
    },
    ProjectionEntry {
        variant: "TaskDependencyAdded",
        status: ProjectionStatus::Ephemeral {
            reason: "Task dependency edges — graph projection owns the read model, not cairn-store",
        },
    },
    ProjectionEntry {
        variant: "TaskDependencyResolved",
        status: ProjectionStatus::Ephemeral {
            reason: "Task dependency edges — graph projection owns the read model, not cairn-store",
        },
    },
    ProjectionEntry {
        variant: "TaskLeaseExpired",
        status: ProjectionStatus::Ephemeral {
            reason: "Lease expiry — owned by FlowFabric lease-history; cairn mirrors via TaskStateChanged",
        },
    },
    ProjectionEntry {
        variant: "TaskPriorityChanged",
        status: ProjectionStatus::Ephemeral {
            reason: "Priority changes — scheduler-layer concern; audit via event log",
        },
    },
    ProjectionEntry {
        variant: "TriggerCreated",
        status: ProjectionStatus::Ephemeral {
            reason: "Triggers — projection lives in cairn-signal TriggerService, rebuilt from log via replay_triggers (RFC-025 Phase 1.5a)",
        },
    },
    ProjectionEntry {
        variant: "TriggerDeleted",
        status: ProjectionStatus::Ephemeral {
            reason: "Triggers — projection lives in cairn-signal TriggerService",
        },
    },
    ProjectionEntry {
        variant: "TriggerDenied",
        status: ProjectionStatus::Ephemeral {
            reason: "Trigger-fire audit — observability only",
        },
    },
    ProjectionEntry {
        variant: "TriggerDisabled",
        status: ProjectionStatus::Ephemeral {
            reason: "Triggers — projection lives in cairn-signal TriggerService",
        },
    },
    ProjectionEntry {
        variant: "TriggerEnabled",
        status: ProjectionStatus::Ephemeral {
            reason: "Triggers — projection lives in cairn-signal TriggerService",
        },
    },
    ProjectionEntry {
        variant: "TriggerFired",
        status: ProjectionStatus::Ephemeral {
            reason: "Trigger-fire audit — observability only",
        },
    },
    ProjectionEntry {
        variant: "TriggerPendingApproval",
        status: ProjectionStatus::Ephemeral {
            reason: "Trigger-fire audit — observability only",
        },
    },
    ProjectionEntry {
        variant: "TriggerRateLimited",
        status: ProjectionStatus::Ephemeral {
            reason: "Trigger-fire audit — observability only",
        },
    },
    ProjectionEntry {
        variant: "TriggerResumed",
        status: ProjectionStatus::Ephemeral {
            reason: "Triggers — projection lives in cairn-signal TriggerService",
        },
    },
    ProjectionEntry {
        variant: "TriggerSkipped",
        status: ProjectionStatus::Ephemeral {
            reason: "Trigger-fire audit — observability only",
        },
    },
    ProjectionEntry {
        variant: "TriggerSuspended",
        status: ProjectionStatus::Ephemeral {
            reason: "Triggers — projection lives in cairn-signal TriggerService",
        },
    },
    ProjectionEntry {
        variant: "WorkspaceBackendDegraded",
        status: ProjectionStatus::Ephemeral {
            reason: "F65 observability: SSE + metrics only; no read-model table",
        },
    },
    // ── Stubbed (77) — Phase 2a / Phase 2b work ───────────────────────────
    // Each entry is a silent-read risk on pg/sqlite today. See RFC-025
    // for the migration order; the pre-commit hook + CI grep step reject
    // new additions.
    ProjectionEntry {
        variant: "ApprovalDelegated",
        status: ProjectionStatus::Stubbed {
            tracking: "RFC-025 Phase 2a (approvals)",
        },
    },
    ProjectionEntry {
        variant: "AuditLogEntryRecorded",
        status: ProjectionStatus::Stubbed {
            tracking: "RFC-025 Phase 2b (audits — new table required)",
        },
    },
    ProjectionEntry {
        variant: "ChannelCreated",
        status: ProjectionStatus::Stubbed {
            tracking: "RFC-025 Phase 2a (channels)",
        },
    },
    ProjectionEntry {
        variant: "ChannelMessageConsumed",
        status: ProjectionStatus::Stubbed {
            tracking: "RFC-025 Phase 2a (channels)",
        },
    },
    ProjectionEntry {
        variant: "ChannelMessageSent",
        status: ProjectionStatus::Stubbed {
            tracking: "RFC-025 Phase 2a (channels)",
        },
    },
    ProjectionEntry {
        variant: "CheckpointStrategySet",
        status: ProjectionStatus::Stubbed {
            tracking: "RFC-025 Phase 2a (checkpoint strategies)",
        },
    },
    ProjectionEntry {
        variant: "CredentialKeyRotated",
        status: ProjectionStatus::Stubbed {
            tracking: "RFC-025 Phase 2a (credentials)",
        },
    },
    ProjectionEntry {
        variant: "CredentialRevoked",
        status: ProjectionStatus::Stubbed {
            tracking: "RFC-025 Phase 2a (credentials)",
        },
    },
    ProjectionEntry {
        variant: "CredentialStored",
        status: ProjectionStatus::Stubbed {
            tracking: "RFC-025 Phase 2a (credentials)",
        },
    },
    ProjectionEntry {
        variant: "DefaultSettingCleared",
        status: ProjectionStatus::Stubbed {
            tracking: "RFC-025 Phase 2a (defaults)",
        },
    },
    ProjectionEntry {
        variant: "DefaultSettingSet",
        status: ProjectionStatus::Stubbed {
            tracking: "RFC-025 Phase 2a (defaults)",
        },
    },
    ProjectionEntry {
        variant: "EntitlementOverrideSet",
        status: ProjectionStatus::Stubbed {
            tracking: "RFC-025 Phase 2a (entitlements)",
        },
    },
    ProjectionEntry {
        variant: "EvalBaselineLocked",
        status: ProjectionStatus::Stubbed {
            tracking: "RFC-025 Phase 2a (eval baselines)",
        },
    },
    ProjectionEntry {
        variant: "EvalBaselineSet",
        status: ProjectionStatus::Stubbed {
            tracking: "RFC-025 Phase 2a (eval baselines)",
        },
    },
    ProjectionEntry {
        variant: "EvalDatasetCreated",
        status: ProjectionStatus::Stubbed {
            tracking: "RFC-025 Phase 2a (eval datasets)",
        },
    },
    ProjectionEntry {
        variant: "EvalDatasetEntryAdded",
        status: ProjectionStatus::Stubbed {
            tracking: "RFC-025 Phase 2a (eval datasets)",
        },
    },
    ProjectionEntry {
        variant: "EvalRubricCreated",
        status: ProjectionStatus::Stubbed {
            tracking: "RFC-025 Phase 2a (eval rubrics)",
        },
    },
    ProjectionEntry {
        variant: "EvalRunArchived",
        status: ProjectionStatus::Stubbed {
            tracking: "RFC-025 Phase 1 (evals)",
        },
    },
    ProjectionEntry {
        variant: "EvalRunCompleted",
        status: ProjectionStatus::Stubbed {
            tracking: "RFC-025 Phase 1 (evals)",
        },
    },
    ProjectionEntry {
        variant: "EvalRunStarted",
        status: ProjectionStatus::Stubbed {
            tracking: "RFC-025 Phase 1 (evals)",
        },
    },
    ProjectionEntry {
        variant: "EventLogCompacted",
        status: ProjectionStatus::Stubbed {
            tracking: "RFC-025 Phase 2b (event-log compaction audit)",
        },
    },
    ProjectionEntry {
        variant: "ExternalWorkerReactivated",
        status: ProjectionStatus::Stubbed {
            tracking: "RFC-025 Phase 2b (external workers — FF-owned live state, cairn-side projection)",
        },
    },
    ProjectionEntry {
        variant: "ExternalWorkerRegistered",
        status: ProjectionStatus::Stubbed {
            tracking: "RFC-025 Phase 2b (external workers — FF-owned live state, cairn-side projection)",
        },
    },
    ProjectionEntry {
        variant: "ExternalWorkerReported",
        status: ProjectionStatus::Stubbed {
            tracking: "RFC-025 Phase 2b (external workers — FF-owned live state, cairn-side projection)",
        },
    },
    ProjectionEntry {
        variant: "ExternalWorkerSuspended",
        status: ProjectionStatus::Stubbed {
            tracking: "RFC-025 Phase 2b (external workers — FF-owned live state, cairn-side projection)",
        },
    },
    ProjectionEntry {
        variant: "GuardrailPolicyCreated",
        status: ProjectionStatus::Stubbed {
            tracking: "RFC-025 Phase 2a (guardrails)",
        },
    },
    ProjectionEntry {
        variant: "GuardrailPolicyEvaluated",
        status: ProjectionStatus::Stubbed {
            tracking: "RFC-025 Phase 2a (guardrails)",
        },
    },
    ProjectionEntry {
        variant: "IngestJobCompleted",
        status: ProjectionStatus::Stubbed {
            tracking: "RFC-025 Phase 2a (ingest jobs)",
        },
    },
    ProjectionEntry {
        variant: "IngestJobStarted",
        status: ProjectionStatus::Stubbed {
            tracking: "RFC-025 Phase 2a (ingest jobs)",
        },
    },
    ProjectionEntry {
        variant: "LicenseActivated",
        status: ProjectionStatus::Stubbed {
            tracking: "RFC-025 Phase 2a (licenses)",
        },
    },
    ProjectionEntry {
        variant: "NotificationPreferenceSet",
        status: ProjectionStatus::Stubbed {
            tracking: "RFC-025 Phase 2a (notifications)",
        },
    },
    ProjectionEntry {
        variant: "NotificationSent",
        status: ProjectionStatus::Stubbed {
            tracking: "RFC-025 Phase 2a (notifications)",
        },
    },
    ProjectionEntry {
        variant: "OperatorIntervention",
        status: ProjectionStatus::Stubbed {
            tracking: "RFC-025 Phase 2a (operator interventions)",
        },
    },
    ProjectionEntry {
        variant: "OperatorProfileCreated",
        status: ProjectionStatus::Stubbed {
            tracking: "RFC-025 Phase 2a (operator profiles)",
        },
    },
    ProjectionEntry {
        variant: "OperatorProfileUpdated",
        status: ProjectionStatus::Stubbed {
            tracking: "RFC-025 Phase 2a (operator profiles)",
        },
    },
    ProjectionEntry {
        variant: "OutcomeRecorded",
        status: ProjectionStatus::Stubbed {
            tracking: "RFC-025 Phase 2b (outcomes)",
        },
    },
    ProjectionEntry {
        variant: "PauseScheduled",
        status: ProjectionStatus::Stubbed {
            tracking: "RFC-025 Phase 2a (pause schedules)",
        },
    },
    ProjectionEntry {
        variant: "PermissionDecisionRecorded",
        status: ProjectionStatus::Stubbed {
            tracking: "RFC-025 Phase 2a (permission decisions)",
        },
    },
    ProjectionEntry {
        variant: "PlanApproved",
        status: ProjectionStatus::Stubbed {
            tracking: "RFC-025 Phase 2b (plan review events — RFC 018)",
        },
    },
    ProjectionEntry {
        variant: "PlanProposed",
        status: ProjectionStatus::Stubbed {
            tracking: "RFC-025 Phase 2b (plan review events — RFC 018)",
        },
    },
    ProjectionEntry {
        variant: "PlanRejected",
        status: ProjectionStatus::Stubbed {
            tracking: "RFC-025 Phase 2b (plan review events — RFC 018)",
        },
    },
    ProjectionEntry {
        variant: "PlanRevisionRequested",
        status: ProjectionStatus::Stubbed {
            tracking: "RFC-025 Phase 2b (plan review events — RFC 018)",
        },
    },
    ProjectionEntry {
        variant: "ProviderBindingCreated",
        status: ProjectionStatus::Stubbed {
            tracking: "RFC-025 Phase 3 (provider bindings vs connections split)",
        },
    },
    ProjectionEntry {
        variant: "ProviderBindingStateChanged",
        status: ProjectionStatus::Stubbed {
            tracking: "RFC-025 Phase 3 (provider bindings vs connections split)",
        },
    },
    ProjectionEntry {
        variant: "ProviderBudgetAlertTriggered",
        status: ProjectionStatus::Stubbed {
            tracking: "RFC-025 Phase 2a (provider budgets)",
        },
    },
    ProjectionEntry {
        variant: "ProviderBudgetExceeded",
        status: ProjectionStatus::Stubbed {
            tracking: "RFC-025 Phase 2a (provider budgets)",
        },
    },
    ProjectionEntry {
        variant: "ProviderBudgetSet",
        status: ProjectionStatus::Stubbed {
            tracking: "RFC-025 Phase 2a (provider budgets)",
        },
    },
    ProjectionEntry {
        variant: "ProviderConnectionDeleted",
        status: ProjectionStatus::Stubbed {
            tracking: "RFC-025 Phase 3 (provider connections — ephemeral vs projected pending research)",
        },
    },
    ProjectionEntry {
        variant: "ProviderConnectionRegistered",
        status: ProjectionStatus::Stubbed {
            tracking: "RFC-025 Phase 3 (provider connections — ephemeral vs projected pending research)",
        },
    },
    ProjectionEntry {
        variant: "ProviderHealthChecked",
        status: ProjectionStatus::Stubbed {
            tracking: "RFC-025 Phase 3 (provider health — ephemeral vs projected pending research)",
        },
    },
    ProjectionEntry {
        variant: "ProviderHealthScheduleSet",
        status: ProjectionStatus::Stubbed {
            tracking: "RFC-025 Phase 3 (provider health)",
        },
    },
    ProjectionEntry {
        variant: "ProviderHealthScheduleTriggered",
        status: ProjectionStatus::Stubbed {
            tracking: "RFC-025 Phase 3 (provider health)",
        },
    },
    ProjectionEntry {
        variant: "ProviderMarkedDegraded",
        status: ProjectionStatus::Stubbed {
            tracking: "RFC-025 Phase 3 (provider health)",
        },
    },
    ProjectionEntry {
        variant: "ProviderModelRegistered",
        status: ProjectionStatus::Stubbed {
            tracking: "RFC-025 Phase 3 (provider models)",
        },
    },
    ProjectionEntry {
        variant: "ProviderPoolConnectionAdded",
        status: ProjectionStatus::Stubbed {
            tracking: "RFC-025 Phase 3 (provider pools — ephemeral vs projected pending research)",
        },
    },
    ProjectionEntry {
        variant: "ProviderPoolConnectionRemoved",
        status: ProjectionStatus::Stubbed {
            tracking: "RFC-025 Phase 3 (provider pools — ephemeral vs projected pending research)",
        },
    },
    ProjectionEntry {
        variant: "ProviderPoolCreated",
        status: ProjectionStatus::Stubbed {
            tracking: "RFC-025 Phase 3 (provider pools — ephemeral vs projected pending research)",
        },
    },
    ProjectionEntry {
        variant: "ProviderRecovered",
        status: ProjectionStatus::Stubbed {
            tracking: "RFC-025 Phase 3 (provider health)",
        },
    },
    ProjectionEntry {
        variant: "ProviderRetryPolicySet",
        status: ProjectionStatus::Stubbed {
            tracking: "RFC-025 Phase 3 (provider retry policies)",
        },
    },
    ProjectionEntry {
        variant: "RecoveryEscalated",
        status: ProjectionStatus::Stubbed {
            tracking: "RFC-025 Phase 2b (recovery escalation audit)",
        },
    },
    ProjectionEntry {
        variant: "ResourceShareRevoked",
        status: ProjectionStatus::Stubbed {
            tracking: "RFC-025 Phase 2b (resource sharing)",
        },
    },
    ProjectionEntry {
        variant: "ResourceShared",
        status: ProjectionStatus::Stubbed {
            tracking: "RFC-025 Phase 2b (resource sharing)",
        },
    },
    ProjectionEntry {
        variant: "RetentionPolicySet",
        status: ProjectionStatus::Stubbed {
            tracking: "RFC-025 Phase 2a (retention)",
        },
    },
    ProjectionEntry {
        variant: "RoutePolicyUpdated",
        status: ProjectionStatus::Stubbed {
            tracking: "RFC-025 Phase 2a (route policies — Created is projected, Updated is not)",
        },
    },
    ProjectionEntry {
        variant: "RunCostAlertSet",
        status: ProjectionStatus::Stubbed {
            tracking: "RFC-025 Phase 2a (run cost alerts)",
        },
    },
    ProjectionEntry {
        variant: "RunCostAlertTriggered",
        status: ProjectionStatus::Stubbed {
            tracking: "RFC-025 Phase 2a (run cost alerts)",
        },
    },
    ProjectionEntry {
        variant: "RunCostUpdated",
        status: ProjectionStatus::Stubbed {
            tracking: "RFC-025 Phase 2a (run cost updates — parity with SessionCostUpdated)",
        },
    },
    ProjectionEntry {
        variant: "ScheduledTaskCreated",
        status: ProjectionStatus::Stubbed {
            tracking: "RFC-025 Phase 2b (scheduled tasks)",
        },
    },
    ProjectionEntry {
        variant: "SignalIngested",
        status: ProjectionStatus::Stubbed {
            tracking: "RFC-025 Phase 2b (signal ingest)",
        },
    },
    ProjectionEntry {
        variant: "SoulPatchApplied",
        status: ProjectionStatus::Stubbed {
            tracking: "RFC-025 Phase 2b (soul patches)",
        },
    },
    ProjectionEntry {
        variant: "SoulPatchProposed",
        status: ProjectionStatus::Stubbed {
            tracking: "RFC-025 Phase 2b (soul patches)",
        },
    },
    ProjectionEntry {
        variant: "SpendAlertTriggered",
        status: ProjectionStatus::Stubbed {
            tracking: "RFC-025 Phase 2a (spend alerts)",
        },
    },
    ProjectionEntry {
        variant: "SubagentSpawned",
        status: ProjectionStatus::Stubbed {
            tracking: "RFC-025 Phase 2b (subagents — RFC 014 parent/child run graph)",
        },
    },
    ProjectionEntry {
        variant: "TenantQuotaSet",
        status: ProjectionStatus::Stubbed {
            tracking: "RFC-025 Phase 2a (quotas)",
        },
    },
    ProjectionEntry {
        variant: "TenantQuotaViolated",
        status: ProjectionStatus::Stubbed {
            tracking: "RFC-025 Phase 2a (quotas)",
        },
    },
    ProjectionEntry {
        variant: "ToolRecoveryPaused",
        status: ProjectionStatus::Stubbed {
            tracking: "RFC-025 Phase 2b (tool recovery audit)",
        },
    },
    ProjectionEntry {
        variant: "UserMessageAppended",
        status: ProjectionStatus::Stubbed {
            tracking: "RFC-025 Phase 2b (user messages)",
        },
    },
];

/// Return the registry entry for `variant`, or `None` if unregistered.
///
/// Linear scan. The registry has 156 entries at Phase 0 and is called
/// at most once per boot (`assert_no_stubs_for_persistent_backend`),
/// plus a handful of test-time calls in `projection_parity.rs`. On a
/// modern CPU the scan costs tens of nanoseconds — a `phf::Map` or a
/// `once_cell::sync::Lazy<HashMap>` would add a crate dependency or
/// a one-time allocation for no observable win.
///
/// Revisit the data structure only if a hot path starts calling
/// `lookup` per-event (none does today; event dispatch goes through
/// the pg/sqlite/InMemory appliers directly, not through the registry).
///
/// Tests that iterate can walk [`REGISTRY`] directly; no need to go
/// through `lookup`.
pub fn lookup(variant: &str) -> Option<ProjectionStatus> {
    REGISTRY
        .iter()
        .find(|entry| entry.variant == variant)
        .map(|entry| entry.status)
}

/// Fail if any registered variant is still `Stubbed` when running against a
/// persistent backend (Postgres or SQLite). In-memory boots call this with
/// a no-op path because the in-memory store materializes projections
/// through a different code path that cannot leak empty reads (RFC-025
/// §"Silent-read protection").
///
/// Phase 0 is infrastructure-only: the caller (cairn-app) currently logs
/// the returned error at `WARN` and proceeds. Phase 2c flips that to a
/// hard boot failure once Phase 2a + Phase 2b land and the stub list is
/// empty.
pub fn assert_no_stubs_for_persistent_backend(backend: Backend) -> Result<(), RegistryError> {
    let stubbed: Vec<&'static str> = REGISTRY
        .iter()
        .filter(|e| e.status.is_stubbed())
        .map(|e| e.variant)
        .collect();
    if stubbed.is_empty() {
        Ok(())
    } else {
        Err(RegistryError::StubbedVariantsPresent { backend, stubbed })
    }
}

/// In-memory equivalent of [`assert_no_stubs_for_persistent_backend`]:
/// unconditional `Ok(())`. Exists so the `AppState::new` boot path has a
/// single uniform call irrespective of the selected backend.
pub fn assert_no_stubs_for_in_memory() -> Result<(), RegistryError> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_has_no_duplicate_variants() {
        let mut seen: std::collections::HashSet<&'static str> = std::collections::HashSet::new();
        for entry in REGISTRY {
            assert!(
                seen.insert(entry.variant),
                "duplicate registry entry for variant {}",
                entry.variant
            );
        }
    }

    #[test]
    fn lookup_returns_registered_status() {
        let status = lookup("SessionCreated").expect("SessionCreated is Projected");
        assert!(status.is_projected());
        let status = lookup("EvalRunStarted").expect("EvalRunStarted is Stubbed");
        assert!(status.is_stubbed());
        let status = lookup("CircuitBreakerTripped").expect("CircuitBreakerTripped is Ephemeral");
        assert!(status.is_ephemeral());
        assert!(lookup("NotAVariant").is_none());
    }

    #[test]
    fn assert_no_stubs_in_memory_always_ok() {
        assert!(assert_no_stubs_for_in_memory().is_ok());
    }

    #[test]
    fn assert_no_stubs_for_postgres_lists_every_stub() {
        // Phase 0: this MUST fail (we haven't migrated anything yet), and
        // the error payload MUST name every Stubbed registry entry so the
        // operator sees a concrete action list in the boot log.
        let err = assert_no_stubs_for_persistent_backend(Backend::Postgres)
            .expect_err("Phase 0: registry still carries Stubbed variants");
        let RegistryError::StubbedVariantsPresent { backend, stubbed } = err;
        assert_eq!(backend, Backend::Postgres);
        assert!(
            !stubbed.is_empty(),
            "Phase 0 should surface at least one stubbed variant"
        );
        // Spot-check a handful of variants that must be in the list
        // until Phase 1/2a/2b land.
        for required in [
            "EvalRunStarted",
            "CredentialStored",
            "AuditLogEntryRecorded",
        ] {
            assert!(
                stubbed.contains(&required),
                "{required} should be listed as stubbed in Phase 0"
            );
        }
    }

    #[test]
    fn assert_no_stubs_for_sqlite_lists_every_stub() {
        let err = assert_no_stubs_for_persistent_backend(Backend::Sqlite)
            .expect_err("Phase 0: registry still carries Stubbed variants");
        let RegistryError::StubbedVariantsPresent { backend, .. } = err;
        assert_eq!(backend, Backend::Sqlite);
    }

    #[test]
    fn registry_counts_match_rfc025_phase_0_classification() {
        let projected = REGISTRY.iter().filter(|e| e.status.is_projected()).count();
        let ephemeral = REGISTRY.iter().filter(|e| e.status.is_ephemeral()).count();
        let stubbed = REGISTRY.iter().filter(|e| e.status.is_stubbed()).count();
        // RFC-025 Phase 0 audit (2026-04-28). If these numbers change,
        // update the registry AND the RFC/memory note — the audit is the
        // baseline against which Phase 2a/2b progress is measured.
        assert_eq!(
            projected, 48,
            "Projected count drifted; update registry + RFC"
        );
        assert_eq!(
            ephemeral, 31,
            "Ephemeral count drifted; update registry + RFC"
        );
        assert_eq!(stubbed, 77, "Stubbed count drifted; update registry + RFC");
        assert_eq!(projected + ephemeral + stubbed, 156);
    }

    #[test]
    fn error_display_includes_variant_list() {
        let err = assert_no_stubs_for_persistent_backend(Backend::Postgres).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("EvalRunStarted"));
        assert!(msg.contains("Postgres") || msg.contains("postgres"));
    }
}
