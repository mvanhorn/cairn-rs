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
    // RFC-025 Phase 1 (milestone 7): five eval lifecycle variants
    // flipped from Stubbed / Ephemeral → Projected. pg V034 migration +
    // sqlite schema.rs carry the `eval_runs` read-model table; in-memory
    // store mirrors the projection. The two new variants (Scored /
    // RubricScored) landed as Ephemeral staging in milestone 1 and
    // become Projected here now that all three backends wire them.
    ProjectionEntry {
        variant: "EvalRubricScored",
        status: ProjectionStatus::Projected {
            table: Some("eval_runs"),
        },
    },
    ProjectionEntry {
        variant: "EvalRunArchived",
        status: ProjectionStatus::Projected {
            table: Some("eval_runs"),
        },
    },
    ProjectionEntry {
        variant: "EvalRunCompleted",
        status: ProjectionStatus::Projected {
            table: Some("eval_runs"),
        },
    },
    ProjectionEntry {
        variant: "EvalRunScored",
        status: ProjectionStatus::Projected {
            table: Some("eval_runs"),
        },
    },
    ProjectionEntry {
        variant: "EvalRunStarted",
        status: ProjectionStatus::Projected {
            table: Some("eval_runs"),
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
    // RFC-025 Phase 1.5a: RunTemplateCreated / RunTemplateDeleted flipped
    // Ephemeral → Projected. Templates are durable state (the trigger
    // service dereferences `run_template_id` on every fire) so the pg /
    // sqlite / in_memory projections all write a row to `run_templates`.
    ProjectionEntry {
        variant: "RunTemplateCreated",
        status: ProjectionStatus::Projected {
            table: Some("run_templates"),
        },
    },
    ProjectionEntry {
        variant: "RunTemplateDeleted",
        status: ProjectionStatus::Projected {
            table: Some("run_templates"),
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
    // RFC-025 Phase 1.5a: 8 state-carrying variants flipped
    // Ephemeral → Projected. Every lifecycle edge (Created, Enabled,
    // Disabled, Suspended, Resumed, Deleted, RunTemplateCreated,
    // RunTemplateDeleted) writes into the `triggers` / `run_templates`
    // tables inside the event-append transaction. Restart reads go
    // straight to the projection; `AppState::replay_triggers` is gone.
    ProjectionEntry {
        variant: "TriggerCreated",
        status: ProjectionStatus::Projected {
            table: Some("triggers"),
        },
    },
    ProjectionEntry {
        variant: "TriggerDeleted",
        status: ProjectionStatus::Projected {
            table: Some("triggers"),
        },
    },
    // RFC-025 Phase 1.5a: five audit variants write append-only rows
    // into `trigger_fires` (read by the duplicate-fire ledger + rate-
    // limit window + project-budget counter), so they now classify as
    // Projected even though the runtime doesn't rebuild entity state
    // from them at boot. The registry's Projected contract is "backed
    // by a read-model table updated synchronously" — these five meet
    // that contract via `trigger_fires`. (PR #569 review.)
    ProjectionEntry {
        variant: "TriggerDenied",
        status: ProjectionStatus::Projected {
            table: Some("trigger_fires"),
        },
    },
    ProjectionEntry {
        variant: "TriggerDisabled",
        status: ProjectionStatus::Projected {
            table: Some("triggers"),
        },
    },
    ProjectionEntry {
        variant: "TriggerEnabled",
        status: ProjectionStatus::Projected {
            table: Some("triggers"),
        },
    },
    ProjectionEntry {
        variant: "TriggerFired",
        status: ProjectionStatus::Projected {
            table: Some("trigger_fires"),
        },
    },
    ProjectionEntry {
        variant: "TriggerPendingApproval",
        status: ProjectionStatus::Projected {
            table: Some("trigger_fires"),
        },
    },
    ProjectionEntry {
        variant: "TriggerRateLimited",
        status: ProjectionStatus::Projected {
            table: Some("trigger_fires"),
        },
    },
    ProjectionEntry {
        variant: "TriggerResumed",
        status: ProjectionStatus::Projected {
            table: Some("triggers"),
        },
    },
    ProjectionEntry {
        variant: "TriggerSkipped",
        status: ProjectionStatus::Projected {
            table: Some("trigger_fires"),
        },
    },
    ProjectionEntry {
        variant: "TriggerSuspended",
        status: ProjectionStatus::Projected {
            table: Some("triggers"),
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
        status: ProjectionStatus::Projected {
            table: Some("approval_delegations"),
        },
    },
    ProjectionEntry {
        variant: "AuditLogEntryRecorded",
        status: ProjectionStatus::Projected {
            table: Some("audit_log_entries"),
        },
    },
    ProjectionEntry {
        variant: "ChannelCreated",
        status: ProjectionStatus::Projected {
            table: Some("channels"),
        },
    },
    ProjectionEntry {
        variant: "ChannelMessageConsumed",
        status: ProjectionStatus::Projected {
            table: Some("channel_messages"),
        },
    },
    ProjectionEntry {
        variant: "ChannelMessageSent",
        status: ProjectionStatus::Projected {
            table: Some("channel_messages"),
        },
    },
    ProjectionEntry {
        variant: "CheckpointStrategySet",
        status: ProjectionStatus::Projected {
            table: Some("checkpoint_strategies"),
        },
    },
    ProjectionEntry {
        variant: "CredentialKeyRotated",
        status: ProjectionStatus::Projected {
            table: Some("credential_rotations"),
        },
    },
    ProjectionEntry {
        variant: "CredentialRevoked",
        status: ProjectionStatus::Projected {
            table: Some("credentials"),
        },
    },
    ProjectionEntry {
        variant: "CredentialStored",
        status: ProjectionStatus::Projected {
            table: Some("credentials"),
        },
    },
    ProjectionEntry {
        variant: "DefaultSettingCleared",
        status: ProjectionStatus::Projected {
            table: Some("default_settings"),
        },
    },
    ProjectionEntry {
        variant: "DefaultSettingSet",
        status: ProjectionStatus::Projected {
            table: Some("default_settings"),
        },
    },
    ProjectionEntry {
        variant: "EntitlementOverrideSet",
        status: ProjectionStatus::Projected {
            table: Some("entitlement_overrides"),
        },
    },
    ProjectionEntry {
        variant: "EvalBaselineLocked",
        status: ProjectionStatus::Projected {
            table: Some("eval_baselines"),
        },
    },
    ProjectionEntry {
        variant: "EvalBaselineSet",
        status: ProjectionStatus::Projected {
            table: Some("eval_baselines"),
        },
    },
    ProjectionEntry {
        variant: "EvalDatasetCreated",
        status: ProjectionStatus::Projected {
            table: Some("eval_datasets"),
        },
    },
    ProjectionEntry {
        variant: "EvalDatasetEntryAdded",
        status: ProjectionStatus::Projected {
            table: Some("eval_dataset_entries"),
        },
    },
    ProjectionEntry {
        variant: "EvalRubricCreated",
        status: ProjectionStatus::Projected {
            table: Some("eval_rubrics"),
        },
    },
    ProjectionEntry {
        variant: "EventLogCompacted",
        status: ProjectionStatus::Ephemeral {
            reason: "Event-log compaction is a single-shot maintenance operation — the compaction boundary is visible in `event_log` via the first remaining position; no dedicated read-model row is needed and no operator UI queries by compaction timestamp.",
        },
    },
    ProjectionEntry {
        variant: "ExternalWorkerReactivated",
        status: ProjectionStatus::Projected {
            table: Some("external_workers"),
        },
    },
    ProjectionEntry {
        variant: "ExternalWorkerRegistered",
        status: ProjectionStatus::Projected {
            table: Some("external_workers"),
        },
    },
    ProjectionEntry {
        variant: "ExternalWorkerReported",
        status: ProjectionStatus::Projected {
            table: Some("external_workers"),
        },
    },
    ProjectionEntry {
        variant: "ExternalWorkerSuspended",
        status: ProjectionStatus::Projected {
            table: Some("external_workers"),
        },
    },
    ProjectionEntry {
        variant: "GuardrailPolicyCreated",
        status: ProjectionStatus::Projected {
            table: Some("guardrail_policies"),
        },
    },
    ProjectionEntry {
        variant: "GuardrailPolicyEvaluated",
        status: ProjectionStatus::Projected {
            table: Some("guardrail_evaluations"),
        },
    },
    ProjectionEntry {
        variant: "IngestJobCompleted",
        status: ProjectionStatus::Projected {
            table: Some("ingest_jobs"),
        },
    },
    ProjectionEntry {
        variant: "IngestJobStarted",
        status: ProjectionStatus::Projected {
            table: Some("ingest_jobs"),
        },
    },
    ProjectionEntry {
        variant: "LicenseActivated",
        status: ProjectionStatus::Projected {
            table: Some("licenses"),
        },
    },
    ProjectionEntry {
        variant: "NotificationPreferenceSet",
        status: ProjectionStatus::Projected {
            table: Some("notification_preferences"),
        },
    },
    ProjectionEntry {
        variant: "NotificationSent",
        status: ProjectionStatus::Projected {
            table: Some("notifications"),
        },
    },
    ProjectionEntry {
        variant: "OperatorIntervention",
        status: ProjectionStatus::Ephemeral {
            reason: "`OperatorInterventionReadModel::list_by_run` walks the event log directly on every backend (pg/sqlite reads replay into `InMemoryStore` at boot; the read impl iterates `state.events` and filters by `run_id`). A dedicated projection table would duplicate the event-log contents without a new read path — the event log itself is the audit trail, which is the Ephemeral contract. Gated by `load_run_visible_to_tenant` for cross-tenant isolation.",
        },
    },
    ProjectionEntry {
        variant: "OperatorProfileCreated",
        status: ProjectionStatus::Projected {
            table: Some("operator_profiles"),
        },
    },
    ProjectionEntry {
        variant: "OperatorProfileUpdated",
        status: ProjectionStatus::Projected {
            table: Some("operator_profiles"),
        },
    },
    ProjectionEntry {
        variant: "OutcomeRecorded",
        status: ProjectionStatus::Projected {
            table: Some("outcomes"),
        },
    },
    ProjectionEntry {
        variant: "PauseScheduled",
        status: ProjectionStatus::Projected {
            table: Some("pause_schedules"),
        },
    },
    ProjectionEntry {
        variant: "PermissionDecisionRecorded",
        status: ProjectionStatus::Stubbed {
            tracking: "RFC-025 Phase 2b.5 (permission decisions — Ephemeral reclassification deferred; the in-memory applier is already a no-op and no reader exists anywhere in cairn-runtime / cairn-app, so the pg/sqlite log_stub is purely a tracking signal until the parallel pause-lifecycle work lands and the stub-guard diff window clears)",
        },
    },
    ProjectionEntry {
        variant: "PlanApproved",
        status: ProjectionStatus::Projected {
            table: Some("plan_reviews"),
        },
    },
    ProjectionEntry {
        variant: "PlanProposed",
        status: ProjectionStatus::Projected {
            table: Some("plan_reviews"),
        },
    },
    ProjectionEntry {
        variant: "PlanRejected",
        status: ProjectionStatus::Projected {
            table: Some("plan_reviews"),
        },
    },
    ProjectionEntry {
        variant: "PlanRevisionRequested",
        status: ProjectionStatus::Projected {
            table: Some("plan_reviews"),
        },
    },
    // RFC-025 Phase 3 (2026-04-28): 4 provider-state variants flipped
    // Stubbed → Projected. Operator-configured provider state now
    // survives restart (the core F40 contract). See
    // `docs/design/rfcs/RFC-025-provider-boundary-research.md` for the
    // research underpinning this classification:
    //   * Bindings + connections are PROJECTED (persistent config) —
    //     this is what Phase 3 ships.
    //   * Pools (ProviderPool*) are EPHEMERAL (see below): live HTTP-
    //     client state is not persistable; the pool is rebuilt from
    //     bindings + connections on boot.
    //   * Health probes (ProviderHealthChecked, ProviderMarkedDegraded,
    //     ProviderRecovered, ProviderHealthSchedule*) are EPHEMERAL:
    //     the next probe cycle supersedes any persisted status; there
    //     is no operator-visible read-after-restart contract.
    //   * Model capability announcements and retry policies (Provider
    //     ModelRegistered, ProviderRetryPolicySet) are EPHEMERAL: the
    //     in-memory applier is already a no-op today and the runtime
    //     layer has no reader; operator re-announces on restart. See
    //     the comments on each entry below.
    // RFC-025 Phase 2b.4 (2026-04-28): the 10 variants immediately
    // below moved from Stubbed → Ephemeral once the research above
    // was cross-checked against the in-memory applier + service-layer
    // read paths. The stub-guard CI job still catches any *new* stub
    // site, so this reclassification does not weaken the Phase 3b
    // scope signal — Phase 3b is now limited to the work needed to
    // surface ephemeral provider state to operators via SSE/metrics.
    ProjectionEntry {
        variant: "ProviderBindingCreated",
        status: ProjectionStatus::Projected {
            table: Some("provider_bindings"),
        },
    },
    ProjectionEntry {
        variant: "ProviderBindingStateChanged",
        status: ProjectionStatus::Projected {
            table: Some("provider_bindings"),
        },
    },
    ProjectionEntry {
        variant: "ProviderBudgetAlertTriggered",
        status: ProjectionStatus::Projected {
            table: Some("provider_budgets"),
        },
    },
    ProjectionEntry {
        variant: "ProviderBudgetExceeded",
        status: ProjectionStatus::Projected {
            table: Some("provider_budgets"),
        },
    },
    ProjectionEntry {
        variant: "ProviderBudgetSet",
        status: ProjectionStatus::Projected {
            table: Some("provider_budgets"),
        },
    },
    ProjectionEntry {
        variant: "ProviderConnectionDeleted",
        status: ProjectionStatus::Projected {
            table: Some("provider_connections"),
        },
    },
    ProjectionEntry {
        variant: "ProviderConnectionRegistered",
        status: ProjectionStatus::Projected {
            table: Some("provider_connections"),
        },
    },
    // RFC-025 Phase 2b.4: health probes are ephemeral. `ProviderHealth
    // Checked` updates an in-memory `ProviderHealthRecord` that callers
    // read via `ProviderHealthService::run_due_health_checks` — a
    // live-probe endpoint, not a read-after-restart surface. The next
    // probe cycle overwrites the in-memory row regardless of whether
    // the previous check survived restart. Persisting probe history
    // would require a dedicated bounded audit table (bounded-ring, not
    // append-forever) — that is a separate follow-up, not the core F40
    // durability contract. Today the event log itself is the audit
    // trail; operators observe live status via SSE + metrics.
    ProjectionEntry {
        variant: "ProviderHealthChecked",
        status: ProjectionStatus::Ephemeral {
            reason: "Health probe status is rebuilt on the next probe cycle. The in-memory `ProviderHealthRecord` exists only for the live `run_due_health_checks` endpoint; operators observe status via SSE + metrics. Persisting every probe hit would grow without bound with no read-after-restart contract. Event log is the audit trail.",
        },
    },
    ProjectionEntry {
        variant: "ProviderHealthScheduleSet",
        status: ProjectionStatus::Ephemeral {
            reason: "Health-check schedules are derived configuration rebuilt from provider_bindings at boot (the canonical config). The `ProviderHealthSchedule` in-memory record only drives the in-process scheduler loop; operators view + edit schedules via the binding CRUD path, not a schedule-specific projection. Event log is the audit trail.",
        },
    },
    ProjectionEntry {
        variant: "ProviderHealthScheduleTriggered",
        status: ProjectionStatus::Ephemeral {
            reason: "Schedule-tick `last_run_ms` is purely an in-process scheduler marker — restart resets the tick cadence and the next scheduler pass re-triggers probes. No operator read-after-restart surface. Event log is the audit trail.",
        },
    },
    ProjectionEntry {
        variant: "ProviderMarkedDegraded",
        status: ProjectionStatus::Ephemeral {
            reason: "The degraded bit lives on the in-memory `ProviderHealthRecord` and is superseded by the next `ProviderHealthChecked` / `ProviderRecovered` event in-process. Persisting it would suggest a read-after-restart contract cairn does not expose — operators see degradation via SSE + metrics. Event log is the audit trail.",
        },
    },
    ProjectionEntry {
        variant: "ProviderModelRegistered",
        status: ProjectionStatus::Ephemeral {
            reason: "Capability announcements are informational — the in-memory applier is already a no-op and the runtime layer exposes no `ProviderModelReadModel` consumer today (only the in-memory `InMemoryStore` impl exists, used by a handful of tests). Operator re-announces on restart; the event log keeps the audit trail.",
        },
    },
    ProjectionEntry {
        variant: "ProviderPoolConnectionAdded",
        status: ProjectionStatus::Ephemeral {
            reason: "Pool membership tracks live HTTP-client state; it is rebuilt from provider_bindings + provider_connections on boot. Persisting the mutation would diverge from the live pool the moment reqwest reconnects. Event log is the audit trail.",
        },
    },
    ProjectionEntry {
        variant: "ProviderPoolConnectionRemoved",
        status: ProjectionStatus::Ephemeral {
            reason: "Counterpart to `ProviderPoolConnectionAdded` — pool membership is live-HTTP-client state rebuilt from bindings + connections at boot. Event log is the audit trail.",
        },
    },
    ProjectionEntry {
        variant: "ProviderPoolCreated",
        status: ProjectionStatus::Ephemeral {
            reason: "Pools are live HTTP-client state (active_connections, reqwest::Client references). Rebuilt from provider_bindings + provider_connections on boot; persistence would diverge from reality. Event log is the audit trail.",
        },
    },
    ProjectionEntry {
        variant: "ProviderRecovered",
        status: ProjectionStatus::Ephemeral {
            reason: "Recovery flip complements `ProviderMarkedDegraded` — the next probe cycle is authoritative, the in-memory status survives only until the next `ProviderHealthChecked`. No operator read-after-restart surface. Event log is the audit trail.",
        },
    },
    ProjectionEntry {
        variant: "ProviderRetryPolicySet",
        status: ProjectionStatus::Ephemeral {
            reason: "Retry policy today has no reader in cairn-runtime — the HTTP handler only appends the event (see `set_provider_retry_policy_handler` in cairn-app/src/handlers/providers.rs). Operator re-sets on restart; persisting would suggest a read-after-restart contract cairn does not implement. Event log is the audit trail.",
        },
    },
    ProjectionEntry {
        variant: "RecoveryEscalated",
        status: ProjectionStatus::Ephemeral {
            reason: "The event carries no tenant_id so a tenant-scoped read-model projection would require a new event version (domain change). Until then the `RecoveryEscalationReadModel` in-memory impl keeps the observability-only contract: escalations surface via SSE + metrics and the event log is the audit trail. Revisit post-v0.1 when RecoveryEscalated gains tenant_id.",
        },
    },
    ProjectionEntry {
        variant: "ResourceShareRevoked",
        status: ProjectionStatus::Projected {
            table: Some("resource_shares"),
        },
    },
    ProjectionEntry {
        variant: "ResourceShared",
        status: ProjectionStatus::Projected {
            table: Some("resource_shares"),
        },
    },
    ProjectionEntry {
        variant: "RetentionPolicySet",
        status: ProjectionStatus::Projected {
            table: Some("retention_policies"),
        },
    },
    ProjectionEntry {
        variant: "RoutePolicyUpdated",
        status: ProjectionStatus::Projected {
            table: Some("route_policies"),
        },
    },
    ProjectionEntry {
        variant: "RunCostAlertSet",
        status: ProjectionStatus::Projected {
            table: Some("run_cost_alerts"),
        },
    },
    ProjectionEntry {
        variant: "RunCostAlertTriggered",
        status: ProjectionStatus::Projected {
            table: Some("run_cost_alerts"),
        },
    },
    ProjectionEntry {
        variant: "RunCostUpdated",
        status: ProjectionStatus::Projected {
            table: Some("run_costs"),
        },
    },
    ProjectionEntry {
        variant: "ScheduledTaskCreated",
        status: ProjectionStatus::Projected {
            table: Some("scheduled_tasks"),
        },
    },
    ProjectionEntry {
        variant: "SignalIngested",
        status: ProjectionStatus::Projected {
            table: Some("signal_ingestions"),
        },
    },
    ProjectionEntry {
        variant: "SoulPatchApplied",
        status: ProjectionStatus::Projected {
            table: Some("soul_patches"),
        },
    },
    ProjectionEntry {
        variant: "SoulPatchProposed",
        status: ProjectionStatus::Projected {
            table: Some("soul_patches"),
        },
    },
    ProjectionEntry {
        variant: "SpendAlertTriggered",
        status: ProjectionStatus::Ephemeral {
            reason: "No reader in cairn-runtime or cairn-app — the event is appended as an audit record and operators consume it via SSE (see `cairn-app/src/helpers.rs` event-type classification). The in-memory applier is already a no-op; pg/sqlite match. Event log is the audit trail.",
        },
    },
    ProjectionEntry {
        variant: "SubagentSpawned",
        status: ProjectionStatus::Projected {
            table: Some("subagent_spawns"),
        },
    },
    ProjectionEntry {
        variant: "TenantQuotaSet",
        status: ProjectionStatus::Projected {
            table: Some("tenant_quotas"),
        },
    },
    ProjectionEntry {
        variant: "TenantQuotaViolated",
        status: ProjectionStatus::Projected {
            table: Some("tenant_quota_violations"),
        },
    },
    ProjectionEntry {
        variant: "ToolRecoveryPaused",
        status: ProjectionStatus::Projected {
            table: Some("tool_recovery_pauses"),
        },
    },
    ProjectionEntry {
        variant: "UserMessageAppended",
        status: ProjectionStatus::Projected {
            table: Some("user_messages"),
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
        // RFC-025 Phase 1 milestone 7: EvalRunStarted flipped from
        // Stubbed → Projected now that the pg/sqlite/in-memory
        // `eval_runs` projection table is wired.
        let status = lookup("EvalRunStarted").expect("EvalRunStarted is Projected (Phase 1)");
        assert!(status.is_projected());
        // RFC-025 Phase 2a.1 milestone 1: CredentialStored flipped from
        // Stubbed → Projected now that pg V035 + sqlite schema carry the
        // `credentials` read-model table.
        let status =
            lookup("CredentialStored").expect("CredentialStored is Projected (Phase 2a.1)");
        assert!(status.is_projected());
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
        // Phase 0/1: this MUST fail until every Stubbed variant has a
        // real projection; the error payload names each Stubbed entry
        // so the operator sees a concrete action list in the boot log.
        let err = assert_no_stubs_for_persistent_backend(Backend::Postgres)
            .expect_err("Phase 1: registry still carries Stubbed variants pending Phase 2a/2b");
        let RegistryError::StubbedVariantsPresent { backend, stubbed } = err;
        assert_eq!(backend, Backend::Postgres);
        assert!(
            !stubbed.is_empty(),
            "Phase 1 should surface at least one stubbed variant (Phase 2a/2b backlog)"
        );
        // Spot-check variants still in the Stubbed bucket post-Phase-2b.4 m1.
        // Provider health / pools / model / retry variants moved to
        // Ephemeral in Phase 2b.4 m1 (bindings + connections were the
        // only persistent provider-state required by the F40 durability
        // contract). Eval baselines / datasets / rubrics, operator
        // interventions / profiles, permissions, route-policy-Updated,
        // run-cost / spend-alert, and `PauseScheduled` are the 15
        // variants that remain Stubbed pending Phase 2b.4 m2-m4 and
        // the parallel pause-lifecycle work.
        // Post-Phase-2b.4 m4 the Stubbed bucket holds only
        // `PermissionDecisionRecorded` (Phase 2b.5 follow-up — the
        // Ephemeral reclassification is correct but was deferred).
        // PR #595 took PauseScheduled Projected.
        assert!(
            stubbed.contains(&"PermissionDecisionRecorded"),
            "PermissionDecisionRecorded should still be Stubbed post-Phase-2b.4 m4"
        );
        // Confirm every Phase 2b.4 migration left the Stubbed bucket.
        for migrated in [
            "EvalBaselineLocked",
            "EvalBaselineSet",
            "EvalDatasetCreated",
            "EvalDatasetEntryAdded",
            "EvalRubricCreated",
            "OperatorProfileCreated",
            "OperatorProfileUpdated",
            "OperatorIntervention",
            "PauseScheduled",
            "RoutePolicyUpdated",
            "RunCostAlertSet",
            "RunCostAlertTriggered",
            "RunCostUpdated",
            "SpendAlertTriggered",
        ] {
            assert!(
                !stubbed.contains(&migrated),
                "{migrated} should be Projected/Ephemeral after Phase 2b.4"
            );
        }
        // Confirm Phase 2b.4 m1 ephemeral reclassification left the
        // Stubbed bucket for every provider health / pool / model /
        // retry variant.
        for migrated in [
            "ProviderHealthChecked",
            "ProviderHealthScheduleSet",
            "ProviderHealthScheduleTriggered",
            "ProviderMarkedDegraded",
            "ProviderModelRegistered",
            "ProviderPoolCreated",
            "ProviderPoolConnectionAdded",
            "ProviderPoolConnectionRemoved",
            "ProviderRecovered",
            "ProviderRetryPolicySet",
        ] {
            assert!(
                !stubbed.contains(&migrated),
                "{migrated} should be Ephemeral after Phase 2b.4 milestone 1"
            );
        }
        // Confirm Phase-1 eval migrations stayed out of Stubbed.
        for migrated in [
            "EvalRunStarted",
            "EvalRunCompleted",
            "EvalRunArchived",
            "EvalRunScored",
            "EvalRubricScored",
        ] {
            assert!(
                !stubbed.contains(&migrated),
                "{migrated} should be Projected after Phase 1 milestone 7"
            );
        }
        // Confirm Phase-2a.1 milestone 1 credentials left the Stubbed
        // bucket.
        for migrated in [
            "CredentialStored",
            "CredentialRevoked",
            "CredentialKeyRotated",
        ] {
            assert!(
                !stubbed.contains(&migrated),
                "{migrated} should be Projected after Phase 2a.1 milestone 1"
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
        // RFC-025 Phase 1 baselines:
        //   * Phase 0 shipped 48 Projected / 31 Ephemeral / 77 Stubbed.
        //   * Milestone 1 added EvalRunScored + EvalRubricScored as
        //     Ephemeral staging → 48 / 33 / 77.
        //   * Milestone 7 flips five eval variants (Started / Completed
        //     / Archived / Scored / RubricScored) to Projected, removing
        //     two Ephemeral + three Stubbed → 53 / 31 / 74.
        //   * Phase 2a.1 milestone 1 flips three credential variants
        //     (CredentialStored / Revoked / KeyRotated) to Projected,
        //     removing three from Stubbed → 56 / 31 / 71.
        //   * Phase 2a.1 milestone 2 flips two tenant-quota variants
        //     (TenantQuotaSet / Violated) to Projected → 58 / 31 / 69.
        //   * Phase 2a.1 milestone 3 flips three provider-budget variants
        //     (ProviderBudgetSet / AlertTriggered / Exceeded) to Projected
        //     → 61 / 31 / 66.
        //   * Phase 2a.1 milestone 4 flips `LicenseActivated` to Projected
        //     → 62 / 31 / 65.
        //   * Phase 1.5a (PR #569) flips 8 state-carrying trigger /
        //     run_template variants (TriggerCreated, TriggerEnabled,
        //     TriggerDisabled, TriggerSuspended, TriggerResumed,
        //     TriggerDeleted, RunTemplateCreated, RunTemplateDeleted)
        //     from Ephemeral to Projected with backing table `triggers`
        //     / `run_templates`. The 5 audit variants (TriggerFired,
        //     Skipped, Denied, RateLimited, PendingApproval) also flip
        //     Ephemeral → Projected with backing table `trigger_fires`
        //     — they write real projection rows even though the runtime
        //     does not recover entity state from individual audit rows
        //     at boot (per PR #569 Copilot review — Projected contract
        //     is "backed by a read-model table updated synchronously",
        //     not "runtime replays from the table"). Net: +13 Projected,
        //     -13 Ephemeral → 75 / 18 / 65.
        //   * Phase 3 (PR #572) flips the four provider-state variants
        //     (ProviderBindingCreated / StateChanged, ProviderConnection
        //     Registered / Deleted) Stubbed → Projected with backing
        //     tables `provider_bindings` / `provider_connections`. Pools
        //     and health probes stay Stubbed (Phase 3b will flip them
        //     to Ephemeral once the pool / health read-models land).
        //     Net: +4 Projected, -4 Stubbed → 79 / 18 / 61.
        //   * Phase 2b.1 milestone 1 flips `AuditLogEntryRecorded`
        //     Stubbed → Projected with backing table `audit_log_entries`
        //     (pg V041 + sqlite schema.rs). Net: +1 Projected,
        //     -1 Stubbed → 80 / 18 / 60.
        //   * Phase 2b.1 milestone 2 flips `ScheduledTaskCreated`
        //     Stubbed → Projected with backing table `scheduled_tasks`
        //     (pg V042 + sqlite schema.rs). Net: +1 Projected,
        //     -1 Stubbed → 81 / 18 / 59.
        //   * Phase 2b.1 milestone 3 flips `OutcomeRecorded` Stubbed →
        //     Projected with backing table `outcomes` (pg V043 +
        //     sqlite schema.rs). Net: +1 Projected, -1 Stubbed
        //     → 82 / 18 / 58.
        //   * Phase 2b.1 milestone 4 flips the four RFC 018 Plan-review
        //     events (`PlanProposed`, `PlanApproved`, `PlanRejected`,
        //     `PlanRevisionRequested`) Stubbed → Projected with backing
        //     table `plan_reviews` (pg V044 + sqlite schema.rs).
        //     Net: +4 Projected, -4 Stubbed → 86 / 18 / 54.
        //   * Phase 2a.2 milestone 1 flips `ApprovalDelegated` Stubbed →
        //     Projected with backing table `approval_delegations` (pg V045
        //     + sqlite schema.rs). Net: +1 Projected, -1 Stubbed
        //     → 87 / 18 / 53.
        //   * Phase 2a.2 milestone 2 flips `GuardrailPolicyCreated` +
        //     `GuardrailPolicyEvaluated` Stubbed → Projected with backing
        //     tables `guardrail_policies` + `guardrail_evaluations` (pg
        //     V046 + sqlite schema.rs). Net: +2 Projected, -2 Stubbed
        //     → 89 / 18 / 51.
        //   * Phase 2a.2 milestone 3 flips `RetentionPolicySet` Stubbed →
        //     Projected with backing table `retention_policies` (pg V047
        //     + sqlite schema.rs). Net: +1 Projected, -1 Stubbed
        //     → 90 / 18 / 50.
        //   * Phase 2a.2 milestone 4 flips `EntitlementOverrideSet`
        //     Stubbed → Projected with backing table `entitlement_overrides`
        //     (pg V048 + sqlite schema.rs). Net: +1 Projected, -1 Stubbed
        //     → 91 / 18 / 49.
        //   * Phase 2b.2 milestone 1 flips the four `ExternalWorker*`
        //     events (`Registered`, `Suspended`, `Reactivated`,
        //     `Reported`) Stubbed → Projected with backing table
        //     `external_workers` (pg V049 — renumbered from V045 after
        //     Phase 2a.2 took V045-V048; sqlite schema.rs).
        //     Net: +4 Projected, -4 Stubbed → 95 / 18 / 45.
        //   * Phase 2b.2b milestone 1 flips `ResourceShared` +
        //     `ResourceShareRevoked` Stubbed → Projected with backing
        //     table `resource_shares` (pg V051 + sqlite schema.rs).
        //     Net: +2 Projected, -2 Stubbed → 97 / 18 / 43.
        //   * Phase 2b.2b milestone 2 flips `SignalIngested` Stubbed →
        //     Projected with backing table `signal_ingestions` (pg V052
        //     + sqlite schema.rs). Net: +1 Projected, -1 Stubbed
        //     → 98 / 18 / 42.
        //   * Phase 2b.2b milestone 3 flips `SubagentSpawned` Stubbed →
        //     Projected with backing table `subagent_spawns` (pg V053
        //     + sqlite schema.rs). Net: +1 Projected, -1 Stubbed
        //     → 99 / 18 / 41.
        //   * Phase 2b.2b milestone 4 flips `UserMessageAppended`
        //     Stubbed → Projected with backing table `user_messages`
        //     (pg V054 + sqlite schema.rs). Net: +1 Projected,
        //     -1 Stubbed → 100 / 18 / 40.
        //   * Phase 2b.2b milestone 5 flips `SoulPatchProposed` +
        //     `SoulPatchApplied` Stubbed → Projected with backing
        //     table `soul_patches` (pg V055 + sqlite schema.rs).
        //     Net: +2 Projected, -2 Stubbed → 102 / 18 / 38.
        //   * Phase 2b.2b milestone 6 flips `ToolRecoveryPaused`
        //     Stubbed → Projected with backing table
        //     `tool_recovery_pauses` (pg V056 + sqlite schema.rs),
        //     and flips `EventLogCompacted` + `RecoveryEscalated`
        //     Stubbed → Ephemeral (see registry comments for each on
        //     why no read-model row is needed / achievable without
        //     domain changes).
        //     Net: +1 Projected, +2 Ephemeral, -3 Stubbed
        //     → 103 / 20 / 35.
        //   * Phase 2b.3 milestone 1 flips `IngestJobStarted` +
        //     `IngestJobCompleted` Stubbed → Projected with backing
        //     table `ingest_jobs` (pg V057 + sqlite schema.rs).
        //     Net: +2 Projected, -2 Stubbed → 105 / 20 / 33.
        //   * Phase 2b.3 milestone 2 flips `DefaultSettingSet` +
        //     `DefaultSettingCleared` Stubbed → Projected with backing
        //     table `default_settings` (pg V058 + sqlite schema.rs).
        //     Net: +2 Projected, -2 Stubbed → 107 / 20 / 31.
        //   * Phase 2b.3 milestone 3 flips `ChannelCreated` +
        //     `ChannelMessageSent` + `ChannelMessageConsumed` Stubbed →
        //     Projected with backing tables `channels` + `channel_messages`
        //     (pg V059 + sqlite schema.rs).
        //     Net: +3 Projected, -3 Stubbed → 110 / 20 / 28.
        //   * Phase 2b.3 milestone 4 flips `NotificationPreferenceSet` +
        //     `NotificationSent` Stubbed → Projected with backing tables
        //     `notification_preferences` + `notifications`
        //     (pg V060 + sqlite schema.rs).
        //     Net: +2 Projected, -2 Stubbed → 112 / 20 / 26.
        //   * Phase 2b.3 milestone 5 flips `CheckpointStrategySet`
        //     Stubbed → Projected with backing table
        //     `checkpoint_strategies` (pg V061 + sqlite schema.rs).
        //     Net: +1 Projected, -1 Stubbed → 113 / 20 / 25.
        //   * PR #595 (issue #592, parallel pause-lifecycle agent):
        //     flips `PauseScheduled` Stubbed → Projected with backing
        //     table `pause_schedules` (pg V062 from that PR + sqlite
        //     schema.rs). Net: +1 Projected, -1 Stubbed → 114 / 20 / 24.
        //   * Phase 2b.4 milestone 1 flips the 10 provider-state
        //     variants that Phase 3 deferred (`ProviderHealthChecked`,
        //     `ProviderHealthScheduleSet`, `ProviderHealthSchedule
        //     Triggered`, `ProviderMarkedDegraded`, `ProviderModel
        //     Registered`, `ProviderPoolCreated`, `ProviderPool
        //     ConnectionAdded`, `ProviderPoolConnectionRemoved`,
        //     `ProviderRecovered`, `ProviderRetryPolicySet`) Stubbed →
        //     Ephemeral. Pools + health probes are live-HTTP-client
        //     state rebuilt from bindings on boot; retry + model events
        //     have no reader in cairn-runtime. See each registry entry
        //     for the per-variant rationale.
        //     Net: +10 Ephemeral, -10 Stubbed → 114 / 30 / 14.
        //   * Phase 2b.4 milestone 2 flips the five eval-catalog
        //     variants (`EvalBaselineLocked`, `EvalBaselineSet`,
        //     `EvalDatasetCreated`, `EvalDatasetEntryAdded`,
        //     `EvalRubricCreated`) Stubbed → Projected with backing
        //     tables `eval_datasets` + `eval_dataset_entries` +
        //     `eval_rubrics` + `eval_baselines` (pg V063 + sqlite
        //     schema.rs; renumbered from V062 after PR #595 took V062
        //     for `pause_schedules`). Net: +5 Projected, -5 Stubbed
        //     → 119 / 30 / 9.
        //   * Phase 2b.4 milestone 3 flips `OperatorProfileCreated` +
        //     `OperatorProfileUpdated` Stubbed → Projected with backing
        //     table `operator_profiles` (pg V064 + sqlite schema.rs),
        //     and flips `OperatorIntervention` Stubbed → Ephemeral
        //     (intervention is read via an event-log walk so the log
        //     itself is the projection). `PermissionDecisionRecorded`
        //     is deferred to Phase 2b.5 — the Ephemeral reclassification
        //     is correct (no reader, audit-only).
        //     Net: +2 Projected, +1 Ephemeral, -2 Stubbed
        //     → 121 / 31 / 7.
        //   * Phase 2b.4 milestone 4 flips `RunCostUpdated` +
        //     `RunCostAlertSet` + `RunCostAlertTriggered` +
        //     `RoutePolicyUpdated` Stubbed → Projected with backing
        //     tables `run_costs` + `run_cost_alerts` (pg V065 + sqlite
        //     schema.rs; renumbered from V064) and a delta-update on
        //     the existing `route_policies` row, and flips
        //     `SpendAlertTriggered` Stubbed → Ephemeral (audit-only;
        //     no reader in cairn).
        //     Net: +4 Projected, +1 Ephemeral, -5 Stubbed
        //     → 125 / 32 / 1.
        //     The lone remaining Stubbed variant is
        //     `PermissionDecisionRecorded` (Phase 2b.5 Ephemeral
        //     reclassification). PR #595 took `PauseScheduled`
        //     Projected ahead of 2b.4 landing.
        // If you're editing this test, confirm the registry edit
        // matches the milestone you're landing.
        assert_eq!(
            projected, 125,
            "Projected count drifted; update registry + RFC"
        );
        assert_eq!(
            ephemeral, 32,
            "Ephemeral count drifted; update registry + RFC"
        );
        assert_eq!(stubbed, 1, "Stubbed count drifted; update registry + RFC");
        assert_eq!(projected + ephemeral + stubbed, 158);
    }

    #[test]
    fn error_display_includes_variant_list() {
        let err = assert_no_stubs_for_persistent_backend(Backend::Postgres).unwrap_err();
        let msg = err.to_string();
        // Audits + scheduled tasks + outcomes + plan-reviews + external
        // workers + resource-sharing + signal-ingest + subagents +
        // soul-patches + user-messages + tool-recovery-paused +
        // recovery-escalated + event-log-compacted all left the Stubbed
        // bucket in Phases 2b.1/2b.2/2b.2b. Ingest-jobs + defaults +
        // channels left in Phase 2b.3 m1/m2/m3. Pick a later-phase
        // variant that still lives in the Stubbed bucket.
        assert!(msg.contains("PermissionDecisionRecorded"));
        assert!(msg.contains("Postgres") || msg.contains("postgres"));
    }
}
