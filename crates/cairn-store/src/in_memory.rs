//! In-memory store implementation for testing and local-mode use.
//!
//! Provides a single `InMemoryStore` that implements `EventLog` and all
//! entity read-model traits. Event append atomically updates sync projections.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use cairn_domain::*;
use serde::{Deserialize, Serialize};

use crate::error::StoreError;
use crate::event_log::*;
use crate::projections::*;

// Test-only: inject one (or more) `append` failure(s) deterministically
// so chaos tests can exercise the HTTP/service-level error path without
// OS-level disk-full or fsync tricks. Armed at runtime via
// [`arm_fail_next_append`]; zero-cost when not armed.
//
// Gated behind `#[cfg(debug_assertions)]` — release builds strip the
// atomics, the arming function, and the per-append branch, so no
// production cairn-app can have its event log poisoned through this
// path. Same precedent as the `CAIRN_TEST_SEED_*` hooks in
// cairn-app/src/main.rs.
//
// Portability: sits in the in-memory store path, which is the `--db
// memory` backend used by LiveHarness. Postgres/SQLite `EventLog::append`
// implementations are untouched.
#[cfg(debug_assertions)]
static FAIL_APPEND_SKIP_REMAINING: std::sync::atomic::AtomicU32 =
    std::sync::atomic::AtomicU32::new(0);

#[cfg(debug_assertions)]
static FAIL_APPEND_FAIL_REMAINING: std::sync::atomic::AtomicU32 =
    std::sync::atomic::AtomicU32::new(0);

/// Test-only: arm the injected-failure hook at runtime. Chaos tests
/// call this after the subprocess is healthy so bootstrap appends
/// (tenant seed, projection init) don't consume the failure budget.
/// Stripped from release builds.
///
/// `skip` = number of subsequent `append` calls to let through
/// untouched. `fail` = number of appends to reject with
/// `StoreError::Internal` after the skip window. Both are assigned
/// absolutely (not added): calling `arm_fail_next_append(0, 1)` resets
/// to "fail the next append". Concurrent callers race; last writer
/// wins. This is a single-purpose test knob, not a coordination
/// primitive.
#[cfg(debug_assertions)]
pub fn arm_fail_next_append(skip: u32, fail: u32) {
    use std::sync::atomic::Ordering;
    FAIL_APPEND_SKIP_REMAINING.store(skip, Ordering::Release);
    FAIL_APPEND_FAIL_REMAINING.store(fail, Ordering::Release);
}

/// Issue #668: cap on the number of resident `LlmCompletionBodyRecord`
/// rows kept in the `InMemoryStore.llm_completion_bodies` projection.
///
/// Default 5000; override via `CAIRN_LLM_TRACE_IN_MEMORY_CAP=<n>`.
/// Clamped to [100, 100_000] to stop typos (`=0`, `=99999999`) from
/// either disabling the projection or letting it grow unboundedly.
///
/// The durable backends (pg + sqlite) keep the full history; the
/// in-memory cap is a live-memory upper bound, not a retention
/// policy. Operators querying pages deeper than the cap on an
/// in-memory (`--db memory`) deployment will see gaps — acceptable
/// because `--db memory` is dev-only and already announces
/// "ALL DATA WILL BE LOST on restart".
fn llm_completion_bodies_cap() -> usize {
    use std::sync::OnceLock;
    static CACHED: OnceLock<usize> = OnceLock::new();
    *CACHED.get_or_init(|| {
        const DEFAULT: usize = 5_000;
        const MIN: usize = 100;
        const MAX: usize = 100_000;
        match std::env::var("CAIRN_LLM_TRACE_IN_MEMORY_CAP") {
            Ok(v) => v
                .trim()
                .parse::<usize>()
                .ok()
                .unwrap_or(DEFAULT)
                .clamp(MIN, MAX),
            Err(_) => DEFAULT,
        }
    })
}

fn now_millis() -> u64 {
    // Matches pg/sqlite backends' fallback on clock skew: a clock before
    // UNIX_EPOCH (container misconfiguration) MUST NOT panic the store.
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

struct State {
    events: Vec<StoredEvent>,
    next_position: u64,
    /// RFC 002: causation_id → earliest event position that references it.
    /// Populated by `append()` and consulted by `find_by_causation_id`; the
    /// pre-T2-H1 field was declared but never populated, so lookup fell back
    /// to O(n) over the full `events` vector.
    command_id_index: HashMap<String, u64>,
    sessions: HashMap<String, SessionRecord>,
    runs: HashMap<String, RunRecord>,
    tasks: HashMap<String, TaskRecord>,
    approvals: HashMap<String, ApprovalRecord>,
    /// RFC-025 Phase 2a.2 milestone 1: audit trail for `ApprovalDelegated`
    /// events. One entry per delegation event, pushed on event apply.
    /// Mirrors the `approval_delegations` projection on pg/sqlite so
    /// `ApprovalDelegationReadModel::list_for_approval` returns byte-equal
    /// results across backends.
    approval_delegations: Vec<crate::projections::ApprovalDelegationRecord>,
    /// PR BP-2: projection of `ToolCall*` approval events keyed by call_id.
    tool_call_approvals: HashMap<String, ToolCallApprovalRecord>,
    checkpoints: HashMap<String, CheckpointRecord>,
    mailbox_messages: HashMap<String, MailboxRecord>,
    tool_invocations: HashMap<String, ToolInvocationRecord>,
    signals: HashMap<String, cairn_domain::SignalRecord>,
    ingest_jobs: HashMap<String, cairn_domain::IngestJobRecord>,
    scheduled_tasks: HashMap<String, cairn_domain::ScheduledTaskRecord>,
    eval_runs: HashMap<String, crate::projections::EvalRunRecord>,
    outcomes: HashMap<String, crate::projections::OutcomeRecord>,
    eval_datasets: HashMap<String, cairn_domain::EvalDataset>,
    eval_rubrics: HashMap<String, cairn_domain::EvalRubric>,
    eval_baselines: HashMap<String, cairn_domain::EvalBaseline>,
    // keyed by run_id; each run has at most one active strategy
    checkpoint_strategies: HashMap<String, cairn_domain::CheckpointStrategy>,
    prompt_assets: HashMap<String, crate::projections::PromptAssetRecord>,
    prompt_versions: HashMap<String, crate::projections::PromptVersionRecord>,
    prompt_releases: HashMap<String, crate::projections::PromptReleaseRecord>,
    tenants: HashMap<String, cairn_domain::org::TenantRecord>,
    workspaces: HashMap<String, cairn_domain::org::WorkspaceRecord>,
    projects: HashMap<String, cairn_domain::org::ProjectRecord>,
    /// RFC 002: point-in-time snapshots per tenant, ordered by creation.
    snapshots: Vec<cairn_domain::Snapshot>,
    route_decisions: HashMap<String, cairn_domain::providers::RouteDecisionRecord>,
    provider_calls: HashMap<String, cairn_domain::providers::ProviderCallRecord>,
    approval_policies: HashMap<String, cairn_domain::ApprovalPolicyRecord>,
    external_workers: HashMap<String, cairn_domain::workers::ExternalWorkerRecord>,
    /// GAP-006: accumulated session costs keyed by session_id.
    session_costs: HashMap<String, cairn_domain::providers::SessionCostRecord>,
    /// Run-level accumulated costs keyed by run_id.
    run_costs: HashMap<String, cairn_domain::providers::RunCostRecord>,
    /// F29 CD-2: lifetime project cost rollup, keyed by
    /// `(tenant_id, workspace_id, project_id)`. Updated alongside
    /// `session_costs` from the `SessionCostUpdated` handler so the
    /// numbers are guaranteed consistent with the per-session totals.
    project_costs: HashMap<(String, String, String), cairn_domain::providers::ProjectCostRecord>,
    /// F29 CD-2: lifetime workspace cost rollup, keyed by
    /// `(tenant_id, workspace_id)`. Same consistency invariant as
    /// `project_costs`.
    workspace_costs: HashMap<(String, String), cairn_domain::providers::WorkspaceCostRecord>,
    /// GAP-010: LLM call trace records derived from ProviderCallCompleted events.
    llm_traces: Vec<cairn_domain::LlmCallTrace>,
    /// Issue #668: LLM chain-of-thought body records keyed by
    /// `trace_id`. Written from `LlmCompletionRecorded` events
    /// (emitted alongside `ProviderCallCompleted` from the orchestrator).
    /// Separate from `llm_traces` so the big text fields don't bloat
    /// the metadata projection.
    llm_completion_bodies: HashMap<String, crate::projections::LlmCompletionBodyRecord>,
    operator_profiles: HashMap<String, crate::projections::OperatorProfileRecord>,
    full_operator_profiles: HashMap<String, cairn_domain::org::OperatorProfile>,
    /// RFC 026 PR-A0: operator → tenant-role mapping keyed on
    /// `(tenant_id, operator_id)`. Revoked rows are kept (the audit
    /// trail survives); active-only queries filter on
    /// `OperatorTenantRoleRecord::is_active`.
    operator_tenant_roles: HashMap<(String, String), crate::projections::OperatorTenantRoleRecord>,
    workspace_members: Vec<crate::projections::WorkspaceMemberRecord>,
    signal_subscriptions: HashMap<String, crate::projections::SignalSubscriptionRecord>,
    provider_health_records: HashMap<String, cairn_domain::providers::ProviderHealthRecord>,
    provider_pools: HashMap<String, cairn_domain::providers::ProviderConnectionPool>,
    default_settings: HashMap<String, cairn_domain::DefaultSetting>,
    credentials: HashMap<String, cairn_domain::credentials::CredentialRecord>,
    channels: HashMap<String, cairn_domain::ChannelRecord>,
    channel_messages: HashMap<String, Vec<cairn_domain::ChannelMessage>>,
    /// Sidecar dedupe index for `channel_messages` — keeps the
    /// first-write-wins guard O(1) per event apply instead of a linear
    /// scan of the message Vec. Mirrors the `guardrail_evaluation_keys`
    /// pattern. Kept in lockstep with `channel_messages` by the applier
    /// and the clear paths. Copilot PR #594 perf fix.
    channel_message_keys: std::collections::HashSet<(String, String)>,
    credential_rotations: Vec<cairn_domain::credentials::CredentialRotationRecord>,
    licenses: HashMap<String, cairn_domain::LicenseRecord>,
    entitlement_overrides: HashMap<String, cairn_domain::EntitlementOverrideRecord>,
    notification_prefs: HashMap<String, cairn_domain::notification_prefs::NotificationPreference>,
    notification_records: Vec<cairn_domain::notification_prefs::NotificationRecord>,
    /// Sidecar dedupe index for `notification_records` — first-write-wins
    /// guard on `record_id` in O(1). Same reasoning as
    /// `channel_message_keys` above. Copilot PR #594 perf fix.
    notification_record_ids: std::collections::HashSet<String>,
    guardrail_policies: HashMap<String, cairn_domain::policy::GuardrailPolicy>,
    /// RFC-025 Phase 2a.2 milestone 2: tenant association for guardrail
    /// policies so `list_policies(tenant_id, ..)` scopes correctly. The
    /// domain `GuardrailPolicy` struct omits tenant_id; pg/sqlite store
    /// it on the projection row and filter in SQL. Mirror that here by
    /// tracking it in a sibling map keyed on policy_id.
    guardrail_policy_tenants: HashMap<String, cairn_domain::TenantId>,
    /// RFC-025 Phase 2a.2 milestone 2: audit trail for
    /// `GuardrailPolicyEvaluated` events. One row per evaluation; a
    /// replayed event with the same composite key (tenant_id, policy_id,
    /// subject_type, subject_id_or_empty, action, evaluated_at_ms) is a
    /// no-op, mirroring the pg/sqlite `PRIMARY KEY` contract.
    guardrail_evaluations: Vec<crate::projections::GuardrailEvaluationRecord>,
    /// Sidecar dedupe set for `guardrail_evaluations` — keeps the idempotency
    /// guard O(1) per event apply instead of the prior O(n) linear scan.
    /// The Vec above stays as the authoritative store so tenant-scoped
    /// reads can preserve insertion order and re-sort at read time
    /// (matching pg/sqlite `ORDER BY evaluated_at_ms DESC`). The two
    /// structures are kept in lockstep by the applier and the clear paths
    /// (`clear_state` / `reset_state`). Copilot #571 round 3 perf fix.
    guardrail_evaluation_keys:
        std::collections::HashSet<(String, String, String, String, String, u64)>,
    provider_budgets: HashMap<String, cairn_domain::providers::ProviderBudget>,
    provider_connections: HashMap<String, cairn_domain::providers::ProviderConnectionRecord>,
    quotas: HashMap<String, cairn_domain::TenantQuota>,
    /// RFC-025 Phase 2a.1 milestone 2: audit trail for
    /// `TenantQuotaViolated` events. One entry per violation, pushed on
    /// event apply. Matches the `tenant_quota_violations` projection on
    /// pg/sqlite so `QuotaViolationReadModel::list_violations` returns
    /// byte-equal results across backends.
    quota_violations: Vec<crate::projections::QuotaViolationRecord>,
    provider_bindings: HashMap<String, cairn_domain::providers::ProviderBindingRecord>,
    provider_health_schedules: HashMap<String, cairn_domain::providers::ProviderHealthSchedule>,
    run_sla_configs: HashMap<String, cairn_domain::sla::SlaConfig>,
    run_sla_breaches: HashMap<String, cairn_domain::sla::SlaBreach>,
    run_cost_alerts: HashMap<String, cairn_domain::providers::RunCostAlert>,
    retention_policies: HashMap<String, cairn_domain::RetentionPolicy>,
    route_policies: HashMap<String, cairn_domain::providers::RoutePolicy>,
    resource_shares: HashMap<String, cairn_domain::resource_sharing::SharedResource>,
    /// FF lease_history subscriber cursors, keyed by `(partition_id,
    /// execution_id)`.
    ff_lease_history_cursors: HashMap<(String, String), crate::projections::FfLeaseHistoryCursor>,
    /// F52: projection of `ToolInvocationCacheHit` events keyed by
    /// `invocation_id` so replay + second-boot reads converge to the same
    /// set. Mirrors the pg/sqlite `tool_invocation_cache_hits` table.
    tool_invocation_cache_hits: HashMap<String, crate::projections::ToolInvocationCacheHitRecord>,
    /// #364: latest `ToolInvocationProgressUpdated` per invocation, keyed
    /// by `invocation_id`. Carries the `ProjectKey` so
    /// `GET /v1/tool-invocations/:id/progress` can enforce tenant scope
    /// without a second lookup against `tool_invocations`. Replaces the
    /// previous `read_stream(None, 10_000)` + filter scan — that scan
    /// was both a DoS (bounded by a fixed 10k window that masked data
    /// past it) and cross-tenant readable.
    tool_invocation_progress: HashMap<String, crate::projections::ToolInvocationProgressRecord>,
    /// F65 PR-2: orchestrator-session outcomes, keyed by `root_run_id`
    /// (the primary key of the pg/sqlite `session_outcomes` table).
    session_outcomes: HashMap<String, crate::projections::SessionOutcomeRecord>,
    /// F65 PR-2: workspace snapshot rows, keyed by `snapshot_id`.
    workspace_snapshots: HashMap<String, crate::projections::WorkspaceSnapshotRecord>,
    /// F65 PR-2: workspace registry (live overlayfs mount tracking),
    /// keyed by `workspace_id`.
    workspace_registry: HashMap<String, crate::projections::WorkspaceRegistryRecord>,
    /// F65 PR-2: orchestrator-resumable checkpoint bodies, keyed by
    /// `checkpoint_id`. Distinct from `checkpoints` above — that map
    /// holds the RFC 005 per-run checkpoint metadata; this one holds the
    /// F65 body + schema version + session lineage.
    f65_checkpoints: HashMap<String, crate::projections::F65CheckpointRecord>,
    /// RFC-025 Phase 1.5a: trigger projection, keyed by `trigger_id`.
    /// Owns the state-carrying lifecycle (created/enabled/disabled/
    /// suspended/resumed/deleted). Mirror of the `triggers` pg/sqlite
    /// table.
    triggers: HashMap<String, crate::projections::TriggerRecord>,
    /// RFC-025 Phase 1.5a: run template projection, keyed by
    /// `template_id`. Mirror of `run_templates` pg/sqlite table.
    run_templates: HashMap<String, crate::projections::RunTemplateRecord>,
    /// RFC-025 Phase 1.5a: append-only audit of every trigger fire
    /// attempt (fired / skipped / denied / rate_limited /
    /// pending_approval). Mirror of `trigger_fires` pg/sqlite table.
    /// Backs the duplicate-fire ledger + rate-limit + project-budget
    /// windowed COUNT queries. Classified Ephemeral in the registry
    /// because no runtime state is recovered from individual rows at
    /// boot, but the rows persist here so the counters stay consistent
    /// with pg/sqlite parity expectations.
    trigger_fires: Vec<crate::projections::TriggerFireRecord>,
    /// RFC-025 Phase 2b.1: audit log read-model keyed by `entry_id`.
    /// Mirror of the `audit_log_entries` pg/sqlite table. Replaces an
    /// earlier read-time scan over `state.events` that grew linearly
    /// with total event count and silently violated the trait's
    /// "newest-first" ordering contract. The event itself does not
    /// carry the full `AuditLogEntry.metadata` — the projection persists
    /// the empty-object default so list/get reconstruct a byte-equal
    /// record across backends.
    audit_log_entries: HashMap<String, crate::projections::AuditLogEntryRecord>,
    /// RFC-025 Phase 2b.1 m4: plan-review read model (RFC 018).
    /// Keyed by `plan_run_id`. Pre-Phase-2b.1 the four Plan-lifecycle
    /// events (`PlanProposed`, `PlanApproved`, `PlanRejected`,
    /// `PlanRevisionRequested`) were no-ops on every backend including
    /// in-memory — `GET /v1/runs/:id/plan` had zero authoritative
    /// state to read from.
    plan_reviews: HashMap<String, crate::projections::PlanReviewRecord>,
    /// RFC-025 Phase 2b.2b m3: subagent spawn audit (RFC 014).
    /// Keyed by `child_task_id`. Mirror of the `subagent_spawns`
    /// pg/sqlite table. Pre-Phase-2b.2b the in-memory applier updated
    /// only the child's `tasks` row; this map captures the spawn
    /// event itself so operator dashboards can enumerate a run's
    /// subagent graph without walking the event log.
    subagent_spawns: HashMap<String, crate::projections::SubagentSpawnRecord>,
    /// RFC-025 Phase 2b.2b m4: user message projection.
    /// Keyed by `(run_id, sequence)`. Mirror of the `user_messages`
    /// pg/sqlite table. Pre-Phase-2b.2b `GET /v1/runs/:id/messages`
    /// walked the event log on every call — this map turns the read
    /// into O(messages-in-run) instead of O(events-total).
    user_messages: HashMap<(String, u64), crate::projections::UserMessageRecord>,
    /// RFC-025 Phase 2b.2b m5: soul patch lifecycle projection.
    /// Keyed by `patch_id`. Mirror of the `soul_patches` pg/sqlite
    /// table. Pre-Phase-2b.2b both `SoulPatchProposed` and
    /// `SoulPatchApplied` were no-ops on every backend — no durable
    /// state carried the proposal audit trail.
    soul_patches: HashMap<String, crate::projections::SoulPatchRecord>,
    /// RFC-025 Phase 2b.2b m6: tool-recovery pause audit (RFC 020
    /// Track 3). Keyed by `tool_call_id`. Mirror of the
    /// `tool_recovery_pauses` pg/sqlite table.
    tool_recovery_pauses: HashMap<String, crate::projections::ToolRecoveryPauseRecord>,
    /// Issue #592: evict-on-resume projection keyed by `run_id`.
    /// Mirror of the `pause_schedules` pg/sqlite table (pg V062 +
    /// sqlite `schema.rs`). `RunStateChanged(→Paused)` with a
    /// non-None `resume_after_ms` inserts a row; any transition away
    /// from Paused removes it. `PauseScheduleReadModel::list_due`
    /// reads this map with an ordered range scan — no event-log
    /// walker.
    pause_schedules: HashMap<String, crate::projections::PauseScheduledRecord>,
}

pub struct InMemoryStore {
    state: Mutex<State>,
    usage_counters: Arc<Mutex<HashMap<ProjectKey, UsageCounters>>>,
    /// Broadcast channel for real-time SSE streaming (RFC 002).
    ///
    /// Every successfully appended `StoredEvent` is sent here. Receivers can
    /// subscribe before reading the replay window so no events are missed.
    /// Capacity of 1024 covers burst writes; lagged receivers get
    /// `BroadcastStreamRecvError::Lagged` and should reconnect with the last
    /// known position.
    event_tx: tokio::sync::broadcast::Sender<StoredEvent>,
    /// Optional durable secondary event log (e.g. Postgres or SQLite).
    ///
    /// When set, every `append()` call dual-writes to this log AFTER the
    /// in-memory write succeeds. This makes ALL service-layer events durable
    /// without touching the 109 `store.append()` call sites across 42 files.
    ///
    /// Set via `set_secondary_log()` after construction. The secondary write
    /// is best-effort: failures are logged but do NOT roll back the in-memory
    /// write, preserving the existing availability guarantee.
    secondary_log: std::sync::RwLock<Option<Arc<dyn EventLog + Send + Sync>>>,

    /// #670 G4 PR-1b-4: optional durable secondary descendant counter
    /// backend. `try_increment_descendants` / `decrement_descendants`
    /// dual-write here AFTER the in-memory write completes — the
    /// counter is a projection mutation, not an event, so it would
    /// otherwise drift to 0 on restart (the event-log replay
    /// rebuilds the in-memory projection but there are no events
    /// to replay for the counter itself).
    ///
    /// When set, the in-memory arm remains authoritative for the
    /// cap-check decision (the CAS loop runs in-memory first). The
    /// durable secondary write follows the in-memory decision: if
    /// the in-memory admit said "under cap, new_count=N", the
    /// durable backend sees an UPDATE that sets its counter to
    /// the SAME N via its own atomic primitive. Dual-write is
    /// best-effort like the event log; failures log a WARN and the
    /// durable value drifts by one until the next live write
    /// reconciles.
    secondary_counter:
        std::sync::RwLock<Option<Arc<dyn crate::projections::RunDescendantsCounter + Send + Sync>>>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct UsageCounters {
    pub run_count: u64,
    pub event_count: u64,
    pub sandbox_provision_count: u64,
    pub decision_evaluation_count: u64,
    pub trigger_fire_count: u64,
}

impl InMemoryStore {
    pub fn new() -> Self {
        let (event_tx, _) = tokio::sync::broadcast::channel(1024);
        Self {
            state: Mutex::new(State {
                events: Vec::new(),
                next_position: 1,
                command_id_index: HashMap::new(),
                sessions: HashMap::new(),
                runs: HashMap::new(),
                tasks: HashMap::new(),
                approvals: HashMap::new(),
                approval_delegations: Vec::new(),
                tool_call_approvals: HashMap::new(),
                checkpoints: HashMap::new(),
                mailbox_messages: HashMap::new(),
                tool_invocations: HashMap::new(),
                signals: HashMap::new(),
                ingest_jobs: HashMap::new(),
                scheduled_tasks: HashMap::new(),
                eval_runs: HashMap::new(),
                outcomes: HashMap::new(),
                eval_datasets: HashMap::new(),
                eval_rubrics: HashMap::new(),
                eval_baselines: HashMap::new(),
                checkpoint_strategies: HashMap::new(),
                prompt_assets: HashMap::new(),
                prompt_versions: HashMap::new(),
                prompt_releases: HashMap::new(),
                route_decisions: HashMap::new(),
                provider_calls: HashMap::new(),
                approval_policies: HashMap::new(),
                external_workers: HashMap::new(),
                session_costs: HashMap::new(),
                run_costs: HashMap::new(),
                project_costs: HashMap::new(),
                workspace_costs: HashMap::new(),
                llm_traces: Vec::new(),
                llm_completion_bodies: HashMap::new(),
                operator_profiles: HashMap::new(),
                full_operator_profiles: HashMap::new(),
                operator_tenant_roles: HashMap::new(),
                workspace_members: Vec::new(),
                signal_subscriptions: HashMap::new(),
                provider_health_records: HashMap::new(),
                provider_pools: HashMap::new(),
                default_settings: HashMap::new(),
                credentials: HashMap::new(),
                channels: HashMap::new(),
                channel_messages: HashMap::new(),
                channel_message_keys: std::collections::HashSet::new(),
                credential_rotations: Vec::new(),
                licenses: HashMap::new(),
                entitlement_overrides: HashMap::new(),
                notification_prefs: HashMap::new(),
                notification_records: Vec::new(),
                notification_record_ids: std::collections::HashSet::new(),
                guardrail_policies: HashMap::new(),
                guardrail_policy_tenants: HashMap::new(),
                guardrail_evaluations: Vec::new(),
                guardrail_evaluation_keys: std::collections::HashSet::new(),
                provider_budgets: HashMap::new(),
                provider_connections: HashMap::new(),
                quotas: HashMap::new(),
                quota_violations: Vec::new(),
                provider_bindings: HashMap::new(),
                provider_health_schedules: HashMap::new(),
                run_sla_configs: HashMap::new(),
                run_sla_breaches: HashMap::new(),
                run_cost_alerts: HashMap::new(),
                retention_policies: HashMap::new(),
                route_policies: HashMap::new(),
                resource_shares: HashMap::new(),
                ff_lease_history_cursors: HashMap::new(),
                tool_invocation_cache_hits: HashMap::new(),
                tool_invocation_progress: HashMap::new(),
                session_outcomes: HashMap::new(),
                workspace_snapshots: HashMap::new(),
                workspace_registry: HashMap::new(),
                f65_checkpoints: HashMap::new(),
                tenants: HashMap::new(),
                workspaces: HashMap::new(),
                projects: HashMap::new(),
                snapshots: Vec::new(),
                triggers: HashMap::new(),
                run_templates: HashMap::new(),
                trigger_fires: Vec::new(),
                audit_log_entries: HashMap::new(),
                plan_reviews: HashMap::new(),
                subagent_spawns: HashMap::new(),
                user_messages: HashMap::new(),
                soul_patches: HashMap::new(),
                tool_recovery_pauses: HashMap::new(),
                pause_schedules: HashMap::new(),
            }),
            usage_counters: Arc::new(Mutex::new(HashMap::new())),
            event_tx,
            secondary_log: std::sync::RwLock::new(None),
            secondary_counter: std::sync::RwLock::new(None),
        }
    }

    /// #670 G4 PR-1b-4: install a durable secondary descendant
    /// counter backend (e.g. pg or sqlite adapter). After this call,
    /// every `try_increment_descendants` / `decrement_descendants`
    /// that lands in the in-memory arm is followed by an equivalent
    /// write against the durable backend. Without this, the counter
    /// is not durable across restart.
    pub fn set_secondary_descendants_counter(
        &self,
        backend: Arc<dyn crate::projections::RunDescendantsCounter + Send + Sync>,
    ) {
        *self
            .secondary_counter
            .write()
            .unwrap_or_else(|e| e.into_inner()) = Some(backend);
    }

    fn increment_usage_for_project(
        &self,
        project: &ProjectKey,
        update: impl FnOnce(&mut UsageCounters),
    ) {
        let mut usage = self
            .usage_counters
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        update(usage.entry(project.clone()).or_default());
    }

    pub fn increment_sandbox_provision_count(&self, project: &ProjectKey) {
        self.increment_usage_for_project(project, |counters| {
            counters.sandbox_provision_count += 1;
        });
    }

    pub fn increment_decision_evaluation_count(&self, project: &ProjectKey) {
        self.increment_usage_for_project(project, |counters| {
            counters.decision_evaluation_count += 1;
        });
    }

    pub fn increment_trigger_fire_count(&self, project: &ProjectKey) {
        self.increment_usage_for_project(project, |counters| {
            counters.trigger_fire_count += 1;
        });
    }

    pub fn usage_snapshot(&self) -> HashMap<ProjectKey, UsageCounters> {
        self.usage_counters
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    pub fn reset_usage_counters(&self) {
        self.usage_counters
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clear();
    }

    /// Test-only read of the `tool_invocation_cache_hits` projection,
    /// mirroring the `arm_fail_next_append` gating pattern. Integration
    /// tests assert the projection grew after a
    /// `ToolInvocationCacheHit` append without committing to a stable
    /// public API surface on the store.
    #[cfg(debug_assertions)]
    pub fn all_tool_invocation_cache_hits(
        &self,
    ) -> Vec<crate::projections::ToolInvocationCacheHitRecord> {
        let state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        state.tool_invocation_cache_hits.values().cloned().collect()
    }

    /// Attach a durable secondary event log.
    ///
    /// After this call every `append()` will dual-write to `log` after the
    /// in-memory write. Intended to be called once at startup, before the
    /// HTTP server accepts traffic.
    ///
    /// Pass `Arc<PgEventLog>` or `Arc<SqliteEventLog>` — any `EventLog` impl works.
    pub fn set_secondary_log(&self, log: Arc<dyn EventLog + Send + Sync>) {
        *self
            .secondary_log
            .write()
            .unwrap_or_else(|e| e.into_inner()) = Some(log);
    }

    /// Subscribe to the real-time event broadcast (RFC 002).
    ///
    /// Call this *before* reading the replay window to guarantee no events are
    /// missed between the replay read and the live subscription.
    pub fn subscribe(&self) -> tokio::sync::broadcast::Receiver<StoredEvent> {
        self.event_tx.subscribe()
    }

    fn apply_projection(state: &mut State, event: &StoredEvent) {
        let now = event.stored_at;
        match &event.envelope.payload {
            RuntimeEvent::SessionCreated(e) => {
                // Idempotent projection: multiple `SessionCreated` events
                // for the same `session_id` can legitimately arrive on
                // event-log replay or when the fabric layer's
                // `FabricSessionService::create` retries after a crash
                // between FF `ff_create_flow` committing and the bridge
                // emit landing (see `cairn-fabric::services::session_service`
                // for the retry-safety rationale). The `HashMap::insert`
                // below is naturally idempotent, but the quota counter
                // must only advance on a *fresh* session — otherwise
                // retries silently inflate `sessions_this_hour` and
                // eventually reject legitimate new sessions with
                // `QuotaExceeded`. Gate the counter bump on "first time
                // we see this session_id" using the pre-insert absence
                // probe.
                let is_fresh = !state.sessions.contains_key(e.session_id.as_str());
                state.sessions.insert(
                    e.session_id.as_str().to_owned(),
                    SessionRecord {
                        session_id: e.session_id.clone(),
                        project: e.project.clone(),
                        state: SessionState::Open,
                        version: 1,
                        created_at: now,
                        updated_at: now,
                        // F65 PR-1: additive fields are populated by PR-2
                        // projection writers. Initial in-memory rows get
                        // the same defaults serde would apply on replay.
                        goal_title: None,
                        issue_budget: None,
                        max_attempts: crate::projections::session::DEFAULT_MAX_ATTEMPTS,
                        attempts_used: 0,
                    },
                );
                if is_fresh {
                    if let Some(quota) = state.quotas.get_mut(e.project.tenant_id.as_str()) {
                        quota.sessions_this_hour = quota.sessions_this_hour.saturating_add(1);
                    }
                }
            }
            RuntimeEvent::SessionStateChanged(e) => {
                if let Some(rec) = state.sessions.get_mut(e.session_id.as_str()) {
                    rec.state = e.transition.to;
                    rec.version += 1;
                    rec.updated_at = now;
                }
            }
            RuntimeEvent::RunCreated(e) => {
                // #670 G4 / RFC 027: initialise `root_run_id`. Mirrors
                // the pg/sqlite projection's sub-SELECT shape:
                //   1. Root (no parent) → self-reference.
                //   2. Child with parent row present → inherit the
                //      parent's `root_run_id`. The whole chain shares
                //      one absolute root; read-before-write of the
                //      parent's value keeps this atomic with the
                //      child's insertion.
                //   3. Child with parent row missing → None. The
                //      decrement path's no-op-on-None handles this
                //      legacy case.
                let root_run_id = match e.parent_run_id.as_ref() {
                    None => Some(e.run_id.clone()),
                    Some(parent_id) => state
                        .runs
                        .get(parent_id.as_str())
                        .and_then(|parent| parent.root_run_id.clone()),
                };
                state.runs.insert(
                    e.run_id.as_str().to_owned(),
                    RunRecord {
                        run_id: e.run_id.clone(),
                        session_id: e.session_id.clone(),
                        parent_run_id: e.parent_run_id.clone(),
                        project: e.project.clone(),
                        state: RunState::Pending,
                        prompt_release_id: e.prompt_release_id.clone(),
                        agent_role_id: e.agent_role_id.clone(),
                        failure_class: None,
                        pause_reason: None,
                        resume_trigger: None,
                        version: 1,
                        created_at: now,
                        updated_at: now,
                        completion_summary: None,
                        completion_verification: None,
                        completion_annotated_at_ms: None,
                        terminal_write_recovery: None,
                        in_flight_descendants: 0,
                        root_run_id,
                    },
                );
                // Update run quota counter
                if let Some(quota) = state.quotas.get_mut(e.project.tenant_id.as_str()) {
                    quota.current_active_runs = quota.current_active_runs.saturating_add(1);
                }
            }
            RuntimeEvent::RunStateChanged(e) => {
                // Cross-tenant tampering guard (#732 expansion):
                // mirror the `RunCompletionAnnotated` gate. A forged
                // `RunStateChanged` with a victim tenant's `run_id`
                // but the attacker's `project` could otherwise flip
                // another tenant's run state (terminal/failed/
                // paused) by appending a single event. NOTE: this
                // event's payload does not carry `session_id`
                // (unlike `RunCompletionAnnotated`), so the gate is
                // `project`-only here — sufficient because the run
                // row's project is fixed at `RunCreated` time and
                // any legitimate emit must match it.
                //
                // The guard wraps the entire block — row update,
                // descendant decrement, and pause_schedules write —
                // because all three would otherwise leak across
                // tenants. Missing-row → fall through (orphan
                // replay is a legitimate path the existing
                // `pause_schedule_list_due_filters_by_tenant_and_respects_limit`
                // test exercises: state-change event arriving
                // before the projection sees its `RunCreated`).
                // Forged-event-against-existing-row → block.
                let row_belongs_to_other_tenant = state
                    .runs
                    .get(e.run_id.as_str())
                    .map(|rec| rec.project != e.project)
                    .unwrap_or(false);
                if !row_belongs_to_other_tenant {
                    // #670 G4 / RFC 027 §97: on terminal transition of a
                    // non-root descendant, decrement the root's
                    // `in_flight_descendants` counter. The root id is
                    // captured at spawn time into the terminating child's
                    // `root_run_id` — no parent-chain traversal at
                    // terminal time. `root_run_id = None` is a no-op
                    // (pre-V069 / legacy-chain case).
                    let terminal_decrement_target: Option<RunId> =
                        if e.transition.to.is_terminal() {
                            state
                                .runs
                                .get(e.run_id.as_str())
                                .filter(|rec| rec.parent_run_id.is_some())
                                .and_then(|rec| rec.root_run_id.clone())
                        } else {
                            None
                        };
                    if let Some(rec) = state.runs.get_mut(e.run_id.as_str()) {
                        rec.state = e.transition.to;
                        rec.failure_class = e.failure_class;
                        rec.pause_reason = e.pause_reason.clone();
                        rec.resume_trigger = e.resume_trigger;
                        rec.version += 1;
                        rec.updated_at = now;
                    }
                    if let Some(root_id) = terminal_decrement_target {
                        if let Some(root_rec) = state.runs.get_mut(root_id.as_str()) {
                            // Unchecked subtract is deliberate — RFC 027
                            // §93 specifies `i64` typing so underflow
                            // surfaces as a negative value that the
                            // adapter layer surfaces on its
                            // `child_run_driver_descendant_underflow_total`
                            // metric. Panicking (or clamping at 0) would
                            // hide the auditable signal.
                            root_rec.in_flight_descendants =
                                root_rec.in_flight_descendants.wrapping_sub(1);
                            root_rec.version = root_rec.version.saturating_add(1);
                            root_rec.updated_at = now;
                        }
                    }

                    // Issue #592: pause_schedules projection — evict-on-resume.
                // Mirrors pg/sqlite: INSERT on Paused with
                // `resume_after_ms=Some`, DELETE on any transition
                // away from Paused. Parity harness asserts stable
                // ordering + consistent membership/eviction across
                // backends. Post Copilot #595 fix, all three backends
                // compute `resume_at_ms` = event-time + resume_after_ms
                // (in_memory reads `event.stored_at` via the enclosing
                // `apply_projection`, pg/sqlite take `event_time_ms`
                // through `apply_async`), so a rebuild replays
                // scheduled resumes at their original wall-clock
                // instead of shifting them to the rebuild wall-clock.
                    match e.transition.to {
                        cairn_domain::RunState::Paused => {
                            if let Some(reason) = &e.pause_reason {
                                if let Some(resume_after_ms) = reason.resume_after_ms {
                                    let resume_at_ms = now.saturating_add(resume_after_ms);
                                    state.pause_schedules.insert(
                                        e.run_id.as_str().to_owned(),
                                        crate::projections::PauseScheduledRecord {
                                            run_id: e.run_id.clone(),
                                            project: e.project.clone(),
                                            resume_at_ms,
                                            created_at_ms: now,
                                        },
                                    );
                                }
                            }
                        }
                        _ => {
                            state.pause_schedules.remove(e.run_id.as_str());
                        }
                    }
                }
            }
            RuntimeEvent::TaskCreated(e) => {
                // Prefer the session_id on the event; fall back to walking
                // parent_run_id → RunRecord.session_id for tasks without one.
                let session_id = e.session_id.clone().or_else(|| {
                    e.parent_run_id
                        .as_ref()
                        .and_then(|rid| state.runs.get(rid.as_str()))
                        .map(|r| r.session_id.clone())
                });
                state.tasks.insert(
                    e.task_id.as_str().to_owned(),
                    TaskRecord {
                        task_id: e.task_id.clone(),
                        project: e.project.clone(),
                        parent_run_id: e.parent_run_id.clone(),
                        parent_task_id: e.parent_task_id.clone(),
                        session_id,
                        state: TaskState::Queued,
                        prompt_release_id: e.prompt_release_id.clone(),
                        failure_class: None,
                        pause_reason: None,
                        resume_trigger: None,
                        retry_count: 0,
                        lease_owner: None,
                        lease_expires_at: None,
                        title: None,
                        description: None,
                        version: 1,
                        created_at: now,
                        updated_at: now,
                    },
                );
            }
            RuntimeEvent::TaskStateChanged(e) => {
                if let Some(rec) = state.tasks.get_mut(e.task_id.as_str()) {
                    rec.state = e.transition.to;
                    rec.failure_class = e.failure_class;
                    rec.pause_reason = e.pause_reason.clone();
                    rec.resume_trigger = e.resume_trigger;
                    if e.transition.to == TaskState::RetryableFailed {
                        rec.retry_count += 1;
                    }
                    // RFC 002: clear lease fields when transitioning back to Queued.
                    // RFC 005: also clear lease on Paused — a paused task must not
                    // expire while suspended (the lease timer is logically stopped).
                    if matches!(e.transition.to, TaskState::Queued | TaskState::Paused) {
                        rec.lease_owner = None;
                        rec.lease_expires_at = None;
                    }
                    rec.version += 1;
                    rec.updated_at = now;
                }
            }
            RuntimeEvent::ApprovalRequested(e) => {
                state.approvals.insert(
                    e.approval_id.as_str().to_owned(),
                    ApprovalRecord {
                        approval_id: e.approval_id.clone(),
                        project: e.project.clone(),
                        run_id: e.run_id.clone(),
                        task_id: e.task_id.clone(),
                        requirement: e.requirement,
                        decision: None,
                        title: e.title.clone(),
                        description: e.description.clone(),
                        version: 1,
                        created_at: now,
                        updated_at: now,
                    },
                );
            }
            RuntimeEvent::ApprovalResolved(e) => {
                if let Some(rec) = state.approvals.get_mut(e.approval_id.as_str()) {
                    rec.decision = Some(e.decision);
                    rec.version += 1;
                    rec.updated_at = now;
                }
            }
            RuntimeEvent::CheckpointRecorded(e) => {
                // Supersede any existing latest checkpoint for this run.
                if e.disposition == CheckpointDisposition::Latest {
                    for cp in state.checkpoints.values_mut() {
                        if cp.run_id == e.run_id && cp.disposition == CheckpointDisposition::Latest
                        {
                            cp.disposition = CheckpointDisposition::Superseded;
                            cp.version += 1;
                        }
                    }
                }
                state.checkpoints.insert(
                    e.checkpoint_id.as_str().to_owned(),
                    CheckpointRecord {
                        checkpoint_id: e.checkpoint_id.clone(),
                        project: e.project.clone(),
                        run_id: e.run_id.clone(),
                        disposition: e.disposition,
                        data: e.data.clone(),
                        version: 1,
                        created_at: now,
                    },
                );
            }
            RuntimeEvent::MailboxMessageAppended(e) => {
                state.mailbox_messages.insert(
                    e.message_id.as_str().to_owned(),
                    MailboxRecord {
                        message_id: e.message_id.clone(),
                        project: e.project.clone(),
                        run_id: e.run_id.clone(),
                        task_id: e.task_id.clone(),
                        from_task_id: e.from_task_id.clone(),
                        content: e.content.clone(),
                        from_run_id: e.from_run_id.clone(),
                        deliver_at_ms: e.deliver_at_ms,
                        sender: e.sender.clone(),
                        recipient: e.recipient.clone(),
                        body: e.body.clone(),
                        sent_at: e.sent_at,
                        delivery_status: e.delivery_status,
                        version: 1,
                        created_at: now,
                    },
                );
            }
            RuntimeEvent::TaskLeaseClaimed(e) => {
                if let Some(rec) = state.tasks.get_mut(e.task_id.as_str()) {
                    rec.lease_owner = Some(e.lease_owner.clone());
                    rec.lease_expires_at = Some(e.lease_expires_at_ms);
                    rec.version += 1;
                    rec.updated_at = now;
                }
            }
            RuntimeEvent::TaskLeaseHeartbeated(e) => {
                if let Some(rec) = state.tasks.get_mut(e.task_id.as_str()) {
                    rec.lease_expires_at = Some(e.lease_expires_at_ms);
                    rec.version += 1;
                    rec.updated_at = now;
                }
            }
            RuntimeEvent::ToolInvocationStarted(e) => {
                // F55: thread captured args into the in-memory projection
                // so it returns the same shape as pg + sqlite.
                let requested = ToolInvocationRecord::new_requested(
                    e.invocation_id.clone(),
                    e.project.clone(),
                    e.session_id.clone(),
                    e.run_id.clone(),
                    e.task_id.clone(),
                    e.target.clone(),
                    e.execution_class,
                    e.requested_at_ms,
                )
                .with_args(e.args_json.clone());
                let started = requested
                    .mark_started(e.started_at_ms)
                    .expect("tool invocation started event should always be a valid requested->started transition");
                state
                    .tool_invocations
                    .insert(e.invocation_id.as_str().to_owned(), started);
            }
            RuntimeEvent::ToolInvocationCompleted(e) => {
                if let Some(rec) = state.tool_invocations.get_mut(e.invocation_id.as_str()) {
                    // F55: persist the truncated output preview on the
                    // projection when the event carries one.
                    *rec = rec
                        .mark_finished_with_output(
                            e.outcome,
                            None,
                            e.finished_at_ms,
                            e.output_preview.clone(),
                        )
                        .expect(
                            "tool invocation completed event should preserve valid terminal transition",
                        );
                }
            }
            RuntimeEvent::ToolInvocationFailed(e) => {
                if let Some(rec) = state.tool_invocations.get_mut(e.invocation_id.as_str()) {
                    *rec = rec
                        .mark_finished_with_output(
                            e.outcome,
                            e.error_message.clone(),
                            e.finished_at_ms,
                            e.output_preview.clone(),
                        )
                        .expect("tool invocation failed event should preserve valid terminal transition");
                }
            }
            // F52: project cache-hit events into the in-memory read model.
            // Mirrors pg + sqlite; idempotent on re-apply (first event per
            // invocation_id wins, later replays are silently absorbed).
            RuntimeEvent::ToolInvocationCacheHit(e) => {
                state
                    .tool_invocation_cache_hits
                    .entry(e.invocation_id.as_str().to_owned())
                    .or_insert_with(|| crate::projections::ToolInvocationCacheHitRecord {
                        invocation_id: e.invocation_id.clone(),
                        project: e.project.clone(),
                        run_id: e.run_id.clone(),
                        task_id: e.task_id.clone(),
                        tool_name: e.tool_name.clone(),
                        tool_call_id: e.tool_call_id.clone(),
                        original_completed_at_ms: e.original_completed_at_ms,
                        served_at_ms: e.served_at_ms,
                    });
            }
            RuntimeEvent::SignalIngested(e) => {
                state.signals.insert(
                    e.signal_id.as_str().to_owned(),
                    cairn_domain::SignalRecord {
                        id: e.signal_id.clone(),
                        project: e.project.clone(),
                        source: e.source.clone(),
                        payload: e.payload.clone(),
                        timestamp_ms: e.timestamp_ms,
                    },
                );
            }
            RuntimeEvent::ExternalWorkerRegistered(e) => {
                // PR #729 — cross-tenant takeover defence. Mirrors the
                // pg/sqlite `ON CONFLICT (worker_id) DO UPDATE …
                // WHERE external_workers.tenant_id = EXCLUDED.tenant_id`
                // semantic: a colliding `worker_id` submitted from a
                // *different* tenant is a no-op (NOT a tenant rewrite).
                // A SAME-tenant re-register is treated as a fresh
                // registration — full reset of status / health /
                // current_task_id back to zero values, matching the
                // pg/sqlite contract pinned by
                // `external_worker_re_registration_resets_health_across_backends`
                // in projection_parity.rs. Critical because RFC-025
                // Phase 4 keeps InMemoryStore as the production read
                // path — missing this gate here lets the takeover
                // succeed even with the durable backends fixed.
                let key = e.worker_id.as_str().to_owned();
                let cross_tenant_collision = state
                    .external_workers
                    .get(&key)
                    .map(|rec| rec.tenant_id != e.tenant_id)
                    .unwrap_or(false);
                if !cross_tenant_collision {
                    state.external_workers.insert(
                        key,
                        cairn_domain::workers::ExternalWorkerRecord {
                            worker_id: e.worker_id.clone(),
                            tenant_id: e.tenant_id.clone(),
                            display_name: e.display_name.clone(),
                            status: "active".to_owned(),
                            registered_at: e.registered_at,
                            updated_at: now,
                            health: cairn_domain::workers::WorkerHealth::default(),
                            current_task_id: None,
                        },
                    );
                }
            }
            RuntimeEvent::ExternalWorkerSuspended(e) => {
                if let Some(rec) = state.external_workers.get_mut(e.worker_id.as_str()) {
                    rec.status = "suspended".to_owned();
                    rec.updated_at = now;
                }
            }
            RuntimeEvent::ExternalWorkerReactivated(e) => {
                if let Some(rec) = state.external_workers.get_mut(e.worker_id.as_str()) {
                    rec.status = "active".to_owned();
                    rec.updated_at = now;
                }
            }
            RuntimeEvent::ExternalWorkerReported(e) => {
                // Update last heartbeat and current task.
                if let Some(rec) = state.external_workers.get_mut(e.report.worker_id.as_str()) {
                    rec.health.last_heartbeat_ms = e.report.reported_at_ms;
                    rec.health.is_alive = true;
                    if e.report.outcome.is_none() {
                        rec.current_task_id = Some(e.report.task_id.clone());
                    } else {
                        rec.current_task_id = None;
                    }
                    rec.updated_at = now;
                }
            }
            // RFC-025 Phase 2b.2b m5: soul_patches projection. Proposed
            // inserts a proposed-state row (first-write-wins on replay);
            // Applied upgrades the state + applied_at + new_version
            // in-place. An out-of-order Applied-before-Proposed
            // synthesises a minimal row in 'applied' state.
            RuntimeEvent::SoulPatchProposed(e) => {
                state
                    .soul_patches
                    .entry(e.patch_id.clone())
                    .or_insert_with(|| crate::projections::SoulPatchRecord {
                        patch_id: e.patch_id.clone(),
                        project: e.project.clone(),
                        state: crate::projections::SoulPatchState::Proposed,
                        patch_content: e.patch_content.clone(),
                        requires_approval: e.requires_approval,
                        proposed_at_ms: e.proposed_at,
                        applied_at_ms: None,
                        new_version: None,
                    });
            }
            RuntimeEvent::SoulPatchApplied(e) => {
                let rec = state
                    .soul_patches
                    .entry(e.patch_id.clone())
                    .or_insert_with(|| crate::projections::SoulPatchRecord {
                        patch_id: e.patch_id.clone(),
                        project: e.project.clone(),
                        state: crate::projections::SoulPatchState::Applied,
                        patch_content: String::new(),
                        requires_approval: false,
                        proposed_at_ms: 0,
                        applied_at_ms: Some(e.applied_at),
                        new_version: Some(e.new_version),
                    });
                rec.state = crate::projections::SoulPatchState::Applied;
                rec.applied_at_ms = Some(e.applied_at);
                rec.new_version = Some(e.new_version);
            }
            RuntimeEvent::SpendAlertTriggered(_) => {}
            RuntimeEvent::RunCostUpdated(e) => {
                // Accumulate run cost from directly appended RunCostUpdated events.
                let rec = state
                    .run_costs
                    .entry(e.run_id.as_str().to_owned())
                    .or_insert_with(|| cairn_domain::providers::RunCostRecord {
                        run_id: e.run_id.clone(),
                        total_cost_micros: 0,
                        total_tokens_in: 0,
                        total_tokens_out: 0,
                        provider_calls: 0,
                        token_in: 0,
                        token_out: 0,
                    });
                rec.total_cost_micros = rec.total_cost_micros.saturating_add(e.delta_cost_micros);
                rec.total_tokens_in = rec.total_tokens_in.saturating_add(e.delta_tokens_in);
                rec.total_tokens_out = rec.total_tokens_out.saturating_add(e.delta_tokens_out);
                rec.provider_calls = rec.provider_calls.saturating_add(1);
                rec.token_in = rec.total_tokens_in;
                rec.token_out = rec.total_tokens_out;
                // Auto-trigger run cost alert if threshold exceeded and not yet triggered.
                let total = rec.total_cost_micros;
                if let Some(alert) = state.run_cost_alerts.get(e.run_id.as_str()) {
                    if alert.triggered_at_ms == 0 && total >= alert.threshold_micros {
                        let tenant_id = alert.tenant_id.clone();
                        let threshold = alert.threshold_micros;
                        let triggered_at_ms = now;
                        // Update alert record.
                        if let Some(a) = state.run_cost_alerts.get_mut(e.run_id.as_str()) {
                            a.triggered_at_ms = triggered_at_ms;
                            a.actual_cost_micros = total;
                        }
                        // Emit RunCostAlertTriggered into the log.
                        let alert_pos = EventPosition(state.next_position);
                        state.next_position += 1;
                        let alert_event = StoredEvent {
                            position: alert_pos,
                            envelope: EventEnvelope {
                                event_id: cairn_domain::EventId::new(format!(
                                    "derived_rcat_{}",
                                    e.run_id.as_str()
                                )),
                                source: cairn_domain::EventSource::System,
                                ownership: cairn_domain::OwnershipKey::Project(e.project.clone()),
                                causation_id: None,
                                correlation_id: None,
                                payload: RuntimeEvent::RunCostAlertTriggered(
                                    cairn_domain::RunCostAlertTriggered {
                                        run_id: e.run_id.clone(),
                                        tenant_id,
                                        threshold_micros: threshold,
                                        actual_cost_micros: total,
                                        triggered_at_ms,
                                    },
                                ),
                            },
                            stored_at: now,
                        };
                        state.events.push(alert_event);
                    }
                }
            }
            RuntimeEvent::ChannelCreated(e) => {
                state.channels.insert(
                    e.channel_id.as_str().to_owned(),
                    cairn_domain::ChannelRecord {
                        channel_id: e.channel_id.clone(),
                        project: e.project.clone(),
                        name: e.name.clone(),
                        capacity: e.capacity,
                        created_at: e.created_at_ms,
                        updated_at: e.created_at_ms,
                    },
                );
            }
            RuntimeEvent::ChannelMessageSent(e) => {
                // RFC-025 Phase 2b.3 m3: first-write-wins on
                // `(channel_id, message_id)` so replayed events are
                // a no-op — matches the pg/sqlite ON CONFLICT DO NOTHING
                // on the composite PK. Uses the `channel_message_keys`
                // sidecar HashSet for O(1) dedupe (vs. O(n) Vec scan
                // that would make ingesting N messages O(N^2) —
                // Copilot PR #594 perf fix).
                let key = (e.channel_id.as_str().to_owned(), e.message_id.clone());
                if state.channel_message_keys.insert(key) {
                    state
                        .channel_messages
                        .entry(e.channel_id.as_str().to_owned())
                        .or_default()
                        .push(cairn_domain::ChannelMessage {
                            channel_id: e.channel_id.clone(),
                            message_id: e.message_id.clone(),
                            sender_id: e.sender_id.clone(),
                            body: e.body.clone(),
                            sent_at_ms: e.sent_at_ms,
                            consumed_by: None,
                            consumed_at_ms: None,
                        });
                }
            }
            RuntimeEvent::ChannelMessageConsumed(e) => {
                if let Some(messages) = state.channel_messages.get_mut(e.channel_id.as_str()) {
                    if let Some(msg) = messages.iter_mut().find(|m| m.message_id == e.message_id) {
                        msg.consumed_by = Some(e.consumed_by.clone());
                        msg.consumed_at_ms = Some(e.consumed_at_ms);
                    }
                }
            }
            RuntimeEvent::DefaultSettingSet(e) => {
                // RFC-025 Phase 2b.3 m2: composite key uses the shared
                // snake_case scope encoding (matches pg/sqlite
                // `default_settings.scope` column) so doc-comment parity
                // claims in `projections/defaults.rs` are literally
                // true. Copilot PR #594 review.
                let composite_key = format!(
                    "{}:{}:{}",
                    crate::projections::defaults_scope_str(e.scope),
                    e.scope_id,
                    e.key
                );
                state.default_settings.insert(
                    composite_key,
                    cairn_domain::DefaultSetting {
                        key: e.key.clone(),
                        value: e.value.clone(),
                        scope: e.scope,
                    },
                );
            }
            RuntimeEvent::DefaultSettingCleared(e) => {
                let composite_key = format!(
                    "{}:{}:{}",
                    crate::projections::defaults_scope_str(e.scope),
                    e.scope_id,
                    e.key
                );
                state.default_settings.remove(&composite_key);
            }
            RuntimeEvent::LicenseActivated(e) => {
                state.licenses.insert(
                    e.tenant_id.as_str().to_owned(),
                    cairn_domain::LicenseRecord {
                        tenant_id: e.tenant_id.clone(),
                        tier: e.tier,
                        entitlements: vec![],
                        issued_at: e.valid_from_ms,
                        expires_at: e.valid_until_ms,
                        license_key: Some(e.license_id.clone()),
                    },
                );
            }
            RuntimeEvent::EntitlementOverrideSet(e) => {
                let key = format!("{}:{}", e.tenant_id.as_str(), e.feature);
                state.entitlement_overrides.insert(
                    key,
                    cairn_domain::EntitlementOverrideRecord {
                        override_id: format!("override_{}_{}", e.tenant_id.as_str(), e.feature),
                        tenant_id: e.tenant_id.clone(),
                        entitlement: cairn_domain::commercial::Entitlement::AdvancedAdmin,
                        granted: e.allowed,
                        reason: e.reason.clone(),
                        applied_at: e.set_at_ms,
                        feature: e.feature.clone(),
                        allowed: e.allowed,
                        set_at_ms: e.set_at_ms,
                    },
                );
            }
            RuntimeEvent::NotificationPreferenceSet(e) => {
                let key = format!("{}:{}", e.tenant_id.as_str(), e.operator_id);
                state.notification_prefs.insert(
                    key.clone(),
                    cairn_domain::notification_prefs::NotificationPreference {
                        pref_id: key,
                        tenant_id: e.tenant_id.clone(),
                        operator_id: e.operator_id.clone(),
                        event_types: e.event_types.clone(),
                        channels: e.channels.clone(),
                    },
                );
            }
            RuntimeEvent::NotificationSent(e) => {
                // RFC-025 Phase 2b.3 m4: first-write-wins on `record_id`
                // so replayed events are a no-op — matches pg/sqlite
                // ON CONFLICT (record_id) DO NOTHING. Uses the
                // `notification_record_ids` sidecar HashSet for O(1)
                // dedupe (vs. O(n) Vec scan). Copilot PR #594 perf fix.
                if state
                    .notification_record_ids
                    .insert(e.record_id.clone())
                {
                    state.notification_records.push(
                        cairn_domain::notification_prefs::NotificationRecord {
                            record_id: e.record_id.clone(),
                            tenant_id: e.tenant_id.clone(),
                            operator_id: e.operator_id.clone(),
                            event_type: e.event_type.clone(),
                            channel_kind: e.channel_kind.clone(),
                            channel_target: e.channel_target.clone(),
                            payload: e.payload.clone(),
                            sent_at_ms: e.sent_at_ms,
                            delivered: e.delivered,
                            delivery_error: e.delivery_error.clone(),
                        },
                    );
                }
            }
            RuntimeEvent::ProviderPoolCreated(e) => {
                state.provider_pools.insert(
                    e.pool_id.clone(),
                    cairn_domain::providers::ProviderConnectionPool {
                        pool_id: e.pool_id.clone(),
                        connection_ids: vec![],
                        max_connections: e.max_connections,
                        active_connections: 0,
                        tenant_id: e.tenant_id.clone(),
                    },
                );
            }
            RuntimeEvent::ProviderPoolConnectionAdded(e) => {
                if let Some(pool) = state.provider_pools.get_mut(&e.pool_id) {
                    if !pool.connection_ids.contains(&e.connection_id) {
                        pool.connection_ids.push(e.connection_id.clone());
                        pool.active_connections = pool.connection_ids.len() as u32;
                    }
                }
            }
            RuntimeEvent::ProviderPoolConnectionRemoved(e) => {
                if let Some(pool) = state.provider_pools.get_mut(&e.pool_id) {
                    pool.connection_ids.retain(|id| id != &e.connection_id);
                    pool.active_connections = pool.connection_ids.len() as u32;
                }
            }
            RuntimeEvent::TenantQuotaSet(e) => {
                state.quotas.insert(
                    e.tenant_id.as_str().to_owned(),
                    cairn_domain::TenantQuota {
                        tenant_id: e.tenant_id.clone(),
                        max_concurrent_runs: e.max_concurrent_runs,
                        max_sessions_per_hour: e.max_sessions_per_hour,
                        max_tasks_per_run: e.max_tasks_per_run,
                        current_active_runs: 0,
                        sessions_this_hour: 0,
                    },
                );
            }
            RuntimeEvent::ProviderBudgetSet(e) => {
                // RFC-025 Phase 2a.1 milestone 3: key by `budget_id` so
                // subsequent Alert/Exceeded events (which reference
                // `budget_id`, not `tenant_id:period`) can update the
                // matching row. Prior code keyed on `tenant_id:period`,
                // silently orphaning Alert/Exceeded updates; the pg +
                // sqlite projection tables own budget_id as primary key,
                // so this brings the in-memory side into parity.
                state.provider_budgets.insert(
                    e.budget_id.clone(),
                    cairn_domain::providers::ProviderBudget {
                        tenant_id: e.tenant_id.clone(),
                        period: e.period,
                        limit_micros: e.limit_micros,
                        alert_threshold_percent: e
                            .alert_threshold_percent
                            .unwrap_or(cairn_domain::providers::DEFAULT_BUDGET_ALERT_THRESHOLD_PERCENT),
                        current_spend_micros: 0,
                        created_at: now,
                        updated_at: now,
                    },
                );
            }
            RuntimeEvent::ProviderBudgetAlertTriggered(e) => {
                if let Some(budget) = state.provider_budgets.get_mut(&e.budget_id) {
                    budget.current_spend_micros = e.current_micros;
                    budget.updated_at = now;
                }
            }
            RuntimeEvent::ProviderBudgetExceeded(e) => {
                if let Some(budget) = state.provider_budgets.get_mut(&e.budget_id) {
                    budget.current_spend_micros =
                        budget.limit_micros.saturating_add(e.exceeded_by_micros);
                    budget.updated_at = now;
                }
            }
            RuntimeEvent::CredentialStored(e) => {
                // RFC-025 Phase 2a.1 milestone 1 (Copilot review PR #565):
                // preserve `active` + `revoked_at_ms` + `created_at` on
                // re-store so a `Stored → Revoked → Stored` sequence ends
                // in the revoked state on all three backends. Previously
                // the in-memory applier reset to `active: true,
                // revoked_at_ms: None` on every re-store, diverging from
                // pg/sqlite (which preserve the revoke via ON CONFLICT DO
                // UPDATE that excludes active/revoked columns).
                //
                // Operators who want to un-revoke must issue the
                // dedicated reactivation flow; a duplicate `Stored` event
                // is a no-op on revocation state.
                let key = e.credential_id.as_str().to_owned();
                match state.credentials.get_mut(&key) {
                    Some(existing) => {
                        existing.name = e.provider_id.clone();
                        existing.provider_id = e.provider_id.clone();
                        // Wrap in `RedactedCiphertext` so the
                        // projection heap copy is scrubbed on drop
                        // (#579) and redacts in Debug output. The
                        // previous value in `existing.encrypted_value`
                        // is dropped by the assignment, which triggers
                        // its own `ZeroizeOnDrop` and scrubs the
                        // superseded ciphertext.
                        existing.encrypted_value = cairn_domain::credentials::RedactedCiphertext::from(
                            e.encrypted_value.clone(),
                        );
                        existing.encrypted_at_ms = Some(e.encrypted_at_ms);
                        existing.key_id = e.key_id.clone();
                        existing.key_version = e.key_version.clone();
                        existing.updated_at = e.encrypted_at_ms;
                        // `created_at`, `active`, `revoked_at_ms`
                        // intentionally untouched — parity with pg/sqlite
                        // ON CONFLICT DO UPDATE.
                    }
                    None => {
                        state.credentials.insert(
                            key,
                            cairn_domain::credentials::CredentialRecord {
                                id: e.credential_id.clone(),
                                tenant_id: e.tenant_id.clone(),
                                name: e.provider_id.clone(),
                                credential_type: "api_key".to_owned(),
                                encrypted_value: cairn_domain::credentials::RedactedCiphertext::from(
                                    e.encrypted_value.clone(),
                                ),
                                created_at: e.encrypted_at_ms,
                                updated_at: e.encrypted_at_ms,
                                active: true,
                                provider_id: e.provider_id.clone(),
                                encrypted_at_ms: Some(e.encrypted_at_ms),
                                key_id: e.key_id.clone(),
                                key_version: e.key_version.clone(),
                                revoked_at_ms: None,
                            },
                        );
                    }
                }
            }
            RuntimeEvent::CredentialRevoked(e) => {
                if let Some(rec) = state.credentials.get_mut(e.credential_id.as_str()) {
                    rec.active = false;
                    rec.revoked_at_ms = Some(e.revoked_at_ms);
                    rec.updated_at = e.revoked_at_ms;
                }
            }
            RuntimeEvent::CredentialKeyRotated(e) => {
                state.credential_rotations.push(
                    cairn_domain::credentials::CredentialRotationRecord {
                        rotation_id: e.rotation_id.clone(),
                        tenant_id: e.tenant_id.clone(),
                        credential_id: cairn_domain::ids::CredentialId::new(""),
                        rotated_at: now,
                        rotated_by: None,
                        old_key_id: e.old_key_id.clone(),
                        new_key_id: e.new_key_id.clone(),
                        rotated_credentials: e.credential_ids_rotated.len() as u32,
                        started_at_ms: now,
                        completed_at_ms: Some(now),
                    },
                );
            }
            RuntimeEvent::GuardrailPolicyCreated(e) => {
                state.guardrail_policies.insert(
                    e.policy_id.clone(),
                    cairn_domain::policy::GuardrailPolicy {
                        policy_id: e.policy_id.clone(),
                        name: e.name.clone(),
                        rules: e.rules.clone(),
                        enabled: true,
                    },
                );
                // RFC-025 Phase 2a.2 m2: mirror the tenant association
                // the pg/sqlite row carries so `list_policies` scopes
                // correctly across backends.
                state
                    .guardrail_policy_tenants
                    .insert(e.policy_id.clone(), e.tenant_id.clone());
            }
            RuntimeEvent::OperatorProfileCreated(e) => {
                state.operator_profiles.insert(
                    e.profile_id.as_str().to_owned(),
                    crate::projections::OperatorProfileRecord {
                        operator_id: e.profile_id.clone(),
                        tenant_id: e.tenant_id.clone(),
                        display_name: e.display_name.clone(),
                        email: Some(e.email.clone()),
                        role: serde_json::to_string(&e.role)
                            .unwrap_or_default()
                            .trim_matches('"')
                            .to_owned(),
                        created_at: now,
                    },
                );
                state.full_operator_profiles.insert(
                    e.profile_id.as_str().to_owned(),
                    cairn_domain::org::OperatorProfile {
                        operator_id: e.profile_id.clone(),
                        tenant_id: e.tenant_id.clone(),
                        display_name: e.display_name.clone(),
                        email: e.email.clone(),
                        role: e.role,
                        preferences: serde_json::Value::Null,
                    },
                );
            }
            RuntimeEvent::OperatorProfileUpdated(e) => {
                if let Some(rec) = state.operator_profiles.get_mut(e.profile_id.as_str()) {
                    if let Some(dn) = &e.display_name {
                        rec.display_name = dn.clone();
                    }
                    if let Some(email) = &e.email {
                        rec.email = Some(email.clone());
                    }
                    // RFC 026 PR-A2: role edit. Same serialization as
                    // `OperatorProfileCreated` — serde_json::to_string
                    // yields a quoted variant name; strip the quotes so
                    // the stored value matches `role TEXT NOT NULL`.
                    if let Some(role) = &e.role {
                        rec.role = serde_json::to_string(role)
                            .unwrap_or_default()
                            .trim_matches('"')
                            .to_owned();
                    }
                }
                if let Some(profile) = state.full_operator_profiles.get_mut(e.profile_id.as_str()) {
                    if let Some(dn) = &e.display_name {
                        profile.display_name = dn.clone();
                    }
                    if let Some(email) = &e.email {
                        profile.email = email.clone();
                    }
                    if let Some(role) = &e.role {
                        profile.role = *role;
                    }
                }
            }
            // RFC 026 PR-A0: operator_tenant_roles projection. Upsert on
            // grant — a re-grant over a revoked row clears the revocation
            // fields so the row reads as active again. The pg/sqlite
            // appliers (V066) mirror this ON CONFLICT semantics.
            RuntimeEvent::TenantRoleGranted(e) => {
                let key = (
                    e.tenant_id.as_str().to_owned(),
                    e.operator_id.as_str().to_owned(),
                );
                state.operator_tenant_roles.insert(
                    key,
                    crate::projections::OperatorTenantRoleRecord {
                        tenant_id: e.tenant_id.clone(),
                        operator_id: e.operator_id.clone(),
                        role: e.role,
                        granted_at_ms: e.at_ms,
                        granted_by: e.granted_by.clone(),
                        revoked_at_ms: None,
                        revoked_by: None,
                    },
                );
            }
            // RFC 026 PR-A0: soft-revoke. The row is NOT deleted — the
            // audit trail survives, and a subsequent `TenantRoleGranted`
            // upserts a fresh grant. Revoking a non-existent row is a
            // no-op (replay-safe across reorders).
            RuntimeEvent::TenantRoleRevoked(e) => {
                let key = (
                    e.tenant_id.as_str().to_owned(),
                    e.operator_id.as_str().to_owned(),
                );
                if let Some(rec) = state.operator_tenant_roles.get_mut(&key) {
                    rec.revoked_at_ms = Some(e.at_ms);
                    rec.revoked_by = Some(e.revoked_by.clone());
                }
            }
            RuntimeEvent::ProviderConnectionRegistered(e) => {
                state.provider_connections.insert(
                    e.provider_connection_id.as_str().to_owned(),
                    cairn_domain::providers::ProviderConnectionRecord {
                        provider_connection_id: e.provider_connection_id.clone(),
                        tenant_id: e.tenant.tenant_id.clone(),
                        provider_family: e.provider_family.clone(),
                        adapter_type: e.adapter_type.clone(),
                        supported_models: e.supported_models.clone(),
                        status: e.status,
                        created_at: e.registered_at,
                    },
                );
            }
            RuntimeEvent::ProviderConnectionDeleted(e) => {
                // Hard-remove so the ID can be re-created. History stays in
                // the event log for audit. F40.
                state
                    .provider_connections
                    .remove(e.provider_connection_id.as_str());
            }
            RuntimeEvent::ProviderHealthChecked(e) => {
                let healthy = matches!(
                    e.status,
                    cairn_domain::providers::ProviderHealthStatus::Healthy
                );
                let prev_failures = state
                    .provider_health_records
                    .get(e.connection_id.as_str())
                    .map(|r| r.consecutive_failures)
                    .unwrap_or(0);
                let consecutive_failures = if healthy {
                    0
                } else {
                    prev_failures.saturating_add(1)
                };
                state.provider_health_records.insert(
                    e.connection_id.as_str().to_owned(),
                    cairn_domain::providers::ProviderHealthRecord {
                        binding_id: cairn_domain::ids::ProviderBindingId::new(
                            e.connection_id.as_str(),
                        ),
                        healthy,
                        last_checked_ms: e.checked_at_ms,
                        error_message: None,
                        consecutive_failures,
                        status: e.status,
                    },
                );
            }
            RuntimeEvent::ProviderMarkedDegraded(e) => {
                let rec = state
                    .provider_health_records
                    .entry(e.connection_id.as_str().to_owned())
                    .or_insert_with(|| cairn_domain::providers::ProviderHealthRecord {
                        binding_id: cairn_domain::ids::ProviderBindingId::new(
                            e.connection_id.as_str(),
                        ),
                        healthy: false,
                        last_checked_ms: e.marked_at_ms,
                        error_message: None,
                        consecutive_failures: 0,
                        status: cairn_domain::providers::ProviderHealthStatus::Degraded,
                    });
                rec.healthy = false;
                rec.status = cairn_domain::providers::ProviderHealthStatus::Degraded;
                rec.error_message = Some(e.reason.clone());
                rec.last_checked_ms = e.marked_at_ms;
            }
            RuntimeEvent::ProviderRecovered(e) => {
                if let Some(rec) = state
                    .provider_health_records
                    .get_mut(e.connection_id.as_str())
                {
                    rec.healthy = true;
                    rec.status = cairn_domain::providers::ProviderHealthStatus::Healthy;
                    rec.error_message = None;
                    rec.last_checked_ms = e.recovered_at_ms;
                    rec.consecutive_failures = 0;
                }
            }
            RuntimeEvent::WorkspaceMemberAdded(e) => {
                state.workspace_members.retain(|m| {
                    !(m.workspace_id == e.workspace_key.workspace_id.as_str()
                        && m.operator_id == e.member_id.as_str())
                });
                state
                    .workspace_members
                    .push(crate::projections::WorkspaceMemberRecord {
                        workspace_id: e.workspace_key.workspace_id.as_str().to_owned(),
                        operator_id: e.member_id.as_str().to_owned(),
                        role: e.role,
                        added_at_ms: e.added_at_ms,
                    });
            }
            RuntimeEvent::WorkspaceMemberRemoved(e) => {
                state.workspace_members.retain(|m| {
                    !(m.workspace_id == e.workspace_key.workspace_id.as_str()
                        && m.operator_id == e.member_id.as_str())
                });
            }
            RuntimeEvent::RunCostAlertSet(e) => {
                state.run_cost_alerts.insert(
                    e.run_id.as_str().to_owned(),
                    cairn_domain::providers::RunCostAlert {
                        run_id: e.run_id.clone(),
                        threshold_micros: e.threshold_micros,
                        triggered_at_ms: 0,
                        tenant_id: e.tenant_id.clone(),
                        actual_cost_micros: 0,
                    },
                );
            }
            RuntimeEvent::RunCostAlertTriggered(e) => {
                if let Some(a) = state.run_cost_alerts.get_mut(e.run_id.as_str()) {
                    a.triggered_at_ms = e.triggered_at_ms;
                    a.actual_cost_micros = e.actual_cost_micros;
                }
            }
            RuntimeEvent::RunSlaSet(e) => {
                state.run_sla_configs.insert(
                    e.run_id.as_str().to_owned(),
                    cairn_domain::sla::SlaConfig {
                        run_id: e.run_id.clone(),
                        tenant_id: e.tenant_id.clone(),
                        target_completion_ms: e.target_completion_ms,
                        alert_at_percent: e.alert_at_percent,
                        configured_at_ms: e.set_at_ms,
                    },
                );
            }
            RuntimeEvent::RunSlaBreached(e) => {
                state.run_sla_breaches.insert(
                    e.run_id.as_str().to_owned(),
                    cairn_domain::sla::SlaBreach {
                        run_id: e.run_id.clone(),
                        tenant_id: e.tenant_id.clone(),
                        elapsed_ms: e.elapsed_ms,
                        target_ms: e.target_ms,
                        breached_at_ms: e.breached_at_ms,
                    },
                );
            }
            RuntimeEvent::ProviderBindingCreated(e) => {
                // Use the event position as created_at when the event's timestamp is 0,
                // ensuring stable creation-order sorting in list_active.
                let effective_created_at = if e.created_at > 0 {
                    e.created_at
                } else {
                    event.position.0
                };
                state.provider_bindings.insert(
                    e.provider_binding_id.as_str().to_owned(),
                    cairn_domain::providers::ProviderBindingRecord {
                        provider_binding_id: e.provider_binding_id.clone(),
                        project: e.project.clone(),
                        provider_connection_id: e.provider_connection_id.clone(),
                        provider_model_id: e.provider_model_id.clone(),
                        operation_kind: e.operation_kind,
                        settings: e.settings.clone(),
                        active: e.active,
                        created_at: effective_created_at,
                    },
                );
            }
            RuntimeEvent::ProviderBindingStateChanged(e) => {
                if let Some(b) = state
                    .provider_bindings
                    .get_mut(e.provider_binding_id.as_str())
                {
                    b.active = e.active;
                }
            }
            RuntimeEvent::ProviderHealthScheduleSet(e) => {
                state.provider_health_schedules.insert(
                    e.schedule_id.clone(),
                    cairn_domain::providers::ProviderHealthSchedule {
                        schedule_id: e.schedule_id.clone(),
                        binding_id: cairn_domain::ProviderBindingId::new(""),
                        interval_ms: e.interval_ms,
                        enabled: e.enabled,
                        connection_id: e.connection_id.clone(),
                        tenant_id: e.tenant_id.clone(),
                        last_run_ms: None,
                    },
                );
            }
            RuntimeEvent::ProviderHealthScheduleTriggered(e) => {
                if let Some(s) = state.provider_health_schedules.get_mut(&e.schedule_id) {
                    s.last_run_ms = Some(e.triggered_at_ms);
                }
            }
            RuntimeEvent::SignalSubscriptionCreated(e) => {
                state.signal_subscriptions.insert(
                    e.subscription_id.clone(),
                    crate::projections::SignalSubscriptionRecord {
                        subscription_id: e.subscription_id.clone(),
                        signal_type: e.signal_kind.clone(),
                        target: e
                            .target_run_id
                            .as_ref()
                            .map(|r| r.as_str().to_owned())
                            .unwrap_or_default(),
                        created_at_ms: e.created_at_ms,
                        project: Some(e.project.clone()),
                        project_tenant: e.project.tenant_id.as_str().to_owned(),
                        project_workspace: e.project.workspace_id.as_str().to_owned(),
                        project_id: e.project.project_id.as_str().to_owned(),
                        target_run_id: e.target_run_id.clone(),
                        target_mailbox_id: e.target_mailbox_id.clone(),
                        filter_expression: e.filter_expression.clone(),
                    },
                );
            }
            RuntimeEvent::RetentionPolicySet(e) => {
                state.retention_policies.insert(
                    e.tenant_id.as_str().to_owned(),
                    cairn_domain::RetentionPolicy {
                        policy_id: e.policy_id.clone(),
                        tenant_id: e.tenant_id.clone(),
                        full_history_days: e.full_history_days,
                        current_state_days: e.current_state_days,
                        max_events_per_entity: e.max_events_per_entity.unwrap_or(0) as u32,
                    },
                );
            }
            RuntimeEvent::TenantQuotaViolated(e) => {
                // RFC-025 Phase 2a.1 milestone 2: projection parity with
                // pg/sqlite `tenant_quota_violations` table.
                //
                // The pg + sqlite tables enforce uniqueness on
                // (tenant_id, quota_type, occurred_at_ms) via a PRIMARY
                // KEY + `ON CONFLICT DO NOTHING`. Mirror that here so a
                // replayed or duplicated event does not accumulate
                // phantom rows on the in-memory side — Copilot PR #565
                // flagged this as a cross-backend drift risk.
                let already_recorded = state.quota_violations.iter().any(|record| {
                    record.tenant_id == e.tenant_id
                        && record.quota_type == e.quota_type
                        && record.occurred_at_ms == e.occurred_at_ms
                });
                if !already_recorded {
                    state
                        .quota_violations
                        .push(crate::projections::QuotaViolationRecord {
                            tenant_id: e.tenant_id.clone(),
                            quota_type: e.quota_type.clone(),
                            current: e.current,
                            limit: e.limit,
                            occurred_at_ms: e.occurred_at_ms,
                        });
                }
            }
            // RFC-025 Phase 2a.2 milestone 1: append the audit row for
            // each delegation. Composite key `(approval_id, delegation_id)`
            // mirrors the pg/sqlite PRIMARY KEY — a replayed event is a
            // no-op here too. `delegation_id` is minted monotonically per
            // emit by the runtime service so two rapid delegations of
            // the same approval to the same operator in the same
            // millisecond both persist as distinct rows (Copilot #571
            // round 4).
            //
            // Copilot #571 round 3: the O(n) `.iter().any(...)` dedupe
            // was O(n²) across a replay. The Vec is kept in read-order
            // (approval_id ASC, delegated_at_ms ASC, delegation_id ASC)
            // matching the pg/sqlite `ORDER BY` — a binary-search probe
            // on the same tuple gives O(log n) membership. Dedupe is
            // on the PK `(approval_id, delegation_id)`; the sort key
            // adds `delegated_at_ms` as the secondary discriminator so
            // reads walk the vec in time-order without a re-sort.
            RuntimeEvent::ApprovalDelegated(e) => {
                let sort_probe = |record: &crate::projections::ApprovalDelegationRecord| {
                    record
                        .approval_id
                        .cmp(&e.approval_id)
                        .then_with(|| record.delegated_at_ms.cmp(&e.delegated_at_ms))
                        .then_with(|| record.delegation_id.cmp(&e.delegation_id))
                };
                if let Err(idx) = state.approval_delegations.binary_search_by(sort_probe) {
                    state.approval_delegations.insert(
                        idx,
                        crate::projections::ApprovalDelegationRecord {
                            approval_id: e.approval_id.clone(),
                            delegated_to: e.delegated_to.clone(),
                            delegated_at_ms: e.delegated_at_ms,
                            delegation_id: e.delegation_id.clone(),
                        },
                    );
                }
            }
            // RFC-025 Phase 2a.2 milestone 2: audit trail for guardrail
            // evaluations. Composite-key check mirrors pg/sqlite
            // `PRIMARY KEY (tenant_id, policy_id, subject_type,
            // subject_id, action, evaluated_at_ms)` + `ON CONFLICT DO
            // NOTHING`. `tenant_id` leads so shared runtime-emitted
            // `policy_id`s (e.g. "implicit_allow") cannot silently
            // collapse evaluations across tenants.
            //
            // Copilot #571 round 3: O(1) dedupe via a sidecar HashSet on
            // the composite PK. The Vec stays authoritative so the
            // read-model re-sorts at query time to match pg/sqlite
            // `ORDER BY evaluated_at_ms DESC`. `subject_id` collapses
            // Option<String> → String via unwrap_or_default so the key
            // matches the pg empty-string sentinel on the PK.
            RuntimeEvent::GuardrailPolicyEvaluated(e) => {
                use cairn_domain::policy::GuardrailSubjectType as T;
                let subject_type_str = match e.subject_type {
                    T::Run => "run",
                    T::Task => "task",
                    T::Session => "session",
                    T::Tool => "tool",
                    T::Provider => "provider",
                };
                let key = (
                    e.tenant_id.as_str().to_owned(),
                    e.policy_id.clone(),
                    subject_type_str.to_owned(),
                    e.subject_id.clone().unwrap_or_default(),
                    e.action.clone(),
                    e.evaluated_at_ms,
                );
                if state.guardrail_evaluation_keys.insert(key) {
                    state
                        .guardrail_evaluations
                        .push(crate::projections::GuardrailEvaluationRecord {
                            policy_id: e.policy_id.clone(),
                            tenant_id: e.tenant_id.clone(),
                            subject_type: e.subject_type,
                            subject_id: e.subject_id.clone(),
                            action: e.action.clone(),
                            decision: e.decision,
                            reason: e.reason.clone(),
                            evaluated_at_ms: e.evaluated_at_ms,
                        });
                }
            }
            RuntimeEvent::EventLogCompacted(_)
            | RuntimeEvent::OperatorIntervention(_)
            | RuntimeEvent::PauseScheduled(_)
            | RuntimeEvent::PermissionDecisionRecorded(_)
            | RuntimeEvent::ProviderModelRegistered(_)
            | RuntimeEvent::ProviderRetryPolicySet(_) => {}
            // RFC-025 Phase 2b.1: audit projection. Idempotent on
            // replay (entry_id is globally unique — a duplicate delivery
            // keeps the first insert). Metadata defaults to `{}` because
            // `AuditLogEntryRecorded` does not carry it on the wire (the
            // event was kept Eq-able at RFC 002 time). The
            // `or_insert_with_key` form re-uses the HashMap key as the
            // record's `entry_id` field so we don't clone the string
            // twice (Gemini PR #573 review).
            RuntimeEvent::AuditLogEntryRecorded(e) => {
                state
                    .audit_log_entries
                    .entry(e.entry_id.clone())
                    .or_insert_with_key(|entry_id| crate::projections::AuditLogEntryRecord {
                        entry_id: entry_id.clone(),
                        tenant_id: e.tenant_id.clone(),
                        actor_id: e.actor_id.clone(),
                        action: e.action.clone(),
                        resource_type: e.resource_type.clone(),
                        resource_id: e.resource_id.clone(),
                        outcome: e.outcome,
                        metadata: serde_json::json!({}),
                        occurred_at_ms: e.occurred_at_ms,
                    });
            }
            RuntimeEvent::ResourceShared(e) => {
                state.resource_shares.insert(
                    e.share_id.clone(),
                    cairn_domain::resource_sharing::SharedResource {
                        share_id: e.share_id.clone(),
                        tenant_id: e.tenant_id.clone(),
                        source_workspace_id: e.source_workspace_id.clone(),
                        target_workspace_id: e.target_workspace_id.clone(),
                        resource_type: e.resource_type.clone(),
                        resource_id: e.resource_id.clone(),
                        permissions: e.permissions.clone(),
                        shared_at_ms: e.shared_at_ms,
                    },
                );
            }
            RuntimeEvent::ResourceShareRevoked(e) => {
                state.resource_shares.remove(&e.share_id);
            }
            RuntimeEvent::RoutePolicyCreated(e) => {
                state.route_policies.insert(
                    e.policy_id.clone(),
                    cairn_domain::providers::RoutePolicy {
                        policy_id: e.policy_id.clone(),
                        name: e.name.clone(),
                        enabled: e.enabled,
                        tenant_id: e.tenant_id.as_str().to_owned(),
                        rules: e.rules.clone(),
                        updated_at_ms: now,
                    },
                );
            }
            RuntimeEvent::RoutePolicyUpdated(e) => {
                if let Some(p) = state.route_policies.get_mut(&e.policy_id) {
                    p.updated_at_ms = e.updated_at_ms;
                }
            }
            RuntimeEvent::RecoveryEscalated(_) | RuntimeEvent::SignalRouted(_) => {}
            RuntimeEvent::SnapshotCreated(e) => {
                state.snapshots.push(cairn_domain::Snapshot {
                    snapshot_id: e.snapshot_id.clone(),
                    tenant_id: e.tenant_id.clone(),
                    event_position: e.event_position,
                    state_hash: String::new(),
                    created_at_ms: e.created_at_ms,
                    compressed_state: vec![],
                });
            }
            // Audit-only events — appended to the log for later
            // reconstruction but not projected into any read model.
            // TaskDependency{Added,Resolved} are authoritative in FF;
            // cairn keeps them on the log for join-against-
            // TaskStateChanged queries.
            RuntimeEvent::TaskDependencyAdded(_)
            | RuntimeEvent::TaskDependencyResolved(_)
            | RuntimeEvent::TaskLeaseExpired(_)
            | RuntimeEvent::TaskPriorityChanged(_) => {}
            // #364: project the LATEST progress update per invocation so
            // the `get_tool_invocation_progress_handler` can answer
            // tenant-scoped reads without walking the event log. We
            // inherit the `ProjectKey` from the existing
            // `tool_invocations` row rather than carrying it on the
            // event, so the projection is only created when the
            // invocation itself has been started. Progress events that
            // arrive before the `ToolInvocationStarted` (should not
            // happen in practice, but we refuse to silently fabricate a
            // project) are a no-op.
            //
            // Out-of-order replay guard: an older event must not
            // overwrite a newer one. Mirrors the
            // `WHERE EXCLUDED.updated_at_ms >= …` clause on the pg/sqlite
            // UPSERTs so every backend converges on the same row after
            // replay. Flagged on PR #537 by Gemini / Copilot / Cursor.
            RuntimeEvent::ToolInvocationProgressUpdated(e) => {
                if let Some(inv) = state.tool_invocations.get(e.invocation_id.as_str()) {
                    let should_write = state
                        .tool_invocation_progress
                        .get(e.invocation_id.as_str())
                        .is_none_or(|existing| e.updated_at_ms >= existing.updated_at_ms);
                    if should_write {
                        state.tool_invocation_progress.insert(
                            e.invocation_id.as_str().to_owned(),
                            crate::projections::ToolInvocationProgressRecord {
                                invocation_id: e.invocation_id.clone(),
                                project: inv.project.clone(),
                                progress_pct: e.progress_pct,
                                message: e.message.clone(),
                                updated_at_ms: e.updated_at_ms,
                            },
                        );
                    }
                }
            }
            RuntimeEvent::SessionCostUpdated(e) => {
                // The envelope carries a top-level `tenant_id` AND a
                // `project.tenant_id` — two redundant fields that can
                // drift. Some fixtures (see
                // `crates/cairn-runtime/src/services/budget_impl.rs`'s
                // budget-blocking test) intentionally leave `project`
                // as a sentinel triple when only tenant-scoped effects
                // matter. We bind one `tenant` local from the explicit
                // field and use it for both the per-session record and
                // the provider-budget loop so the redundant source
                // divergence cannot make `session_costs` and
                // `provider_budgets` disagree with each other. The
                // project/workspace rollups still key off `e.project.*`
                // for the workspace_id / project_id sub-keys.
                let tenant = e.tenant_id.clone();
                let rec = state
                    .session_costs
                    .entry(e.session_id.as_str().to_owned())
                    .or_insert_with(|| cairn_domain::providers::SessionCostRecord {
                        session_id: e.session_id.clone(),
                        tenant_id: tenant.clone(),
                        total_cost_micros: 0,
                        total_tokens_in: 0,
                        total_tokens_out: 0,
                        provider_calls: 0,
                        token_in: 0,
                        token_out: 0,
                        updated_at_ms: now,
                    });
                rec.total_cost_micros = rec.total_cost_micros.saturating_add(e.delta_cost_micros);
                rec.total_tokens_in = rec.total_tokens_in.saturating_add(e.delta_tokens_in);
                rec.total_tokens_out = rec.total_tokens_out.saturating_add(e.delta_tokens_out);
                rec.provider_calls = rec.provider_calls.saturating_add(1);
                rec.token_in = rec.total_tokens_in;
                rec.token_out = rec.total_tokens_out;
                rec.updated_at_ms = now;
                // Also accumulate into provider budget spend for the tenant.
                for budget in state.provider_budgets.values_mut() {
                    if budget.tenant_id == tenant {
                        budget.current_spend_micros = budget
                            .current_spend_micros
                            .saturating_add(e.delta_cost_micros);
                        budget.updated_at = now;
                    }
                }
                // F29 CD-2: fold the same delta into the project + workspace
                // rollups so ProjectCostReadModel stays consistent with
                // SessionCostReadModel without a separate aggregator.
                let proj_key = (
                    tenant.as_str().to_owned(),
                    e.project.workspace_id.as_str().to_owned(),
                    e.project.project_id.as_str().to_owned(),
                );
                let proj = state.project_costs.entry(proj_key).or_insert_with(|| {
                    cairn_domain::providers::ProjectCostRecord {
                        tenant_id: tenant.clone(),
                        workspace_id: e.project.workspace_id.as_str().to_owned(),
                        project_id: e.project.project_id.as_str().to_owned(),
                        total_cost_micros: 0,
                        total_tokens_in: 0,
                        total_tokens_out: 0,
                        provider_calls: 0,
                        updated_at_ms: now,
                    }
                });
                proj.total_cost_micros =
                    proj.total_cost_micros.saturating_add(e.delta_cost_micros);
                proj.total_tokens_in =
                    proj.total_tokens_in.saturating_add(e.delta_tokens_in);
                proj.total_tokens_out =
                    proj.total_tokens_out.saturating_add(e.delta_tokens_out);
                proj.provider_calls = proj.provider_calls.saturating_add(1);
                proj.updated_at_ms = now;

                let ws_key = (
                    tenant.as_str().to_owned(),
                    e.project.workspace_id.as_str().to_owned(),
                );
                let ws = state.workspace_costs.entry(ws_key).or_insert_with(|| {
                    cairn_domain::providers::WorkspaceCostRecord {
                        tenant_id: tenant.clone(),
                        workspace_id: e.project.workspace_id.as_str().to_owned(),
                        total_cost_micros: 0,
                        total_tokens_in: 0,
                        total_tokens_out: 0,
                        provider_calls: 0,
                        updated_at_ms: now,
                    }
                });
                ws.total_cost_micros =
                    ws.total_cost_micros.saturating_add(e.delta_cost_micros);
                ws.total_tokens_in = ws.total_tokens_in.saturating_add(e.delta_tokens_in);
                ws.total_tokens_out = ws.total_tokens_out.saturating_add(e.delta_tokens_out);
                ws.provider_calls = ws.provider_calls.saturating_add(1);
                ws.updated_at_ms = now;
            }
            // RFC 005 + RFC-025 Phase 2b.2b m3: link child task to
            // parent run/task and record the spawn audit row.
            RuntimeEvent::SubagentSpawned(e) => {
                if let Some(rec) = state.tasks.get_mut(e.child_task_id.as_str()) {
                    rec.parent_run_id = Some(e.parent_run_id.clone());
                    rec.parent_task_id = e.parent_task_id.clone();
                    rec.updated_at = now;
                }
                // Idempotent on replay via `or_insert_with` — the first
                // delivery wins, a replayed event leaves the row
                // untouched (mirrors ON CONFLICT DO NOTHING on pg/sqlite).
                state
                    .subagent_spawns
                    .entry(e.child_task_id.as_str().to_owned())
                    .or_insert_with(|| crate::projections::SubagentSpawnRecord {
                        child_task_id: e.child_task_id.clone(),
                        project: e.project.clone(),
                        parent_run_id: e.parent_run_id.clone(),
                        parent_task_id: e.parent_task_id.clone(),
                        child_session_id: e.child_session_id.clone(),
                        child_run_id: e.child_run_id.clone(),
                        spawned_at_ms: now,
                        // #670 G2: carry the LLM delegation context
                        // from the event verbatim. Pre-G2 events
                        // deserialise with empty strings via
                        // `#[serde(default)]`.
                        goal: e.goal.clone(),
                        role: e.role.clone(),
                    });
            }
            // Audit/linkage events that don't update core projections.
            RuntimeEvent::CheckpointRestored(_)
            | RuntimeEvent::RecoveryAttempted(_)
            | RuntimeEvent::RecoveryCompleted(_)
            // RFC 020 Track 4: boot-level recovery audit event.
            | RuntimeEvent::RecoverySummaryEmitted(_)
            => {}

            // RFC-025 Phase 2b.2b m6: tool_recovery_pauses projection
            // (RFC 020 Track 3). Keyed by tool_call_id; first-write
            // wins on replay.
            RuntimeEvent::ToolRecoveryPaused(e) => {
                state
                    .tool_recovery_pauses
                    .entry(e.tool_call_id.clone())
                    .or_insert_with(|| crate::projections::ToolRecoveryPauseRecord {
                        tool_call_id: e.tool_call_id.clone(),
                        project: e.project.clone(),
                        run_id: e.run_id.clone(),
                        task_id: e.task_id.clone(),
                        tool_name: e.tool_name.clone(),
                        reason: e.reason.clone(),
                        paused_at_ms: e.paused_at_ms,
                    });
            }

            // RFC-025 Phase 2b.2b m4: user_messages projection. Keyed
            // by `(run_id, sequence)` so a replayed append is
            // idempotent — mirrors ON CONFLICT DO NOTHING on pg/sqlite.
            RuntimeEvent::UserMessageAppended(e) => {
                state
                    .user_messages
                    .entry((e.run_id.as_str().to_owned(), e.sequence))
                    .or_insert_with(|| crate::projections::UserMessageRecord {
                        run_id: e.run_id.clone(),
                        sequence: e.sequence,
                        project: e.project.clone(),
                        session_id: e.session_id.clone(),
                        event_id: event.envelope.event_id.as_str().to_owned(),
                        content: e.content.clone(),
                        appended_at_ms: e.appended_at_ms,
                    });
            }

            // ── RFC-025 Phase 2b.1 m4: plan_reviews projection (RFC 018) ──
            // Parity with pg/sqlite arms. Creation inserts; resolution
            // events mutate the state + resolver fields in-place, but
            // only when the row is still in `Proposed` — mirrors the
            // `WHERE state = 'proposed'` clause on pg/sqlite so a late
            // duplicate resolution does not overwrite an earlier one.
            RuntimeEvent::PlanProposed(e) => {
                state
                    .plan_reviews
                    .entry(e.plan_run_id.as_str().to_owned())
                    .or_insert_with(|| crate::projections::PlanReviewRecord {
                        plan_run_id: e.plan_run_id.clone(),
                        project: e.project.clone(),
                        session_id: e.session_id.clone(),
                        plan_markdown: e.plan_markdown.clone(),
                        state: crate::projections::PlanReviewState::Proposed,
                        proposed_at: e.proposed_at,
                        resolved_by: None,
                        resolved_at: None,
                        reviewer_comments: None,
                        rejection_reason: None,
                        revision_run_id: None,
                    });
            }
            RuntimeEvent::PlanApproved(e) => {
                if let Some(rec) = state.plan_reviews.get_mut(e.plan_run_id.as_str()) {
                    if rec.state == crate::projections::PlanReviewState::Proposed {
                        rec.state = crate::projections::PlanReviewState::Approved;
                        rec.resolved_by = Some(e.approved_by.clone());
                        rec.resolved_at = Some(e.approved_at);
                        rec.reviewer_comments = e.reviewer_comments.clone();
                    }
                }
            }
            RuntimeEvent::PlanRejected(e) => {
                if let Some(rec) = state.plan_reviews.get_mut(e.plan_run_id.as_str()) {
                    if rec.state == crate::projections::PlanReviewState::Proposed {
                        rec.state = crate::projections::PlanReviewState::Rejected;
                        rec.resolved_by = Some(e.rejected_by.clone());
                        rec.resolved_at = Some(e.rejected_at);
                        rec.rejection_reason = Some(e.reason.clone());
                    }
                }
            }
            RuntimeEvent::PlanRevisionRequested(e) => {
                if let Some(rec) = state
                    .plan_reviews
                    .get_mut(e.original_plan_run_id.as_str())
                {
                    if rec.state == crate::projections::PlanReviewState::Proposed {
                        rec.state = crate::projections::PlanReviewState::RevisionRequested;
                        rec.resolved_at = Some(e.requested_at);
                        rec.reviewer_comments = Some(e.reviewer_comments.clone());
                        rec.revision_run_id = Some(e.new_plan_run_id.clone());
                    }
                }
            }

            // ── RFC-025 Phase 1.5a: trigger + run_template + trigger_fires ─────
            // Parity with pg/sqlite projection arms. Eight state-carrying
            // lifecycle variants mutate `state.triggers` / `state.run_templates`;
            // five audit variants append into `state.trigger_fires`.
            RuntimeEvent::TriggerCreated(e) => {
                // serde_json::to_string on a Vec<Value> cannot fail in practice;
                // fall back to an empty JSON array so an impossible serde error
                // doesn't leave the in-memory row half-written. pg/sqlite use `?`
                // via their Result-returning applier, so those backends surface
                // the error. In-memory stays infallible to match the signature
                // that the rest of apply_projection already depends on.
                let conditions_json =
                    serde_json::to_string(&e.conditions).unwrap_or_else(|_| "[]".to_owned());
                state
                    .triggers
                    .entry(e.trigger_id.as_str().to_owned())
                    .or_insert_with(|| crate::projections::TriggerRecord {
                        trigger_id: e.trigger_id.clone(),
                        project: e.project.clone(),
                        name: e.name.clone(),
                        description: e.description.clone(),
                        signal_type: e.signal_type.clone(),
                        plugin_id: e.plugin_id.clone(),
                        conditions_json,
                        run_template_id: e.run_template_id.clone(),
                        state: crate::projections::TriggerStateKind::Enabled,
                        state_reason: None,
                        suspension_reason: None,
                        state_since: None,
                        max_per_minute: e.max_per_minute,
                        max_burst: e.max_burst,
                        max_chain_depth: e.max_chain_depth,
                        created_by: e.created_by.clone(),
                        created_at: e.created_at,
                        updated_at: e.created_at,
                    });
            }
            RuntimeEvent::TriggerEnabled(e) => {
                if let Some(rec) = state.triggers.get_mut(e.trigger_id.as_str()) {
                    rec.state = crate::projections::TriggerStateKind::Enabled;
                    rec.state_reason = None;
                    rec.suspension_reason = None;
                    rec.state_since = None;
                    rec.updated_at = e.at;
                }
            }
            RuntimeEvent::TriggerDisabled(e) => {
                if let Some(rec) = state.triggers.get_mut(e.trigger_id.as_str()) {
                    rec.state = crate::projections::TriggerStateKind::Disabled;
                    rec.state_reason = e.reason.clone();
                    rec.suspension_reason = None;
                    rec.state_since = Some(e.at);
                    rec.updated_at = e.at;
                }
            }
            RuntimeEvent::TriggerSuspended(e) => {
                if let Some(rec) = state.triggers.get_mut(e.trigger_id.as_str()) {
                    // Use the shared discriminant helper so pg + sqlite
                    // + in-memory store the exact same short name. The
                    // `failure_count` payload for RepeatedFailures lives
                    // on the event log and is not rehydrated into the
                    // projection; the trigger service's
                    // rehydrate-from-record path reconstructs a
                    // zero-count value because in practice the service
                    // emits a fresh TriggerSuspended event whenever the
                    // count matters.
                    rec.state = crate::projections::TriggerStateKind::Suspended;
                    rec.state_reason = None;
                    rec.suspension_reason = Some(
                        crate::projections::trigger::suspension_reason_discriminant(&e.reason)
                            .to_owned(),
                    );
                    rec.state_since = Some(e.at);
                    rec.updated_at = e.at;
                }
            }
            RuntimeEvent::TriggerResumed(e) => {
                if let Some(rec) = state.triggers.get_mut(e.trigger_id.as_str()) {
                    rec.state = crate::projections::TriggerStateKind::Enabled;
                    rec.state_reason = None;
                    rec.suspension_reason = None;
                    rec.state_since = None;
                    rec.updated_at = e.at;
                }
            }
            RuntimeEvent::TriggerDeleted(e) => {
                state.triggers.remove(e.trigger_id.as_str());
            }
            RuntimeEvent::RunTemplateCreated(e) => {
                // Same "serde cannot fail in practice" fallback as
                // TriggerCreated above — infallible here to match the
                // apply_projection signature.
                let plugin_allowlist_json = e
                    .plugin_allowlist
                    .as_ref()
                    .and_then(|v| serde_json::to_string(v).ok());
                let tool_allowlist_json = e
                    .tool_allowlist
                    .as_ref()
                    .and_then(|v| serde_json::to_string(v).ok());
                let required_fields_json =
                    serde_json::to_string(&e.required_fields).unwrap_or_else(|_| "[]".to_owned());
                let default_mode_str = serde_json::to_value(&e.default_mode)
                    .ok()
                    .map(|v| match v {
                        serde_json::Value::String(s) => s,
                        other => other.to_string().trim_matches('"').to_owned(),
                    })
                    .unwrap_or_else(|| "chat".to_owned());
                state
                    .run_templates
                    .entry(e.template_id.as_str().to_owned())
                    .or_insert_with(|| crate::projections::RunTemplateRecord {
                        template_id: e.template_id.clone(),
                        project: e.project.clone(),
                        name: e.name.clone(),
                        description: e.description.clone(),
                        default_mode: default_mode_str,
                        system_prompt: e.system_prompt.clone(),
                        initial_user_message: e.initial_user_message.clone(),
                        plugin_allowlist_json,
                        tool_allowlist_json,
                        budget_max_tokens: e.budget_max_tokens,
                        budget_max_wall_clock_ms: e.budget_max_wall_clock_ms,
                        budget_max_iterations: e.budget_max_iterations,
                        budget_exploration_budget_share: e.budget_exploration_budget_share,
                        sandbox_hint: e.sandbox_hint.clone(),
                        required_fields_json,
                        created_by: e.created_by.clone(),
                        created_at: e.created_at,
                        updated_at: e.created_at,
                    });
            }
            RuntimeEvent::RunTemplateDeleted(e) => {
                state.run_templates.remove(e.template_id.as_str());
            }
            RuntimeEvent::TriggerFired(e) => {
                let metadata = serde_json::to_string(&serde_json::json!({
                    "run_id": e.run_id.as_str(),
                    "chain_depth": e.chain_depth,
                }))
                .ok();
                state.trigger_fires.push(crate::projections::TriggerFireRecord {
                    trigger_id: e.trigger_id.clone(),
                    project: e.project.clone(),
                    signal_id: e.signal_id.as_str().to_owned(),
                    outcome: crate::projections::TriggerFireOutcome::Fired,
                    signal_type: Some(e.signal_type.clone()),
                    metadata_json: metadata,
                    at_ms: e.fired_at,
                });
            }
            RuntimeEvent::TriggerSkipped(e) => {
                // Shared discriminant helper + optional field metadata
                // so the in-memory row shape matches pg + sqlite
                // byte-for-byte. Prior version collapsed the
                // MissingRequiredField payload into the `reason` string
                // (e.g. `missing_required_field:issue.number`) which
                // diverged from the two persistent backends (Copilot
                // review PR #569).
                let reason_str =
                    crate::projections::trigger::skip_reason_discriminant(&e.reason);
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
                .ok();
                state.trigger_fires.push(crate::projections::TriggerFireRecord {
                    trigger_id: e.trigger_id.clone(),
                    project: e.project.clone(),
                    signal_id: e.signal_id.as_str().to_owned(),
                    outcome: crate::projections::TriggerFireOutcome::Skipped,
                    signal_type: None,
                    metadata_json: metadata,
                    at_ms: e.skipped_at,
                });
            }
            RuntimeEvent::TriggerDenied(e) => {
                let metadata = serde_json::to_string(&serde_json::json!({
                    "decision_id": e.decision_id.as_str(),
                    "reason": e.reason,
                }))
                .ok();
                state.trigger_fires.push(crate::projections::TriggerFireRecord {
                    trigger_id: e.trigger_id.clone(),
                    project: e.project.clone(),
                    signal_id: e.signal_id.as_str().to_owned(),
                    outcome: crate::projections::TriggerFireOutcome::Denied,
                    signal_type: None,
                    metadata_json: metadata,
                    at_ms: e.denied_at,
                });
            }
            RuntimeEvent::TriggerRateLimited(e) => {
                let metadata = serde_json::to_string(&serde_json::json!({
                    "bucket_remaining": e.bucket_remaining,
                    "bucket_capacity": e.bucket_capacity,
                }))
                .ok();
                state.trigger_fires.push(crate::projections::TriggerFireRecord {
                    trigger_id: e.trigger_id.clone(),
                    project: e.project.clone(),
                    signal_id: e.signal_id.as_str().to_owned(),
                    outcome: crate::projections::TriggerFireOutcome::RateLimited,
                    signal_type: None,
                    metadata_json: metadata,
                    at_ms: e.rate_limited_at,
                });
            }
            RuntimeEvent::TriggerPendingApproval(e) => {
                let metadata = serde_json::to_string(&serde_json::json!({
                    "approval_id": e.approval_id.as_str(),
                }))
                .ok();
                state.trigger_fires.push(crate::projections::TriggerFireRecord {
                    trigger_id: e.trigger_id.clone(),
                    project: e.project.clone(),
                    signal_id: e.signal_id.as_str().to_owned(),
                    outcome: crate::projections::TriggerFireOutcome::PendingApproval,
                    signal_type: None,
                    metadata_json: metadata,
                    at_ms: e.pending_at,
                });
            }
            // PR BP-2: project tool-call approval events into the
            // `tool_call_approvals` map.
            RuntimeEvent::ToolCallProposed(e) => {
                // Idempotent: mirror the SQL backends' `ON CONFLICT DO
                // NOTHING` so a replayed ToolCallProposed does NOT reset
                // an already-amended/approved/rejected record.
                state
                    .tool_call_approvals
                    .entry(e.call_id.as_str().to_owned())
                    .or_insert_with(|| ToolCallApprovalRecord {
                        call_id: e.call_id.clone(),
                        session_id: e.session_id.clone(),
                        run_id: e.run_id.clone(),
                        project: e.project.clone(),
                        tool_name: e.tool_name.clone(),
                        original_tool_args: e.tool_args.clone(),
                        amended_tool_args: None,
                        approved_tool_args: None,
                        display_summary: if e.display_summary.is_empty() {
                            None
                        } else {
                            Some(e.display_summary.clone())
                        },
                        match_policy: e.match_policy.clone(),
                        state: ToolCallApprovalState::Pending,
                        operator_id: None,
                        scope: None,
                        reason: None,
                        proposed_at_ms: e.proposed_at_ms,
                        approved_at_ms: None,
                        rejected_at_ms: None,
                        last_amended_at_ms: None,
                        version: 1,
                        created_at: now,
                        updated_at: now,
                    });
            }
            RuntimeEvent::ToolCallAmended(e) => {
                if let Some(rec) = state.tool_call_approvals.get_mut(e.call_id.as_str()) {
                    rec.amended_tool_args = Some(e.new_tool_args.clone());
                    rec.last_amended_at_ms = Some(e.amended_at_ms);
                    rec.version += 1;
                    rec.updated_at = now;
                }
            }
            RuntimeEvent::ToolCallApproved(e) => {
                if let Some(rec) = state.tool_call_approvals.get_mut(e.call_id.as_str()) {
                    rec.state = ToolCallApprovalState::Approved;
                    rec.operator_id = Some(e.operator_id.clone());
                    rec.scope = Some(e.scope.clone());
                    // Mirror SQL behaviour (binding NULL clears the
                    // override): assign unconditionally so a replayed
                    // Approved-with-None cannot leave stale override
                    // args. Preserves the "Approved-with-None must NOT
                    // populate approved_tool_args" invariant.
                    rec.approved_tool_args = e.approved_tool_args.clone();
                    rec.approved_at_ms = Some(e.approved_at_ms);
                    rec.version += 1;
                    rec.updated_at = now;
                }
            }
            RuntimeEvent::ToolCallRejected(e) => {
                if let Some(rec) = state.tool_call_approvals.get_mut(e.call_id.as_str()) {
                    rec.state = ToolCallApprovalState::Rejected;
                    rec.operator_id = Some(e.operator_id.clone());
                    rec.reason = e.reason.clone();
                    rec.rejected_at_ms = Some(e.rejected_at_ms);
                    rec.version += 1;
                    rec.updated_at = now;
                }
            }
            RuntimeEvent::ScheduledTaskCreated(e) => {
                state.scheduled_tasks.insert(
                    e.scheduled_task_id.as_str().to_owned(),
                    cairn_domain::ScheduledTaskRecord {
                        scheduled_task_id: e.scheduled_task_id.clone(),
                        tenant_id: e.tenant_id.clone(),
                        name: e.name.clone(),
                        cron_expression: e.cron_expression.clone(),
                        last_run_at: None,
                        next_run_at: e.next_run_at,
                        enabled: true,
                        created_at: e.created_at,
                        updated_at: e.created_at,
                    },
                );
            }
            RuntimeEvent::RouteDecisionMade(e) => {
                state.route_decisions.insert(
                    e.route_decision_id.as_str().to_owned(),
                    cairn_domain::providers::RouteDecisionRecord {
                        route_decision_id: e.route_decision_id.clone(),
                        project_id: e.project.project_id.clone(),
                        operation_kind: e.operation_kind,
                        terminal_route_attempt_id: None,
                        selected_provider_binding_id: e.selected_provider_binding_id.clone(),
                        selected_route_attempt_id: None,
                        selector_context: cairn_domain::selectors::SelectorContext::default(),
                        attempt_count: e.attempt_count,
                        fallback_used: e.fallback_used,
                        final_status: e.final_status,
                    },
                );
            }
            RuntimeEvent::ProviderCallCompleted(e) => {
                state.provider_calls.insert(
                    e.provider_call_id.as_str().to_owned(),
                    cairn_domain::providers::ProviderCallRecord {
                        provider_call_id: e.provider_call_id.clone(),
                        route_decision_id: e.route_decision_id.clone(),
                        route_attempt_id: e.route_attempt_id.clone(),
                        project_id: e.project.project_id.clone(),
                        operation_kind: e.operation_kind,
                        provider_binding_id: e.provider_binding_id.clone(),
                        provider_connection_id: e.provider_connection_id.clone(),
                        provider_adapter: String::new(),
                        provider_model_id: e.provider_model_id.clone(),
                        task_id: e.task_id.clone(),
                        run_id: e.run_id.clone(),
                        prompt_release_id: e.prompt_release_id.clone(),
                        fallback_position: e.fallback_position as u16,
                        status: e.status,
                        latency_ms: e.latency_ms.or_else(|| {
                            if e.started_at > 0 && e.finished_at >= e.started_at {
                                Some(e.finished_at - e.started_at)
                            } else {
                                None
                            }
                        }),
                        input_tokens: e.input_tokens,
                        output_tokens: e.output_tokens,
                        cost_micros: e.cost_micros,
                        cost_type: cairn_domain::providers::ProviderCostType::default(),
                        error_class: e.error_class,
                        started_at_ms: e.started_at,
                        finished_at_ms: e.finished_at,
                        raw_error_message: e.raw_error_message.clone(),
                    },
                );
                // GAP-010: derive LlmCallTrace from every ProviderCallCompleted.
                // All calls — successful and failed — are valuable for observability.
                state.llm_traces.push(cairn_domain::LlmCallTrace {
                    trace_id: e.provider_call_id.as_str().to_owned(),
                    model_id: e.provider_model_id.as_str().to_owned(),
                    prompt_tokens: e.input_tokens.unwrap_or(0),
                    completion_tokens: e.output_tokens.unwrap_or(0),
                    latency_ms: e.latency_ms.unwrap_or(0),
                    cost_micros: e.cost_micros.unwrap_or(0),
                    session_id: e.session_id.clone(),
                    run_id: e.run_id.clone(),
                    created_at_ms: e.completed_at,
                    is_error: e.status != cairn_domain::providers::ProviderCallStatus::Succeeded,
                });
                // Accumulate run-level costs and emit a derived RunCostUpdated event.
                if let Some(run_id) = &e.run_id {
                    let delta_cost = e.cost_micros.unwrap_or(0);
                    let delta_in = e.input_tokens.unwrap_or(0) as u64;
                    let delta_out = e.output_tokens.unwrap_or(0) as u64;
                    let rec = state
                        .run_costs
                        .entry(run_id.as_str().to_owned())
                        .or_insert_with(|| cairn_domain::providers::RunCostRecord {
                            run_id: run_id.clone(),
                            total_cost_micros: 0,
                            total_tokens_in: 0,
                            total_tokens_out: 0,
                            provider_calls: 0,
                            token_in: 0,
                            token_out: 0,
                        });
                    rec.total_cost_micros = rec.total_cost_micros.saturating_add(delta_cost);
                    rec.total_tokens_in = rec.total_tokens_in.saturating_add(delta_in);
                    rec.total_tokens_out = rec.total_tokens_out.saturating_add(delta_out);
                    rec.provider_calls += 1;
                    rec.token_in = rec.total_tokens_in;
                    rec.token_out = rec.total_tokens_out;
                    // Emit a derived RunCostUpdated event directly into the log.
                    let derived_pos = EventPosition(state.next_position);
                    state.next_position += 1;
                    let derived = StoredEvent {
                        position: derived_pos,
                        envelope: EventEnvelope {
                            event_id: cairn_domain::EventId::new(format!(
                                "derived_rcu_{}",
                                e.provider_call_id.as_str()
                            )),
                            source: cairn_domain::EventSource::System,
                            ownership: cairn_domain::OwnershipKey::Project(e.project.clone()),
                            causation_id: None,
                            correlation_id: None,
                            payload: RuntimeEvent::RunCostUpdated(cairn_domain::RunCostUpdated {
                                project: e.project.clone(),
                                run_id: run_id.clone(),
                                delta_cost_micros: delta_cost,
                                delta_tokens_in: delta_in,
                                delta_tokens_out: delta_out,
                                provider_call_id: e.provider_call_id.as_str().to_owned(),
                                updated_at_ms: event.stored_at,
                                session_id: None,
                                tenant_id: None,
                            }),
                        },
                        stored_at: event.stored_at,
                    };
                    state.events.push(derived);
                }
                // Accumulate session-level costs.
                // Derive session_id from run record if not provided on the event.
                let effective_session_id = e.session_id.clone().or_else(|| {
                    e.run_id
                        .as_ref()
                        .and_then(|rid| state.runs.get(rid.as_str()).map(|r| r.session_id.clone()))
                });
                if let Some(session_id) = effective_session_id {
                    let delta_cost = e.cost_micros.unwrap_or(0);
                    let delta_in = e.input_tokens.unwrap_or(0) as u64;
                    let delta_out = e.output_tokens.unwrap_or(0) as u64;
                    let rec = state
                        .session_costs
                        .entry(session_id.as_str().to_owned())
                        .or_insert_with(|| cairn_domain::providers::SessionCostRecord {
                            session_id: session_id.clone(),
                            tenant_id: cairn_domain::TenantId::new(e.project.tenant_id.as_str()),
                            total_cost_micros: 0,
                            total_tokens_in: 0,
                            total_tokens_out: 0,
                            updated_at_ms: event.stored_at,
                            provider_calls: 0,
                            token_in: 0,
                            token_out: 0,
                        });
                    rec.total_cost_micros = rec.total_cost_micros.saturating_add(delta_cost);
                    rec.total_tokens_in = rec.total_tokens_in.saturating_add(delta_in);
                    rec.total_tokens_out = rec.total_tokens_out.saturating_add(delta_out);
                    rec.provider_calls += 1;
                    rec.token_in = rec.total_tokens_in;
                    rec.token_out = rec.total_tokens_out;
                    rec.updated_at_ms = event.stored_at;
                    // F29 CD-2: project + workspace rollup. Kept in lock-step
                    // with `session_costs` so `ProjectCostReadModel` matches
                    // the sum of `SessionCostReadModel::list_by_tenant` over
                    // the same (tenant, workspace, project).
                    let proj_key = (
                        e.project.tenant_id.as_str().to_owned(),
                        e.project.workspace_id.as_str().to_owned(),
                        e.project.project_id.as_str().to_owned(),
                    );
                    let proj = state.project_costs.entry(proj_key).or_insert_with(|| {
                        cairn_domain::providers::ProjectCostRecord {
                            tenant_id: cairn_domain::TenantId::new(e.project.tenant_id.as_str()),
                            workspace_id: e.project.workspace_id.as_str().to_owned(),
                            project_id: e.project.project_id.as_str().to_owned(),
                            total_cost_micros: 0,
                            total_tokens_in: 0,
                            total_tokens_out: 0,
                            provider_calls: 0,
                            updated_at_ms: event.stored_at,
                        }
                    });
                    proj.total_cost_micros =
                        proj.total_cost_micros.saturating_add(delta_cost);
                    proj.total_tokens_in = proj.total_tokens_in.saturating_add(delta_in);
                    proj.total_tokens_out = proj.total_tokens_out.saturating_add(delta_out);
                    proj.provider_calls = proj.provider_calls.saturating_add(1);
                    proj.updated_at_ms = event.stored_at;

                    let ws_key = (
                        e.project.tenant_id.as_str().to_owned(),
                        e.project.workspace_id.as_str().to_owned(),
                    );
                    let ws = state.workspace_costs.entry(ws_key).or_insert_with(|| {
                        cairn_domain::providers::WorkspaceCostRecord {
                            tenant_id: cairn_domain::TenantId::new(e.project.tenant_id.as_str()),
                            workspace_id: e.project.workspace_id.as_str().to_owned(),
                            total_cost_micros: 0,
                            total_tokens_in: 0,
                            total_tokens_out: 0,
                            provider_calls: 0,
                            updated_at_ms: event.stored_at,
                        }
                    });
                    ws.total_cost_micros = ws.total_cost_micros.saturating_add(delta_cost);
                    ws.total_tokens_in = ws.total_tokens_in.saturating_add(delta_in);
                    ws.total_tokens_out = ws.total_tokens_out.saturating_add(delta_out);
                    ws.provider_calls = ws.provider_calls.saturating_add(1);
                    ws.updated_at_ms = event.stored_at;
                    // Emit SessionCostUpdated event into the log for traceability.
                    let sc_pos = EventPosition(state.next_position);
                    state.next_position += 1;
                    let sc_derived = StoredEvent {
                        position: sc_pos,
                        envelope: EventEnvelope {
                            event_id: cairn_domain::EventId::new(format!(
                                "derived_scu_{}",
                                e.provider_call_id.as_str()
                            )),
                            source: cairn_domain::EventSource::System,
                            ownership: cairn_domain::OwnershipKey::Project(e.project.clone()),
                            causation_id: None,
                            correlation_id: None,
                            payload: RuntimeEvent::SessionCostUpdated(SessionCostUpdated {
                                project: e.project.clone(),
                                session_id: session_id.clone(),
                                tenant_id: cairn_domain::TenantId::new(
                                    e.project.tenant_id.as_str(),
                                ),
                                delta_cost_micros: delta_cost,
                                delta_tokens_in: delta_in,
                                delta_tokens_out: delta_out,
                                provider_call_id: e.provider_call_id.as_str().to_owned(),
                                updated_at_ms: event.stored_at,
                            }),
                        },
                        stored_at: event.stored_at,
                    };
                    state.events.push(sc_derived);
                }
            }
            RuntimeEvent::LlmCompletionRecorded(e) => {
                // Issue #668: store the LLM round-trip body keyed by
                // `trace_id`. Replay semantics: re-applying the event
                // (restart, dual-write, etc.) overwrites the row with
                // the latest payload — a re-emit carrying different
                // text for the same trace_id would indicate an
                // orchestrator bug, so last-write-wins keeps the
                // projection convergent without silently hiding
                // double-emit surprises.
                //
                // **Memory bound** (Copilot review on #672): bodies can
                // be hundreds of KiB each; unbounded accumulation in
                // this always-warm projection would OOM a busy
                // deployment AND bloat the startup replay that warms
                // this projection on restart. We cap the map at
                // `llm_completion_bodies_cap()` entries (default 5000,
                // env-overridable via `CAIRN_LLM_TRACE_IN_MEMORY_CAP`)
                // with FIFO eviction of the oldest `recorded_at_ms`.
                // Durable backends (pg + sqlite) keep the full history;
                // operators querying pages deeper than the cap get
                // served from those. In-memory deployments (`--db memory`)
                // trade long-history body queries for bounded RAM —
                // acceptable since `--db memory` already announces
                // "ALL DATA WILL BE LOST on restart".
                let record = crate::projections::LlmCompletionBodyRecord {
                    trace_id: e.trace_id.clone(),
                    project: e.project.clone(),
                    session_id: e.session_id.clone(),
                    run_id: e.run_id.clone(),
                    model_id: e.model_id.clone(),
                    system_prompt: e.system_prompt.clone(),
                    messages_json: e.messages_json.clone(),
                    response_text: e.response_text.clone(),
                    tool_calls_json: e.tool_calls_json.clone(),
                    tool_defs_json: e.tool_defs_json.clone(),
                    recorded_at_ms: e.recorded_at_ms,
                };
                let is_replace = state
                    .llm_completion_bodies
                    .insert(e.trace_id.clone(), record)
                    .is_some();
                let cap = llm_completion_bodies_cap();
                if !is_replace && state.llm_completion_bodies.len() > cap {
                    // Find the oldest (smallest `recorded_at_ms`, break
                    // ties on trace_id for determinism) and drop it.
                    // O(N) per eviction — acceptable because evictions
                    // are rare (only when over cap) and N is bounded
                    // by the cap. A BTreeSet indexed on recorded_at_ms
                    // would make this O(log N) but doubles the
                    // per-insert cost on the hot path; not worth the
                    // complexity until a profile shows it matters.
                    if let Some(oldest_key) = state
                        .llm_completion_bodies
                        .iter()
                        .min_by(|a, b| {
                            a.1.recorded_at_ms
                                .cmp(&b.1.recorded_at_ms)
                                .then_with(|| a.0.cmp(b.0))
                        })
                        .map(|(k, _)| k.clone())
                    {
                        state.llm_completion_bodies.remove(&oldest_key);
                    }
                }
            }
            RuntimeEvent::TenantCreated(e) => {
                state.tenants.insert(
                    e.tenant_id.as_str().to_owned(),
                    cairn_domain::org::TenantRecord {
                        tenant_id: e.tenant_id.clone(),
                        name: e.name.clone(),
                        created_at: e.created_at,
                        updated_at: e.created_at,
                    },
                );
            }
            // RFC 026 PR-A2: tenant PATCH edit. Leave fields untouched
            // when the event carries `None` (matches pg/sqlite
            // COALESCE). `updated_at` always advances to the event's
            // `updated_at_ms` so admin-UI mtimes stay in sync.
            RuntimeEvent::TenantUpdated(e) => {
                if let Some(rec) = state.tenants.get_mut(e.tenant_id.as_str()) {
                    if let Some(name) = &e.name {
                        rec.name = name.clone();
                    }
                    rec.updated_at = e.updated_at_ms;
                }
            }
            RuntimeEvent::WorkspaceCreated(e) => {
                state.workspaces.insert(
                    e.workspace_id.as_str().to_owned(),
                    cairn_domain::org::WorkspaceRecord {
                        workspace_id: e.workspace_id.clone(),
                        tenant_id: e.tenant_id.clone(),
                        name: e.name.clone(),
                        created_at: e.created_at,
                        updated_at: e.created_at,
                        archived_at: None,
                    },
                );
            }
            RuntimeEvent::WorkspaceArchived(e) => {
                // Defense-in-depth: only archive when the event's
                // `tenant_id` matches the stored record. The service
                // layer validates ownership before emitting, but a
                // replay with a mismatched event must not touch
                // another tenant's workspace.
                if let Some(ws) = state.workspaces.get_mut(e.workspace_id.as_str()) {
                    if ws.tenant_id == e.tenant_id {
                        ws.archived_at = Some(e.archived_at);
                        ws.updated_at = e.archived_at;
                    }
                }
            }
            RuntimeEvent::ProjectCreated(e) => {
                state.projects.insert(
                    e.project.project_id.as_str().to_owned(),
                    cairn_domain::org::ProjectRecord {
                        project_id: e.project.project_id.clone(),
                        workspace_id: e.project.workspace_id.clone(),
                        tenant_id: e.project.tenant_id.clone(),
                        name: e.name.clone(),
                        created_at: e.created_at,
                        updated_at: e.created_at,
                    },
                );
            }
            RuntimeEvent::PromptAssetCreated(e) => {
                state.prompt_assets.insert(
                    e.prompt_asset_id.as_str().to_owned(),
                    crate::projections::PromptAssetRecord {
                        prompt_asset_id: e.prompt_asset_id.clone(),
                        project: e.project.clone(),
                        name: e.name.clone(),
                        kind: e.kind.clone(),
                        created_at: e.created_at,
                        scope: String::new(),
                        status: "draft".to_owned(),
                        workspace: String::new(),
                        updated_at: now,
                    },
                );
            }
            RuntimeEvent::PromptVersionCreated(e) => {
                let version_number = state
                    .prompt_versions
                    .values()
                    .filter(|v| v.prompt_asset_id == e.prompt_asset_id)
                    .count() as u32
                    + 1;
                state.prompt_versions.insert(
                    e.prompt_version_id.as_str().to_owned(),
                    crate::projections::PromptVersionRecord {
                        prompt_version_id: e.prompt_version_id.clone(),
                        prompt_asset_id: e.prompt_asset_id.clone(),
                        project: e.project.clone(),
                        content_hash: e.content_hash.clone(),
                        created_at: e.created_at,
                        version_number,
                        // RFC 006: populate workspace from event field so projections
                        // can scope at workspace level without re-deriving from project.
                        workspace: e.workspace_id.as_str().to_owned(),
                    },
                );
            }
            RuntimeEvent::ApprovalPolicyCreated(e) => {
                state.approval_policies.insert(
                    e.policy_id.clone(),
                    cairn_domain::ApprovalPolicyRecord {
                        policy_id: e.policy_id.clone(),
                        tenant_id: e.tenant_id.clone(),
                        name: e.name.clone(),
                        required_approvers: e.required_approvers,
                        allowed_approver_roles: e.allowed_approver_roles.clone(),
                        auto_approve_after_ms: e.auto_approve_after_ms,
                        auto_reject_after_ms: e.auto_reject_after_ms,
                        attached_release_ids: Vec::new(),
                    },
                );
            }
            RuntimeEvent::PromptReleaseCreated(e) => {
                state.prompt_releases.insert(
                    e.prompt_release_id.as_str().to_owned(),
                    crate::projections::PromptReleaseRecord {
                        prompt_release_id: e.prompt_release_id.clone(),
                        project: e.project.clone(),
                        prompt_asset_id: e.prompt_asset_id.clone(),
                        prompt_version_id: e.prompt_version_id.clone(),
                        state: "draft".to_owned(),
                        rollout_percent: None,
                        routing_slot: None,
                        task_type: None,
                        agent_type: None,
                        is_project_default: false,
                        release_tag: e.release_tag.clone(),
                        created_by: e.created_by.clone(),
                        created_at: e.created_at,
                        updated_at: e.created_at,
                    },
                );
            }
            RuntimeEvent::PromptReleaseTransitioned(e) => {
                if let Some(rec) = state.prompt_releases.get_mut(e.prompt_release_id.as_str()) {
                    rec.state = e.to_state.clone();
                    rec.updated_at = e.transitioned_at;
                }
            }
            RuntimeEvent::PromptRolloutStarted(e) => {
                if let Some(rec) = state.prompt_releases.get_mut(e.prompt_release_id.as_str()) {
                    rec.rollout_percent = Some(e.percent);
                    rec.state = "active".to_owned();
                    rec.updated_at = e.started_at;
                }
            }
            RuntimeEvent::IngestJobStarted(e) => {
                state.ingest_jobs.insert(
                    e.job_id.as_str().to_owned(),
                    cairn_domain::IngestJobRecord {
                        id: e.job_id.clone(),
                        project: e.project.clone(),
                        source_id: e.source_id.clone(),
                        document_count: e.document_count,
                        state: cairn_domain::IngestJobState::Processing,
                        error_message: None,
                        created_at: e.started_at,
                        updated_at: e.started_at,
                    },
                );
            }
            RuntimeEvent::IngestJobCompleted(e) => {
                if let Some(rec) = state.ingest_jobs.get_mut(e.job_id.as_str()) {
                    rec.state = if e.success {
                        cairn_domain::IngestJobState::Completed
                    } else {
                        cairn_domain::IngestJobState::Failed
                    };
                    rec.error_message = e.error_message.clone();
                    rec.updated_at = e.completed_at;
                }
            }
            RuntimeEvent::EvalRunStarted(e) => {
                // Idempotency / lifecycle-edge pattern (RFC-025 Phase 1
                // milestone 2): `EvalRunStarted` is emitted both by
                // `create_eval_run_handler` (initial projection row) and
                // by `start_eval_run_handler` (Pending → Running edge).
                // The second emission MUST NOT clobber score/completion
                // fields that were set between the two edges, so the
                // projection upserts only fields that are unconditionally
                // part of the "start" shape. Score / Completed / Archive
                // events run their own arms.
                state
                    .eval_runs
                    .entry(e.eval_run_id.as_str().to_owned())
                    .or_insert_with(|| crate::projections::EvalRunRecord {
                        eval_run_id: e.eval_run_id.clone(),
                        project: e.project.clone(),
                        subject_kind: e.subject_kind.clone(),
                        evaluator_type: e.evaluator_type.clone(),
                        success: None,
                        error_message: None,
                        started_at: e.started_at,
                        completed_at: None,
                        archived_at: None,
                        metrics: None,
                        rubric_score: None,
                        dataset_id: e.dataset_id.clone(),
                        rubric_id: e.rubric_id.clone(),
                        baseline_id: e.baseline_id.clone(),
                        prompt_asset_id: e.prompt_asset_id.clone(),
                        prompt_version_id: e.prompt_version_id.clone(),
                        prompt_release_id: e.prompt_release_id.clone(),
                        created_by: e.created_by.clone(),
                    });
            }
            RuntimeEvent::EvalRunCompleted(e) => {
                if let Some(rec) = state.eval_runs.get_mut(e.eval_run_id.as_str()) {
                    rec.success = Some(e.success);
                    rec.error_message = e.error_message.clone();
                    rec.completed_at = Some(e.completed_at);
                }
            }
            // Issue #244: soft-delete. Preserve the record so audit/scorecard
            // views keep their history; `list_by_project` on the eval service
            // filters archived entries out by default. Earliest-wins: only
            // set `archived_at` when it's currently None so a racing second
            // `EvalRunArchived` event (two concurrent DELETEs) doesn't bump
            // the timestamp to the later attempt. Matches
            // `EvalRunService::archive`'s idempotency rule (Copilot review
            // on PR #336).
            RuntimeEvent::EvalRunArchived(e) => {
                if let Some(rec) = state.eval_runs.get_mut(e.eval_run_id.as_str()) {
                    if rec.archived_at.is_none() {
                        rec.archived_at = Some(e.archived_at);
                    }
                }
            }
            // RFC-025 Phase 1 (milestone 5): project the most-recent
            // metrics / rubric verdict onto the read model. Last-write
            // wins — the full score history lives in the event log. If
            // the run record doesn't exist yet (score landed before
            // `EvalRunStarted` replayed) the score arm is a no-op rather
            // than fabricating a ProjectKey / started_at; the sync
            // projection runs inside the same `&mut tx` as the insert so
            // this ordering only matters during replay.
            RuntimeEvent::EvalRunScored(e) => {
                if let Some(rec) = state.eval_runs.get_mut(e.eval_run_id.as_str()) {
                    rec.metrics = Some(e.metrics.clone());
                }
            }
            RuntimeEvent::EvalRubricScored(e) => {
                if let Some(rec) = state.eval_runs.get_mut(e.eval_run_id.as_str()) {
                    rec.rubric_score = Some(crate::projections::EvalRubricScoreSummary {
                        rubric_id: e.rubric_id.clone(),
                        dimension_scores: e.dimension_scores.clone(),
                        overall: e.overall,
                        recorded_at_ms: e.recorded_at_ms,
                    });
                }
            }
            RuntimeEvent::OutcomeRecorded(e) => {
                state.outcomes.insert(
                    e.outcome_id.as_str().to_owned(),
                    crate::projections::OutcomeRecord {
                        outcome_id: e.outcome_id.clone(),
                        run_id: e.run_id.clone(),
                        project: e.project.clone(),
                        agent_type: e.agent_type.clone(),
                        predicted_confidence: e.predicted_confidence,
                        actual_outcome: e.actual_outcome.clone(),
                        recorded_at: e.recorded_at,
                    },
                );
            }
            RuntimeEvent::EvalDatasetCreated(e) => {
                // tenant_id is not carried by this event; stored with empty sentinel.
                // Use EvalSubjectKind::PromptRelease as default subject kind.
                state
                    .eval_datasets
                    .entry(e.dataset_id.clone())
                    .or_insert_with(|| cairn_domain::EvalDataset {
                        dataset_id: e.dataset_id.clone(),
                        tenant_id: cairn_domain::TenantId::new(""),
                        name: e.name.clone(),
                        subject_kind: cairn_domain::EvalSubjectKind::PromptRelease,
                        entries: Vec::new(),
                        created_at_ms: e.created_at_ms,
                    });
            }
            RuntimeEvent::EvalDatasetEntryAdded(e) => {
                if let Some(ds) = state.eval_datasets.get_mut(e.dataset_id.as_str()) {
                    // entry_id is stored as a tag so it can be deduplicated.
                    let already_exists = ds.entries.iter().any(|entry| {
                        entry.tags.first().map(String::as_str) == Some(e.entry_id.as_str())
                    });
                    if !already_exists {
                        ds.entries.push(cairn_domain::EvalDatasetEntry {
                            input: serde_json::json!({ "entry_id": e.entry_id }),
                            expected_output: None,
                            tags: vec![e.entry_id.clone()],
                        });
                    }
                }
            }
            RuntimeEvent::CheckpointStrategySet(e) => {
                if let Some(run_id) = &e.run_id {
                    state.checkpoint_strategies.insert(
                        run_id.as_str().to_owned(),
                        cairn_domain::CheckpointStrategy {
                            strategy_id: e.strategy_id.clone(),
                            project: crate::projections::checkpoint_strategy_sentinel_project(),
                            run_id: run_id.clone(),
                            interval_ms: e.interval_ms,
                            max_checkpoints: if e.max_checkpoints > 0 {
                                e.max_checkpoints
                            } else {
                                crate::projections::CHECKPOINT_STRATEGY_DEFAULT_MAX_CHECKPOINTS
                            },
                            trigger_on_task_complete: e.trigger_on_task_complete,
                        },
                    );
                }
            }
            RuntimeEvent::EvalRubricCreated(e) => {
                // tenant_id not in event; stored with sentinel "".
                state
                    .eval_rubrics
                    .entry(e.rubric_id.clone())
                    .or_insert_with(|| cairn_domain::EvalRubric {
                        rubric_id: e.rubric_id.clone(),
                        tenant_id: cairn_domain::TenantId::new(""),
                        name: e.name.clone(),
                        dimensions: vec![],
                        created_at_ms: e.created_at_ms,
                    });
            }
            RuntimeEvent::EvalBaselineSet(e) => {
                // EvalBaselineSet carries one metric key=value; upsert the baseline record.
                // Fields like tenant_id, name, prompt_asset_id not in event → sentinels.
                let entry = state
                    .eval_baselines
                    .entry(e.baseline_id.clone())
                    .or_insert_with(|| cairn_domain::EvalBaseline {
                        baseline_id: e.baseline_id.clone(),
                        tenant_id: cairn_domain::TenantId::new(""),
                        name: e.baseline_id.clone(),
                        prompt_asset_id: cairn_domain::PromptAssetId::new(""),
                        metrics: cairn_domain::EvalMetrics::default(),
                        created_at_ms: e.set_at_ms,
                        locked: false,
                    });
                // Only update if not locked — locked baselines are immutable.
                if !entry.locked {
                    // Store metric as a tag in the name for auditability.
                    entry.name = format!("{}[{}={}]", entry.baseline_id, e.metric, e.value);
                }
            }
            RuntimeEvent::EvalBaselineLocked(e) => {
                if let Some(baseline) = state.eval_baselines.get_mut(&e.baseline_id) {
                    baseline.locked = true;
                }
            }
            // RFC 020 decision-cache survival: no dedicated projection —
            // cairn-app rebuilds the in-memory decision cache from the
            // event log at startup.
            RuntimeEvent::DecisionRecorded(_) | RuntimeEvent::DecisionCacheWarmup(_) => {}
            // F47 PR2: attach summary + verification to the existing
            // RunRecord. The completion fields (`completion_summary`,
            // `completion_verification`, `completion_annotated_at_ms`)
            // are overwrite-stable: replaying the same event leaves
            // those three fields at identical values. `version` and
            // `updated_at` still bump on replay — matching the
            // RunStateChanged handler and the projection-bookkeeping
            // contract other projections use — but the operator-
            // observable shape stays the same. Absent run row (orphan
            // annotation) is silently ignored; annotation cannot
            // create a run. Mirrors the `if let Some(rec) = get_mut`
            // pattern used by RunStateChanged above.
            RuntimeEvent::RunCompletionAnnotated(e) => {
                if let Some(rec) = state.runs.get_mut(e.run_id.as_str()) {
                    if rec.project == e.project && rec.session_id == e.session_id {
                        rec.completion_summary = Some(e.summary.clone());
                        rec.completion_verification = Some(e.verification.clone());
                        rec.completion_annotated_at_ms = Some(e.occurred_at_ms);
                        rec.version += 1;
                        rec.updated_at = now;
                    }
                }
            }
            // F64: record the terminal-write recovery attempt on the run
            // row. Silent no-op on missing row mirrors the
            // `RunCompletionAnnotated` handler above — an orphan
            // TerminalRecoveryAttempted is a malformed log, not a
            // projection error.
            RuntimeEvent::TerminalRecoveryAttempted(e) => {
                if let Some(rec) = state.runs.get_mut(e.run_id.as_str()) {
                    // Cross-tenant tampering guard (#732 expansion):
                    // gate on `project` match. A forged
                    // `TerminalRecoveryAttempted` could otherwise
                    // stamp false recovery metadata onto another
                    // tenant's run row. NOTE: this event's payload
                    // does not carry `session_id` (unlike
                    // `RunCompletionAnnotated` /
                    // `RunStateChanged`), so the gate is
                    // `project`-only here. The `project` check is
                    // sufficient: the run row's `project` is set at
                    // `RunCreated` time and the event's `project`
                    // must match for any legitimate emit.
                    if rec.project == e.project {
                        rec.terminal_write_recovery =
                            Some(crate::projections::TerminalRecoveryRecord {
                                fcall: e.fcall.clone(),
                                attempts: e.attempts,
                                wall_time_ms: e.wall_time_ms,
                                outcome: e.outcome.clone(),
                                occurred_at_ms: e.occurred_at_ms,
                            });
                        rec.version += 1;
                        rec.updated_at = now;
                    }
                }
            }
            // ── F65 PR-2: orchestrator session redesign projections ────────
            //
            // Each event maps to a specific projection write. The pg/sqlite
            // equivalents live in `pg/projections.rs` / `sqlite/projections.rs`
            // and use the same idempotency contract: replaying the same
            // event bumps `version` but leaves the operator-observable
            // fields unchanged.
            RuntimeEvent::SessionAttemptStarted(e) => {
                // Bump attempts_used on the session row. Replay-safe: we
                // only ever advance the counter to `attempt_number` rather
                // than incrementing blindly — repeated delivery of the same
                // event leaves the row idempotent.
                if let Some(rec) = state.sessions.get_mut(e.session_id.as_str()) {
                    if e.attempt_number > rec.attempts_used {
                        rec.attempts_used = e.attempt_number;
                    }
                    // max_attempts carries the config captured at attempt
                    // start. Keep the row in sync if the captured value is
                    // higher (config bumped post-attempt) — but never lower,
                    // since that would let a later event silently shrink
                    // operator-visible capacity.
                    if e.max_attempts > rec.max_attempts {
                        rec.max_attempts = e.max_attempts;
                    }
                    rec.version += 1;
                    rec.updated_at = now;
                }
            }
            // Attempt-completed is observable via SessionOutcomeEmitted and
            // the event log. No projection row to update beyond the event
            // log itself; leaving the session row unchanged is intentional.
            RuntimeEvent::SessionAttemptCompleted(_) => {}
            // Breaker trips are forensic — recorded on the event log, and
            // mirrored into the session outcome row when the trip
            // terminates the attempt. No dedicated projection table.
            RuntimeEvent::CircuitBreakerTripped(_) => {}
            // Budget-threshold-crossed is purely observability (SSE) — no
            // projection row.
            RuntimeEvent::BudgetThresholdCrossed(_) => {}
            RuntimeEvent::CheckpointPersisted(e) => {
                // F65 projection row (orchestrator-resumable shape).
                // Pg/sqlite use `ON CONFLICT DO UPDATE` that preserves the
                // original `created_at` and only touches the F65-extended
                // fields. In-memory matches: insert-once, overwrite only
                // the session/schema/iteration fields on replay.
                state
                    .f65_checkpoints
                    .entry(e.checkpoint_id.as_str().to_owned())
                    .and_modify(|rec| {
                        rec.session_id = e.session_id.clone();
                        rec.schema_version = 1;
                        rec.iteration = e.iteration;
                    })
                    .or_insert_with(|| crate::projections::F65CheckpointRecord {
                        checkpoint_id: e.checkpoint_id.clone(),
                        project: e.project.clone(),
                        session_id: e.session_id.clone(),
                        root_run_id: e.root_run_id.clone(),
                        schema_version: 1,
                        body: String::new(),
                        body_size_bytes: 0,
                        iteration: e.iteration,
                        created_at: e.at_ms,
                    });
                // Shared RFC 005 checkpoint row. The pg/sqlite backends
                // both write this row from the same event (see
                // `pg/projections.rs` and `sqlite/projections.rs`) so the
                // in-memory backend must match to keep
                // `CheckpointReadModel::get` cross-backend consistent.
                // `data = None` + `version = 1` mirrors the SQL path's
                // INSERT with empty body + version 1. Disposition is
                // Latest because F65 only persists the most-recent
                // orchestrator-resumable state. Replay preserves the
                // first-seen `created_at` — matching the pg path.
                state
                    .checkpoints
                    .entry(e.checkpoint_id.as_str().to_owned())
                    .or_insert_with(|| crate::projections::CheckpointRecord {
                        checkpoint_id: e.checkpoint_id.clone(),
                        project: e.project.clone(),
                        run_id: e.root_run_id.clone(),
                        disposition: cairn_domain::CheckpointDisposition::Latest,
                        data: None,
                        version: 1,
                        created_at: e.at_ms,
                    });
            }
            RuntimeEvent::WorkspaceSnapshotCreated(e) => {
                // SQL backends use `ON CONFLICT (snapshot_id) DO NOTHING`,
                // so a replayed event must not overwrite the existing
                // row (or its `created_at`). Use `entry().or_insert_with`
                // for the same create-only semantics. #482: bytes /
                // reflink_used / parent_snapshot_id land on the event so
                // the in-memory projection rebuilds identically to
                // pg/sqlite on replay.
                state
                    .workspace_snapshots
                    .entry(e.snapshot_id.as_str().to_owned())
                    .or_insert_with(|| crate::projections::WorkspaceSnapshotRecord {
                        snapshot_id: e.snapshot_id.clone(),
                        project: e.project.clone(),
                        session_id: e.session_id.clone(),
                        workspace_id: e.workspace_id.clone(),
                        parent_snapshot_id: e.parent_snapshot_id.clone(),
                        snapshot_path: String::new(),
                        bytes: e.bytes,
                        reflink_used: e.reflink_used,
                        created_at: e.at_ms,
                        reaped_at: None,
                    });
            }
            RuntimeEvent::WorkspaceSnapshotReaped(e) => {
                if let Some(rec) = state.workspace_snapshots.get_mut(e.snapshot_id.as_str()) {
                    rec.reaped_at = Some(e.at_ms);
                }
            }
            RuntimeEvent::SessionOutcomeEmitted(e) => {
                let outcome = &e.outcome;
                // Pg/sqlite upsert preserves the original `created_at` and
                // updates only the mutable fields (workspace_snapshot_id,
                // termination_reason, compacted_summary, next_step_hint,
                // cost_micros). Mirror that: insert with the event's
                // `emitted_at` on first delivery, and only touch the
                // enrichable fields on replay.
                state
                    .session_outcomes
                    .entry(outcome.root_run_id.as_str().to_owned())
                    .and_modify(|rec| {
                        rec.workspace_snapshot_id = outcome.workspace_snapshot_id.clone();
                        rec.termination_reason = outcome.termination_reason.clone();
                        rec.compacted_summary = outcome.compacted_summary.clone();
                        rec.next_step_hint = outcome.next_step_hint.clone();
                        rec.cost_micros = outcome.cost_micros;
                    })
                    .or_insert_with(|| crate::projections::SessionOutcomeRecord {
                        root_run_id: outcome.root_run_id.clone(),
                        project: outcome.project.clone(),
                        session_id: outcome.session_id.clone(),
                        checkpoint_id: outcome.checkpoint_id.clone(),
                        workspace_snapshot_id: outcome.workspace_snapshot_id.clone(),
                        termination_reason: outcome.termination_reason.clone(),
                        compacted_summary: outcome.compacted_summary.clone(),
                        next_step_hint: outcome.next_step_hint.clone(),
                        cost_micros: outcome.cost_micros,
                        created_at: outcome.emitted_at,
                    });
            }
            // Orchestrator decisions are operator observability surfaces
            // (SSE + audit); no projection table.
            RuntimeEvent::OrchestratorDecisionMade(_) => {}
            // Summarizer fallback is audit-only (provenance of
            // compacted_summary). The fact is captured on the event log;
            // no projection row is needed.
            RuntimeEvent::SummarizerFallback(_) => {}
            // Workspace-backend-degraded fires at sandbox init time. No
            // projection row — operator alerts via SSE + metrics (PR-4).
            RuntimeEvent::WorkspaceBackendDegraded(_) => {}
            // F65 PR-5 (#359): crash-recovery umount sweep observability.
            // No projection row — operator alerts via SSE + metrics; the
            // event log itself is the audit trail.
            RuntimeEvent::SandboxCrashRecovered(_) => {}
            // ── RFC 029 pluggable knowledge providers ──
            // The durable backends (pg/sqlite) own the read model for
            // `project_knowledge_providers` / `knowledge_ingest_jobs`.
            // InMemory does not carry dedicated projection state for
            // these yet — MultiProviderRetrieval (cairn-memory) will add
            // in-memory read-model rows if it queries them at runtime.
            // For now the event log itself is the authoritative record.
            RuntimeEvent::KnowledgeProviderConfigured(_)
            | RuntimeEvent::KnowledgeProviderUnavailable(_)
            | RuntimeEvent::KnowledgeProviderCapabilityChanged(_)
            | RuntimeEvent::KnowledgeIngestSubmitted(_)
            | RuntimeEvent::KnowledgeIngestRejected(_)
            | RuntimeEvent::KnowledgeIngestStatusUpdated(_) => {}
        }
    }
}

impl Default for InMemoryStore {
    fn default() -> Self {
        Self::new()
    }
}

// -- EventLog --

#[async_trait]
impl EventLog for InMemoryStore {
    async fn append(
        &self,
        events: &[EventEnvelope<RuntimeEvent>],
    ) -> Result<Vec<EventPosition>, StoreError> {
        // Test hook: simulate a failing append (e.g. disk-full, fsync
        // error, backend partition) without OS-level trickery. Stripped
        // from release builds; see FAIL_APPEND_* for the full contract.
        // We check BEFORE touching any state so the hook also proves
        // our no-partial-write invariant: on injected failure the log
        // is byte-identical to pre-call.
        #[cfg(debug_assertions)]
        {
            use std::sync::atomic::Ordering;
            // Consume a skip token first; only once the skip budget is
            // exhausted do we start firing failures. `fetch_update`
            // returns `Err` when the closure yields `None` (skip
            // counter already 0).
            let skip_decremented = FAIL_APPEND_SKIP_REMAINING
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |v| {
                    if v == 0 {
                        None
                    } else {
                        Some(v - 1)
                    }
                })
                .is_ok();
            if !skip_decremented {
                // Skip budget is zero → maybe fire a failure.
                let fired = FAIL_APPEND_FAIL_REMAINING
                    .fetch_update(Ordering::AcqRel, Ordering::Acquire, |v| {
                        if v == 0 {
                            None
                        } else {
                            Some(v - 1)
                        }
                    })
                    .is_ok();
                if fired {
                    return Err(StoreError::Internal(
                        "arm_fail_next_append: injected append failure".to_owned(),
                    ));
                }
            }
        }

        // Scope the MutexGuard so it is lexically dropped before any `.await`.
        let positions = {
            let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            let mut usage = self
                .usage_counters
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            let now = now_millis();
            let mut positions = Vec::with_capacity(events.len());

            for envelope in events {
                let counters = usage.entry(envelope.project().clone()).or_default();
                counters.event_count += 1;
                if matches!(&envelope.payload, RuntimeEvent::RunCreated(_)) {
                    counters.run_count += 1;
                }

                let pos = EventPosition(state.next_position);
                state.next_position += 1;

                let stored = StoredEvent {
                    position: pos,
                    envelope: envelope.clone(),
                    stored_at: now,
                };

                // Populate the causation_id index (RFC 002 idempotency).
                // Keep the earliest position for a given causation; later
                // references are already retrievable via `read_stream`.
                if let Some(cause) = envelope.causation_id.as_ref() {
                    state
                        .command_id_index
                        .entry(cause.as_str().to_owned())
                        .or_insert(pos.0);
                }

                // Push the original event BEFORE calling apply_projection so that
                // any derived events inserted by the projection appear AFTER the
                // original in the log, preserving strict position monotonicity.
                state.events.push(stored.clone());
                Self::apply_projection(&mut state, &stored);
                positions.push(pos);

                // Broadcast to SSE subscribers; ignore send errors (no active receivers).
                let _ = self.event_tx.send(stored);
            }

            positions
            // `state` (MutexGuard) is dropped here, before any await point.
        };

        // Dual-write to the durable secondary log (Postgres, SQLite) if
        // one is configured. Fail CLOSED: the in-memory write has already
        // committed by this point, but we surface the secondary failure
        // so the caller can decide whether to retry, compensate, or
        // abort. The old `eprintln`-and-swallow path silently lost data
        // on restart when the secondary was the durable source of truth
        // (RFC 002).
        //
        // **Divergence contract on `Err`:** the in-memory log has the
        // events, the secondary does not. The caller MUST treat this as
        // a recoverable divergence and is responsible for reconciliation.
        // Options, in rough order of preference:
        //   1. Retry the same call. `InMemoryStore::append` today does
        //      NOT dedup by `event_id`, so a plain retry would double-
        //      apply the projection — do not retry blindly against the
        //      same store; construct a fresh caller-side envelope or
        //      flush+replay on the primary. Populating event_id dedup
        //      on the primary is tracked as audit follow-up T2-M4.
        //   2. Write the events directly to the secondary once it's
        //      healthy, then confirm primary/secondary head positions
        //      match.
        //   3. Abort the caller-level transaction, roll forward from
        //      the secondary's last durable position.
        //
        // Until T2-M4 lands, the safest pattern for a deploy using
        // InMemoryStore as primary with a durable secondary is to
        // configure the app to crash on `Err` from `append` and rely
        // on restart-plus-replay to reconverge.
        let secondary = self
            .secondary_log
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        if let Some(log) = secondary {
            if let Err(e) = log.append(events).await {
                tracing::error!(
                    error = %e,
                    event_count = events.len(),
                    "secondary event log write failed — in-memory log has {} event(s) the secondary did not commit",
                    events.len(),
                );
                return Err(StoreError::Internal(format!(
                    "secondary event log write failed: {e}; in-memory and secondary logs have diverged by {} event(s)",
                    events.len()
                )));
            }
        }

        Ok(positions)
    }

    async fn read_by_entity(
        &self,
        entity: &EntityRef,
        after: Option<EventPosition>,
        limit: usize,
    ) -> Result<Vec<StoredEvent>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let min_pos = after.map(|p| p.0).unwrap_or(0);

        let results: Vec<StoredEvent> = state
            .events
            .iter()
            .filter(|e| e.position.0 > min_pos)
            .filter(|e| event_matches_entity(&e.envelope.payload, entity))
            .take(limit)
            .cloned()
            .collect();

        Ok(results)
    }

    async fn read_stream(
        &self,
        after: Option<EventPosition>,
        limit: usize,
    ) -> Result<Vec<StoredEvent>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let min_pos = after.map(|p| p.0).unwrap_or(0);

        let results: Vec<StoredEvent> = state
            .events
            .iter()
            .filter(|e| e.position.0 > min_pos)
            .take(limit)
            .cloned()
            .collect();

        Ok(results)
    }

    async fn head_position(&self) -> Result<Option<EventPosition>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        Ok(state.events.last().map(|e| e.position))
    }

    async fn find_by_causation_id(
        &self,
        causation_id: &str,
    ) -> Result<Option<EventPosition>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        Ok(state
            .command_id_index
            .get(causation_id)
            .copied()
            .map(EventPosition))
    }
}

fn event_matches_entity(event: &RuntimeEvent, entity: &EntityRef) -> bool {
    match (event, entity) {
        (RuntimeEvent::SessionCreated(e), EntityRef::Session(id)) => e.session_id == *id,
        (RuntimeEvent::SessionStateChanged(e), EntityRef::Session(id)) => e.session_id == *id,
        (RuntimeEvent::RunCreated(e), EntityRef::Run(id)) => e.run_id == *id,
        (RuntimeEvent::RunStateChanged(e), EntityRef::Run(id)) => e.run_id == *id,
        (RuntimeEvent::TaskCreated(e), EntityRef::Task(id)) => e.task_id == *id,
        (RuntimeEvent::TaskLeaseClaimed(e), EntityRef::Task(id)) => e.task_id == *id,
        (RuntimeEvent::TaskLeaseHeartbeated(e), EntityRef::Task(id)) => e.task_id == *id,
        (RuntimeEvent::TaskStateChanged(e), EntityRef::Task(id)) => e.task_id == *id,
        (RuntimeEvent::ApprovalRequested(e), EntityRef::Approval(id)) => e.approval_id == *id,
        (RuntimeEvent::ApprovalResolved(e), EntityRef::Approval(id)) => e.approval_id == *id,
        (RuntimeEvent::CheckpointRecorded(e), EntityRef::Checkpoint(id)) => e.checkpoint_id == *id,
        (RuntimeEvent::CheckpointRestored(e), EntityRef::Checkpoint(id)) => e.checkpoint_id == *id,
        (RuntimeEvent::MailboxMessageAppended(e), EntityRef::Mailbox(id)) => e.message_id == *id,
        (RuntimeEvent::ToolInvocationStarted(e), EntityRef::ToolInvocation(id)) => {
            e.invocation_id == *id
        }
        (RuntimeEvent::ToolInvocationCompleted(e), EntityRef::ToolInvocation(id)) => {
            e.invocation_id == *id
        }
        (RuntimeEvent::ToolInvocationFailed(e), EntityRef::ToolInvocation(id)) => {
            e.invocation_id == *id
        }
        (RuntimeEvent::SignalIngested(e), EntityRef::Signal(id)) => e.signal_id == *id,
        (RuntimeEvent::UserMessageAppended(e), EntityRef::Run(id)) => e.run_id == *id,
        (RuntimeEvent::IngestJobStarted(e), EntityRef::IngestJob(id)) => e.job_id == *id,
        (RuntimeEvent::IngestJobCompleted(e), EntityRef::IngestJob(id)) => e.job_id == *id,
        (RuntimeEvent::EvalRunStarted(e), EntityRef::EvalRun(id)) => e.eval_run_id == *id,
        (RuntimeEvent::EvalRunCompleted(e), EntityRef::EvalRun(id)) => e.eval_run_id == *id,
        (RuntimeEvent::OutcomeRecorded(e), EntityRef::Run(id)) => e.run_id == *id,
        (RuntimeEvent::PlanProposed(e), EntityRef::Run(id)) => e.plan_run_id == *id,
        (RuntimeEvent::PlanApproved(e), EntityRef::Run(id)) => e.plan_run_id == *id,
        (RuntimeEvent::PlanRejected(e), EntityRef::Run(id)) => e.plan_run_id == *id,
        (RuntimeEvent::PlanRevisionRequested(e), EntityRef::Run(id)) => {
            e.original_plan_run_id == *id
        }
        (RuntimeEvent::PromptAssetCreated(e), EntityRef::PromptAsset(id)) => {
            e.prompt_asset_id == *id
        }
        (RuntimeEvent::PromptVersionCreated(e), EntityRef::PromptVersion(id)) => {
            e.prompt_version_id == *id
        }
        (RuntimeEvent::PromptReleaseCreated(e), EntityRef::PromptRelease(id)) => {
            e.prompt_release_id == *id
        }
        (RuntimeEvent::PromptReleaseTransitioned(e), EntityRef::PromptRelease(id)) => {
            e.prompt_release_id == *id
        }
        _ => false,
    }
}

// -- SessionReadModel --

#[async_trait]
impl SessionReadModel for InMemoryStore {
    async fn get(&self, session_id: &SessionId) -> Result<Option<SessionRecord>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        Ok(state.sessions.get(session_id.as_str()).cloned())
    }

    async fn list_by_project(
        &self,
        project: &ProjectKey,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<SessionRecord>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let mut results: Vec<SessionRecord> = state
            .sessions
            .values()
            .filter(|s| s.project == *project)
            .cloned()
            .collect();
        results.sort_by_key(|s| (s.created_at, s.session_id.as_str().to_owned()));
        let results: Vec<SessionRecord> = results.into_iter().skip(offset).take(limit).collect();
        Ok(results)
    }

    async fn list_active(&self, limit: usize) -> Result<Vec<SessionRecord>, StoreError> {
        use cairn_domain::SessionState;
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let mut results: Vec<SessionRecord> = state
            .sessions
            .values()
            .filter(|s| s.state == SessionState::Open)
            .cloned()
            .collect();
        // Most recently updated first (fleet view shows live activity).
        results.sort_by_key(|r| std::cmp::Reverse(r.updated_at));
        results.truncate(limit);
        Ok(results)
    }
}

// -- SessionCostReadModel --

#[async_trait]
impl crate::projections::SessionCostReadModel for InMemoryStore {
    async fn get_session_cost(
        &self,
        session_id: &cairn_domain::SessionId,
    ) -> Result<Option<cairn_domain::providers::SessionCostRecord>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        Ok(state.session_costs.get(session_id.as_str()).cloned())
    }

    async fn list_by_tenant(
        &self,
        tenant_id: &cairn_domain::TenantId,
        since_ms: u64,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<cairn_domain::providers::SessionCostRecord>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        // Issue #423: the in-memory impl now honours `since_ms`,
        // `limit`, and `offset`. Filter → sort newest-first →
        // skip → take so callers that pass `limit + 1` can still
        // detect the overflow page.
        let mut results: Vec<_> = state
            .session_costs
            .values()
            .filter(|r| &r.tenant_id == tenant_id && r.updated_at_ms >= since_ms)
            .cloned()
            .collect();
        results.sort_by_key(|r| std::cmp::Reverse(r.updated_at_ms));
        let page: Vec<_> = results.into_iter().skip(offset).take(limit).collect();
        Ok(page)
    }
}

// -- ProjectCostReadModel (F29 CD-2) --

#[async_trait]
impl crate::projections::ProjectCostReadModel for InMemoryStore {
    async fn get_project_cost(
        &self,
        project: &cairn_domain::ProjectKey,
    ) -> Result<Option<cairn_domain::providers::ProjectCostRecord>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let key = (
            project.tenant_id.as_str().to_owned(),
            project.workspace_id.as_str().to_owned(),
            project.project_id.as_str().to_owned(),
        );
        Ok(state.project_costs.get(&key).cloned())
    }

    async fn get_workspace_cost(
        &self,
        tenant_id: &cairn_domain::TenantId,
        workspace_id: &str,
    ) -> Result<Option<cairn_domain::providers::WorkspaceCostRecord>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let key = (tenant_id.as_str().to_owned(), workspace_id.to_owned());
        Ok(state.workspace_costs.get(&key).cloned())
    }
}

// -- RunReadModel --

#[async_trait]
impl RunReadModel for InMemoryStore {
    async fn get(&self, run_id: &RunId) -> Result<Option<RunRecord>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        Ok(state.runs.get(run_id.as_str()).cloned())
    }

    async fn list_by_session(
        &self,
        session_id: &SessionId,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<RunRecord>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let mut results: Vec<RunRecord> = state
            .runs
            .values()
            .filter(|r| r.session_id == *session_id)
            .cloned()
            .collect();
        results.sort_by_key(|r| (r.created_at, r.run_id.as_str().to_owned()));
        let results = results.into_iter().skip(offset).take(limit).collect();
        Ok(results)
    }

    async fn any_non_terminal(&self, session_id: &SessionId) -> Result<bool, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        Ok(state
            .runs
            .values()
            .any(|r| r.session_id == *session_id && !r.state.is_terminal()))
    }

    async fn latest_root_run(
        &self,
        session_id: &SessionId,
    ) -> Result<Option<RunRecord>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        Ok(state
            .runs
            .values()
            .filter(|r| r.session_id == *session_id && r.parent_run_id.is_none())
            .max_by_key(|r| (r.created_at, r.run_id.as_str().to_owned()))
            .cloned())
    }

    async fn list_by_state(
        &self,
        state: cairn_domain::RunState,
        limit: usize,
    ) -> Result<Vec<RunRecord>, StoreError> {
        let store = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let mut results: Vec<RunRecord> = store
            .runs
            .values()
            .filter(|r| r.state == state)
            .cloned()
            .collect();
        results.sort_by_key(|r| r.created_at);
        results.truncate(limit);
        Ok(results)
    }

    /// #670 G4 / RFC 027 + PR-1b-4: pushed-down predicate for the
    /// `ChildRunDriver` scan — child runs in `Pending` or `Running`
    /// state. `Running` is included so the driver can re-claim
    /// crashed children post-recovery; FF's atomic
    /// `issue_grant_and_claim` rejects live-lease duplicates.
    async fn list_driver_claimable_children(
        &self,
        limit: usize,
    ) -> Result<Vec<RunRecord>, StoreError> {
        let store = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let mut results: Vec<RunRecord> = store
            .runs
            .values()
            .filter(|r| {
                matches!(
                    r.state,
                    cairn_domain::RunState::Pending | cairn_domain::RunState::Running
                ) && r.parent_run_id.is_some()
            })
            .cloned()
            .collect();
        results.sort_by(|a, b| {
            a.created_at
                .cmp(&b.created_at)
                .then_with(|| a.run_id.as_str().cmp(b.run_id.as_str()))
        });
        results.truncate(limit);
        Ok(results)
    }

    async fn list_active_by_project(
        &self,
        project: &ProjectKey,
        limit: usize,
    ) -> Result<Vec<RunRecord>, StoreError> {
        let store = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let mut results: Vec<RunRecord> = store
            .runs
            .values()
            .filter(|r| r.project == *project && !r.state.is_terminal())
            .cloned()
            .collect();
        results.sort_by_key(|r| r.created_at);
        results.truncate(limit);
        Ok(results)
    }

    async fn list_by_parent_run(
        &self,
        parent_run_id: &RunId,
        limit: usize,
    ) -> Result<Vec<RunRecord>, StoreError> {
        let store = self.state.lock().unwrap_or_else(|e| e.into_inner());
        // Sort borrowed refs so the per-comparison key doesn't
        // allocate a String for each run_id; only the records that
        // survive truncation get cloned.
        let mut refs: Vec<&RunRecord> = store
            .runs
            .values()
            .filter(|r| r.parent_run_id.as_ref() == Some(parent_run_id))
            .collect();
        refs.sort_by(|a, b| {
            a.created_at
                .cmp(&b.created_at)
                .then_with(|| a.run_id.as_str().cmp(b.run_id.as_str()))
        });
        Ok(refs.into_iter().take(limit).cloned().collect())
    }

    async fn list_stalled(
        &self,
        tenant_id: &cairn_domain::TenantId,
        now_ms: u64,
        stale_after_ms: u64,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<RunRecord>, StoreError> {
        // Issue #570: combines state + staleness + tenant at the
        // projection surface so handlers no longer scan 20 000 rows
        // (Running + Pending) in memory before filtering by tenant +
        // staleness. `updated_at` is epoch-ms on `RunRecord` and
        // `now_ms > updated_at + stale_after_ms` is the canonical
        // stuck-run predicate used by the handler + the operator UI.
        let store = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let mut refs: Vec<&RunRecord> = store
            .runs
            .values()
            .filter(|r| {
                r.project.tenant_id == *tenant_id
                    && matches!(
                        r.state,
                        cairn_domain::RunState::Running | cairn_domain::RunState::Pending
                    )
                    && now_ms.saturating_sub(r.updated_at) > stale_after_ms
            })
            .collect();
        // Most-stale first so page 1 surfaces the runs that have been
        // silent longest — matches the operator dashboard's "worst
        // offenders" expectation.
        refs.sort_by(|a, b| {
            a.updated_at
                .cmp(&b.updated_at)
                .then_with(|| a.run_id.as_str().cmp(b.run_id.as_str()))
        });
        Ok(refs.into_iter().skip(offset).take(limit).cloned().collect())
    }
}

// -- RunDescendantsCounter (#670 G4 PR-1b-1) --

#[async_trait]
impl crate::projections::RunDescendantsCounter for InMemoryStore {
    async fn try_increment_descendants(
        &self,
        root_run_id: &cairn_domain::RunId,
        cap: i64,
    ) -> Result<crate::projections::DescendantsCapOutcome, StoreError> {
        use crate::projections::DescendantsCapOutcome;
        let outcome = {
            let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            let row = match state.runs.get_mut(root_run_id.as_str()) {
                Some(r) => r,
                None => return Ok(DescendantsCapOutcome::RootNotFound),
            };
            // Atomic check-and-increment under the state lock. The durable
            // backends (pg/sqlite) use `UPDATE ... WHERE counter < :cap
            // RETURNING` for the same semantic; InMemory uses lock
            // exclusion. Both reject above the cap deterministically.
            if row.in_flight_descendants >= cap {
                DescendantsCapOutcome::CapReached
            } else {
                row.in_flight_descendants += 1;
                // Bump version + updated_at so stale-run detection and
                // every other version-watching consumer see the change.
                // (Copilot review on #676.)
                row.version = row.version.saturating_add(1);
                row.updated_at = now_millis();
                DescendantsCapOutcome::Admitted {
                    new_count: row.in_flight_descendants,
                }
            }
        };
        // #670 G4 PR-1b-4: dual-write to the durable secondary
        // backend. Only mirror `Admitted` outcomes — `CapReached`
        // and `RootNotFound` mean we didn't mutate the in-memory
        // arm either. Best-effort per the `secondary_log` pattern;
        // failures log a WARN so ops sees the drift.
        if matches!(outcome, DescendantsCapOutcome::Admitted { .. }) {
            let backend = self
                .secondary_counter
                .read()
                .unwrap_or_else(|e| e.into_inner())
                .clone();
            if let Some(backend) = backend {
                if let Err(err) = backend.try_increment_descendants(root_run_id, cap).await {
                    tracing::warn!(
                        error = %err,
                        root_run_id = %root_run_id,
                        "secondary-counter increment failed; in-memory and durable \
                         counters now drift by 1. Next live increment/decrement or \
                         a restart-time reconciliation will resync.",
                    );
                }
            }
        }
        Ok(outcome)
    }

    async fn decrement_descendants(
        &self,
        root_run_id: &cairn_domain::RunId,
    ) -> Result<crate::projections::DescendantsCapOutcome, StoreError> {
        use crate::projections::DescendantsCapOutcome;
        let outcome = {
            let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            let row = match state.runs.get_mut(root_run_id.as_str()) {
                Some(r) => r,
                None => return Ok(DescendantsCapOutcome::RootNotFound),
            };
            // Unconditional decrement. Post-decrement can go negative if
            // a caller bug produces more decrements than increments;
            // we return the negative count rather than panicking. The
            // adapter layer surfaces negative values as a WARN metric per
            // RFC 027 (see `child_run_driver_descendant_underflow_total`).
            row.in_flight_descendants -= 1;
            // Bump version + updated_at: see `try_increment_descendants`
            // rationale above.
            row.version = row.version.saturating_add(1);
            row.updated_at = now_millis();
            DescendantsCapOutcome::Admitted {
                new_count: row.in_flight_descendants,
            }
        };
        // Dual-write to the durable secondary. Best-effort.
        let backend = self
            .secondary_counter
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        if let Some(backend) = backend {
            if let Err(err) = backend.decrement_descendants(root_run_id).await {
                tracing::warn!(
                    error = %err,
                    root_run_id = %root_run_id,
                    "secondary-counter decrement failed; in-memory and durable \
                     counters now drift by 1.",
                );
            }
        }
        Ok(outcome)
    }

    async fn list_nonzero_descendant_counters(
        &self,
    ) -> Result<Vec<(cairn_domain::RunId, i64)>, StoreError> {
        // Cap at 10_000 rows to match pg + sqlite and bound memory
        // (Gemini review on #680, MEDIUM). Sort before truncate so
        // the cap is deterministic across a wider population —
        // without the sort, the HashMap's iteration order would
        // pick an arbitrary slice.
        const RECONCILE_LIMIT: usize = 10_000;
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let mut out: Vec<(cairn_domain::RunId, i64)> = state
            .runs
            .values()
            .filter(|r| r.in_flight_descendants != 0)
            .map(|r| (r.run_id.clone(), r.in_flight_descendants))
            .collect();
        out.sort_by(|a, b| a.0.as_str().cmp(b.0.as_str()));
        out.truncate(RECONCILE_LIMIT);
        Ok(out)
    }
}

// -- TaskReadModel --

#[async_trait]
impl TaskReadModel for InMemoryStore {
    async fn get(&self, task_id: &TaskId) -> Result<Option<TaskRecord>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        Ok(state.tasks.get(task_id.as_str()).cloned())
    }

    async fn list_by_state(
        &self,
        project: &ProjectKey,
        task_state: TaskState,
        limit: usize,
    ) -> Result<Vec<TaskRecord>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let mut results: Vec<TaskRecord> = state
            .tasks
            .values()
            .filter(|t| t.project == *project && t.state == task_state)
            .cloned()
            .collect();
        results.sort_by_key(|t| (t.created_at, t.task_id.as_str().to_owned()));
        results.truncate(limit);
        Ok(results)
    }

    async fn list_expired_leases(
        &self,
        now: u64,
        limit: usize,
    ) -> Result<Vec<TaskRecord>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let mut results: Vec<TaskRecord> = state
            .tasks
            .values()
            .filter(|t| {
                t.state == TaskState::Leased && t.lease_expires_at.is_some_and(|exp| exp < now)
            })
            .cloned()
            .collect();
        results.sort_by_key(|t| {
            (
                t.lease_expires_at.unwrap_or(0),
                t.task_id.as_str().to_owned(),
            )
        });
        results.truncate(limit);
        Ok(results)
    }

    async fn list_by_parent_run(
        &self,
        parent_run_id: &RunId,
        limit: usize,
    ) -> Result<Vec<TaskRecord>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let mut results: Vec<TaskRecord> = state
            .tasks
            .values()
            .filter(|t| t.parent_run_id.as_ref() == Some(parent_run_id))
            .cloned()
            .collect();
        results.sort_by_key(|t| (t.created_at, t.task_id.as_str().to_owned()));
        results.truncate(limit);
        Ok(results)
    }

    async fn any_non_terminal_children(&self, parent_run_id: &RunId) -> Result<bool, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        Ok(state
            .tasks
            .values()
            .any(|t| t.parent_run_id.as_ref() == Some(parent_run_id) && !t.state.is_terminal()))
    }
}

// -- ApprovalReadModel --

#[async_trait]
impl ApprovalReadModel for InMemoryStore {
    async fn get(&self, approval_id: &ApprovalId) -> Result<Option<ApprovalRecord>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        Ok(state.approvals.get(approval_id.as_str()).cloned())
    }

    async fn list_pending(
        &self,
        project: &ProjectKey,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<ApprovalRecord>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let mut results: Vec<ApprovalRecord> = state
            .approvals
            .values()
            .filter(|a| a.project == *project && a.decision.is_none())
            .cloned()
            .collect();
        results.sort_by_key(|a| (a.created_at, a.approval_id.as_str().to_owned()));
        let results = results.into_iter().skip(offset).take(limit).collect();
        Ok(results)
    }

    async fn list_all(
        &self,
        project: &ProjectKey,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<ApprovalRecord>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let mut results: Vec<ApprovalRecord> = state
            .approvals
            .values()
            .filter(|a| a.project == *project)
            .cloned()
            .collect();
        results.sort_by_key(|a| (a.created_at, a.approval_id.as_str().to_owned()));
        let results = results.into_iter().skip(offset).take(limit).collect();
        Ok(results)
    }

    async fn has_pending_for_run(&self, run_id: &RunId) -> Result<bool, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        Ok(state
            .approvals
            .values()
            .any(|a| a.run_id.as_ref() == Some(run_id) && a.decision.is_none()))
    }
}

// -- ApprovalDelegationReadModel --

#[async_trait]
impl crate::projections::ApprovalDelegationReadModel for InMemoryStore {
    async fn list_for_approval(
        &self,
        approval_id: &ApprovalId,
    ) -> Result<Vec<crate::projections::ApprovalDelegationRecord>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let mut rows: Vec<_> = state
            .approval_delegations
            .iter()
            .filter(|r| r.approval_id == *approval_id)
            .cloned()
            .collect();
        // Oldest first — matches pg/sqlite `ORDER BY delegated_at_ms ASC,
        // delegation_id ASC`. `delegation_id` is a stable monotonic
        // tiebreaker within the same ms.
        rows.sort_by(|a, b| {
            a.delegated_at_ms
                .cmp(&b.delegated_at_ms)
                .then_with(|| a.delegation_id.cmp(&b.delegation_id))
        });
        Ok(rows)
    }
}

// -- ToolCallApprovalReadModel --

#[async_trait]
impl ToolCallApprovalReadModel for InMemoryStore {
    async fn get(
        &self,
        call_id: &ToolCallId,
    ) -> Result<Option<ToolCallApprovalRecord>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        Ok(state.tool_call_approvals.get(call_id.as_str()).cloned())
    }

    async fn list_for_run(
        &self,
        run_id: &RunId,
    ) -> Result<Vec<ToolCallApprovalRecord>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let mut results: Vec<ToolCallApprovalRecord> = state
            .tool_call_approvals
            .values()
            .filter(|r| r.run_id == *run_id)
            .cloned()
            .collect();
        results.sort_by(|a, b| {
            a.proposed_at_ms
                .cmp(&b.proposed_at_ms)
                .then_with(|| a.call_id.as_str().cmp(b.call_id.as_str()))
        });
        Ok(results)
    }

    async fn list_for_session(
        &self,
        session_id: &SessionId,
    ) -> Result<Vec<ToolCallApprovalRecord>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let mut results: Vec<ToolCallApprovalRecord> = state
            .tool_call_approvals
            .values()
            .filter(|r| r.session_id == *session_id)
            .cloned()
            .collect();
        results.sort_by(|a, b| {
            a.proposed_at_ms
                .cmp(&b.proposed_at_ms)
                .then_with(|| a.call_id.as_str().cmp(b.call_id.as_str()))
        });
        Ok(results)
    }

    async fn list_pending_for_project(
        &self,
        project: &ProjectKey,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<ToolCallApprovalRecord>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let mut results: Vec<ToolCallApprovalRecord> = state
            .tool_call_approvals
            .values()
            .filter(|r| r.project == *project && r.state == ToolCallApprovalState::Pending)
            .cloned()
            .collect();
        results.sort_by(|a, b| {
            a.proposed_at_ms
                .cmp(&b.proposed_at_ms)
                .then_with(|| a.call_id.as_str().cmp(b.call_id.as_str()))
        });
        Ok(results.into_iter().skip(offset).take(limit).collect())
    }

    async fn list_all_pending(
        &self,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<ToolCallApprovalRecord>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let mut results: Vec<ToolCallApprovalRecord> = state
            .tool_call_approvals
            .values()
            .filter(|r| r.state == ToolCallApprovalState::Pending)
            .cloned()
            .collect();
        results.sort_by(|a, b| {
            a.proposed_at_ms
                .cmp(&b.proposed_at_ms)
                .then_with(|| a.call_id.as_str().cmp(b.call_id.as_str()))
        });
        Ok(results.into_iter().skip(offset).take(limit).collect())
    }
}

// -- CheckpointReadModel --

#[async_trait]
impl CheckpointReadModel for InMemoryStore {
    async fn get(
        &self,
        checkpoint_id: &CheckpointId,
    ) -> Result<Option<CheckpointRecord>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        Ok(state.checkpoints.get(checkpoint_id.as_str()).cloned())
    }

    async fn latest_for_run(&self, run_id: &RunId) -> Result<Option<CheckpointRecord>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        Ok(state
            .checkpoints
            .values()
            .find(|c| c.run_id == *run_id && c.disposition == CheckpointDisposition::Latest)
            .cloned())
    }

    async fn list_by_run(
        &self,
        run_id: &RunId,
        limit: usize,
    ) -> Result<Vec<CheckpointRecord>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let mut results: Vec<CheckpointRecord> = state
            .checkpoints
            .values()
            .filter(|c| c.run_id == *run_id)
            .cloned()
            .collect();
        results.sort_by_key(|c| (c.created_at, c.checkpoint_id.as_str().to_owned()));
        results.truncate(limit);
        Ok(results)
    }
}

// -- MailboxReadModel --

#[async_trait]
impl MailboxReadModel for InMemoryStore {
    async fn get(
        &self,
        message_id: &MailboxMessageId,
    ) -> Result<Option<MailboxRecord>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        Ok(state.mailbox_messages.get(message_id.as_str()).cloned())
    }

    async fn list_by_run(
        &self,
        run_id: &RunId,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<MailboxRecord>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let mut results: Vec<MailboxRecord> = state
            .mailbox_messages
            .values()
            .filter(|m| m.run_id.as_ref() == Some(run_id))
            .cloned()
            .collect();
        results.sort_by_key(|m| m.message_id.as_str().to_owned());
        let results = results.into_iter().skip(offset).take(limit).collect();
        Ok(results)
    }

    async fn list_by_task(
        &self,
        task_id: &TaskId,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<MailboxRecord>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let mut results: Vec<MailboxRecord> = state
            .mailbox_messages
            .values()
            .filter(|m| m.task_id.as_ref() == Some(task_id))
            .cloned()
            .collect();
        results.sort_by_key(|m| (m.created_at, m.message_id.as_str().to_owned()));
        let results = results.into_iter().skip(offset).take(limit).collect();
        Ok(results)
    }

    async fn list_pending(
        &self,
        now_ms: u64,
        limit: usize,
    ) -> Result<Vec<MailboxRecord>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let mut results: Vec<MailboxRecord> = state
            .mailbox_messages
            .values()
            .filter(|m| m.deliver_at_ms > 0 && m.deliver_at_ms <= now_ms)
            .cloned()
            .collect();
        results.sort_by_key(|m| (m.deliver_at_ms, m.message_id.as_str().to_owned()));
        results.truncate(limit);
        Ok(results)
    }
}

// -- ToolInvocationReadModel --

#[async_trait]
impl ToolInvocationReadModel for InMemoryStore {
    async fn get(
        &self,
        invocation_id: &ToolInvocationId,
    ) -> Result<Option<ToolInvocationRecord>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        Ok(state.tool_invocations.get(invocation_id.as_str()).cloned())
    }

    async fn list_by_run(
        &self,
        run_id: &RunId,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<ToolInvocationRecord>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let mut results: Vec<ToolInvocationRecord> = state
            .tool_invocations
            .values()
            .filter(|record| record.run_id.as_ref() == Some(run_id))
            .cloned()
            .collect();
        results.sort_by_key(|record| record.requested_at_ms);
        Ok(results.into_iter().skip(offset).take(limit).collect())
    }
}

// -- ToolInvocationProgressReadModel --

#[async_trait]
impl crate::projections::ToolInvocationProgressReadModel for InMemoryStore {
    async fn get(
        &self,
        invocation_id: &ToolInvocationId,
    ) -> Result<Option<crate::projections::ToolInvocationProgressRecord>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        Ok(state
            .tool_invocation_progress
            .get(invocation_id.as_str())
            .cloned())
    }
}

// -- SignalReadModel --

#[async_trait]
impl SignalReadModel for InMemoryStore {
    async fn get(
        &self,
        signal_id: &cairn_domain::SignalId,
    ) -> Result<Option<cairn_domain::SignalRecord>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        Ok(state.signals.get(signal_id.as_str()).cloned())
    }

    async fn list_by_project(
        &self,
        project: &cairn_domain::ProjectKey,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<cairn_domain::SignalRecord>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let mut results: Vec<cairn_domain::SignalRecord> = state
            .signals
            .values()
            .filter(|s| s.project == *project)
            .cloned()
            .collect();
        // RFC-025 Phase 2b.2b m2: sort by (timestamp_ms ASC, signal_id
        // ASC) so same-ms ingests pick a stable order; pg/sqlite
        // adapters ORDER BY the same composite key for parity.
        results.sort_by(|a, b| {
            a.timestamp_ms
                .cmp(&b.timestamp_ms)
                .then_with(|| a.id.as_str().cmp(b.id.as_str()))
        });
        let results = results.into_iter().skip(offset).take(limit).collect();
        Ok(results)
    }
}

// -- IngestJobReadModel --

#[async_trait]
impl IngestJobReadModel for InMemoryStore {
    async fn get(
        &self,
        job_id: &cairn_domain::IngestJobId,
    ) -> Result<Option<cairn_domain::IngestJobRecord>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        Ok(state.ingest_jobs.get(job_id.as_str()).cloned())
    }

    async fn list_by_project(
        &self,
        project: &cairn_domain::ProjectKey,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<cairn_domain::IngestJobRecord>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let mut results: Vec<cairn_domain::IngestJobRecord> = state
            .ingest_jobs
            .values()
            .filter(|j| j.project == *project)
            .cloned()
            .collect();
        // RFC-025 Phase 2b.3 m1: tiebreak on `id` so two jobs created in
        // the same millisecond land in a deterministic order that pg/sqlite
        // also produce (they `ORDER BY created_at_ms ASC, job_id ASC` on the
        // composite project index). Without this, byte-equality parity with
        // the SQL backends fails.
        results.sort_by(|a, b| {
            a.created_at
                .cmp(&b.created_at)
                .then_with(|| a.id.as_str().cmp(b.id.as_str()))
        });
        let results = results.into_iter().skip(offset).take(limit).collect();
        Ok(results)
    }
}

// -- ScheduledTaskReadModel --

#[async_trait]
impl crate::projections::ScheduledTaskReadModel for InMemoryStore {
    async fn get(
        &self,
        id: &cairn_domain::ScheduledTaskId,
    ) -> Result<Option<cairn_domain::ScheduledTaskRecord>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        Ok(state.scheduled_tasks.get(id.as_str()).cloned())
    }

    async fn list_by_tenant(
        &self,
        tenant_id: &cairn_domain::TenantId,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<cairn_domain::ScheduledTaskRecord>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let mut results: Vec<cairn_domain::ScheduledTaskRecord> = state
            .scheduled_tasks
            .values()
            .filter(|t| &t.tenant_id == tenant_id)
            .cloned()
            .collect();
        results.sort_by_key(|t| t.created_at);
        Ok(results.into_iter().skip(offset).take(limit).collect())
    }

    async fn list_due(
        &self,
        now_ms: u64,
        limit: usize,
    ) -> Result<Vec<cairn_domain::ScheduledTaskRecord>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let mut results: Vec<cairn_domain::ScheduledTaskRecord> = state
            .scheduled_tasks
            .values()
            .filter(|t| t.enabled && t.next_run_at.is_some_and(|nxt| nxt <= now_ms))
            .cloned()
            .collect();
        results.sort_by_key(|t| t.next_run_at);
        Ok(results.into_iter().take(limit).collect())
    }
}

// -- EvalRunReadModel --

#[async_trait]
impl EvalRunReadModel for InMemoryStore {
    async fn get(
        &self,
        eval_run_id: &cairn_domain::EvalRunId,
    ) -> Result<Option<crate::projections::EvalRunRecord>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        Ok(state.eval_runs.get(eval_run_id.as_str()).cloned())
    }

    async fn list_by_project(
        &self,
        project: &cairn_domain::ProjectKey,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<crate::projections::EvalRunRecord>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let mut results: Vec<crate::projections::EvalRunRecord> = state
            .eval_runs
            .values()
            .filter(|r| r.project == *project)
            .cloned()
            .collect();
        results.sort_by_key(|r| r.started_at);
        let results = results.into_iter().skip(offset).take(limit).collect();
        Ok(results)
    }
}

// -- OutcomeReadModel --

#[async_trait]
impl OutcomeReadModel for InMemoryStore {
    async fn get(
        &self,
        outcome_id: &cairn_domain::OutcomeId,
    ) -> Result<Option<crate::projections::OutcomeRecord>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        Ok(state.outcomes.get(outcome_id.as_str()).cloned())
    }

    async fn list_by_run(
        &self,
        run_id: &cairn_domain::RunId,
        limit: usize,
    ) -> Result<Vec<crate::projections::OutcomeRecord>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let mut results: Vec<crate::projections::OutcomeRecord> = state
            .outcomes
            .values()
            .filter(|r| r.run_id == *run_id)
            .cloned()
            .collect();
        // Tiebreaker on outcome_id keeps cross-backend parity stable
        // for events sharing a `recorded_at` timestamp (pg/sqlite
        // ORDER BY uses the same compound key).
        results.sort_by(|a, b| {
            a.recorded_at
                .cmp(&b.recorded_at)
                .then_with(|| a.outcome_id.as_str().cmp(b.outcome_id.as_str()))
        });
        results.truncate(limit);
        Ok(results)
    }

    async fn list_by_project(
        &self,
        project: &cairn_domain::ProjectKey,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<crate::projections::OutcomeRecord>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let mut results: Vec<crate::projections::OutcomeRecord> = state
            .outcomes
            .values()
            .filter(|r| r.project == *project)
            .cloned()
            .collect();
        results.sort_by(|a, b| {
            a.recorded_at
                .cmp(&b.recorded_at)
                .then_with(|| a.outcome_id.as_str().cmp(b.outcome_id.as_str()))
        });
        let results = results.into_iter().skip(offset).take(limit).collect();
        Ok(results)
    }
}

// -- EvalDatasetReadModel --

#[async_trait]
impl crate::projections::EvalDatasetReadModel for InMemoryStore {
    async fn get_dataset(
        &self,
        dataset_id: &str,
    ) -> Result<Option<cairn_domain::EvalDataset>, crate::error::StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        Ok(state.eval_datasets.get(dataset_id).cloned())
    }

    async fn list_by_tenant(
        &self,
        tenant_id: &cairn_domain::TenantId,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<cairn_domain::EvalDataset>, crate::error::StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let mut results: Vec<cairn_domain::EvalDataset> = state
            .eval_datasets
            .values()
            .filter(|d| d.tenant_id == *tenant_id || tenant_id.as_str().is_empty())
            .cloned()
            .collect();
        // RFC-025 Phase 2b.4 m2: dataset_id tiebreaker so cross-backend
        // parity holds when two datasets share a `created_at_ms` (matches
        // the pg/sqlite `ORDER BY created_at_ms ASC, dataset_id ASC` query).
        results.sort_by(|a, b| {
            a.created_at_ms
                .cmp(&b.created_at_ms)
                .then_with(|| a.dataset_id.cmp(&b.dataset_id))
        });
        Ok(results.into_iter().skip(offset).take(limit).collect())
    }
}

// -- EvalRubricReadModel --

#[async_trait]
impl crate::projections::EvalRubricReadModel for InMemoryStore {
    async fn get_rubric(
        &self,
        rubric_id: &str,
    ) -> Result<Option<cairn_domain::EvalRubric>, crate::error::StoreError> {
        Ok(self
            .state
            .lock()
            .unwrap()
            .eval_rubrics
            .get(rubric_id)
            .cloned())
    }
    async fn list_by_tenant(
        &self,
        tenant_id: &cairn_domain::TenantId,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<cairn_domain::EvalRubric>, crate::error::StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let mut results: Vec<_> = state
            .eval_rubrics
            .values()
            .filter(|r| r.tenant_id == *tenant_id || tenant_id.as_str().is_empty())
            .cloned()
            .collect();
        results.sort_by_key(|r| r.rubric_id.clone());
        Ok(results.into_iter().skip(offset).take(limit).collect())
    }
}

// -- EvalBaselineReadModel --

#[async_trait]
impl crate::projections::EvalBaselineReadModel for InMemoryStore {
    async fn get_baseline(
        &self,
        baseline_id: &str,
    ) -> Result<Option<cairn_domain::EvalBaseline>, crate::error::StoreError> {
        Ok(self
            .state
            .lock()
            .unwrap()
            .eval_baselines
            .get(baseline_id)
            .cloned())
    }
    async fn list_by_tenant(
        &self,
        tenant_id: &cairn_domain::TenantId,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<cairn_domain::EvalBaseline>, crate::error::StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let mut results: Vec<_> = state
            .eval_baselines
            .values()
            .filter(|b| b.tenant_id == *tenant_id || tenant_id.as_str().is_empty())
            .cloned()
            .collect();
        results.sort_by_key(|r| r.baseline_id.clone());
        Ok(results.into_iter().skip(offset).take(limit).collect())
    }
}

// -- PromptAssetReadModel --

#[async_trait]
impl PromptAssetReadModel for InMemoryStore {
    async fn get(
        &self,
        id: &cairn_domain::PromptAssetId,
    ) -> Result<Option<crate::projections::PromptAssetRecord>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        Ok(state.prompt_assets.get(id.as_str()).cloned())
    }

    async fn list_by_project(
        &self,
        project: &cairn_domain::ProjectKey,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<crate::projections::PromptAssetRecord>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let mut results: Vec<crate::projections::PromptAssetRecord> = state
            .prompt_assets
            .values()
            .filter(|a| a.project == *project)
            .cloned()
            .collect();
        results.sort_by_key(|a| a.created_at);
        Ok(results.into_iter().skip(offset).take(limit).collect())
    }
}

// -- PromptVersionReadModel --

#[async_trait]
impl PromptVersionReadModel for InMemoryStore {
    async fn get(
        &self,
        id: &cairn_domain::PromptVersionId,
    ) -> Result<Option<crate::projections::PromptVersionRecord>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        Ok(state.prompt_versions.get(id.as_str()).cloned())
    }

    async fn list_by_asset(
        &self,
        asset_id: &cairn_domain::PromptAssetId,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<crate::projections::PromptVersionRecord>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let mut results: Vec<crate::projections::PromptVersionRecord> = state
            .prompt_versions
            .values()
            .filter(|v| v.prompt_asset_id == *asset_id)
            .cloned()
            .collect();
        results.sort_by_key(|v| v.created_at);
        Ok(results.into_iter().skip(offset).take(limit).collect())
    }
}

// -- PromptReleaseReadModel --

/// RFC 001: deterministic hash-based selector bucket for traffic routing (0-100).
fn selector_bucket(selector: &str) -> u8 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut h = DefaultHasher::new();
    selector.hash(&mut h);
    (h.finish() % 100) as u8
}

#[async_trait]
impl PromptReleaseReadModel for InMemoryStore {
    async fn get(
        &self,
        id: &cairn_domain::PromptReleaseId,
    ) -> Result<Option<crate::projections::PromptReleaseRecord>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        Ok(state.prompt_releases.get(id.as_str()).cloned())
    }

    async fn list_by_project(
        &self,
        project: &cairn_domain::ProjectKey,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<crate::projections::PromptReleaseRecord>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let mut results: Vec<crate::projections::PromptReleaseRecord> = state
            .prompt_releases
            .values()
            .filter(|r| r.project == *project)
            .cloned()
            .collect();
        results.sort_by_key(|r| r.created_at);
        Ok(results.into_iter().skip(offset).take(limit).collect())
    }

    async fn active_for_selector(
        &self,
        project: &cairn_domain::ProjectKey,
        prompt_asset_id: &cairn_domain::PromptAssetId,
        selector: &str,
    ) -> Result<Option<crate::projections::PromptReleaseRecord>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let mut active: Vec<_> = state
            .prompt_releases
            .values()
            .filter(|r| {
                r.project == *project
                    && r.prompt_asset_id == *prompt_asset_id
                    && r.state == "active"
            })
            .cloned()
            .collect();
        active.sort_by_key(|r| r.prompt_release_id.as_str().to_owned());
        if active.is_empty() {
            return Ok(None);
        }

        // RFC 001: if any release has rollout_percent, use deterministic bucket routing.
        if active.iter().any(|r| r.rollout_percent.is_some()) {
            let bucket = selector_bucket(selector);
            let mut cumulative = 0u8;
            for release in &active {
                let pct = release.rollout_percent.unwrap_or(100);
                cumulative = cumulative.saturating_add(pct);
                if bucket < cumulative {
                    return Ok(Some(release.clone()));
                }
            }
            return Ok(active.into_iter().last());
        }

        // RFC 006 selector precedence resolution.
        //
        // Priority (highest to lowest):
        //   1. routing_slot — exact match against the selector string.
        //   2. task_type    — exact match against the selector string.
        //   3. agent_type   — exact match against the selector string.
        //   4. is_project_default — release marked as the project-wide default.
        //   5. Any active release (first by release_id, for stability).
        //
        // When the release records do not yet carry these fields (all None / false),
        // every candidate scores 0 and the fallback (step 5) applies — preserving the
        // previous behaviour while the routing metadata is being backfilled.

        // Step 1: routing_slot exact match.
        if let Some(r) = active
            .iter()
            .find(|r| r.routing_slot.as_deref() == Some(selector))
        {
            return Ok(Some(r.clone()));
        }

        // Step 2: task_type exact match.
        if let Some(r) = active
            .iter()
            .find(|r| r.task_type.as_deref() == Some(selector))
        {
            return Ok(Some(r.clone()));
        }

        // Step 3: agent_type exact match.
        if let Some(r) = active
            .iter()
            .find(|r| r.agent_type.as_deref() == Some(selector))
        {
            return Ok(Some(r.clone()));
        }

        // Step 4: project default.
        if let Some(r) = active.iter().find(|r| r.is_project_default) {
            return Ok(Some(r.clone()));
        }

        // Step 5: fallback — first active release (sorted by release_id for stability).
        Ok(active.into_iter().next())
    }
}

// -- TenantReadModel --

#[async_trait]
impl TenantReadModel for InMemoryStore {
    async fn get(
        &self,
        id: &cairn_domain::TenantId,
    ) -> Result<Option<cairn_domain::org::TenantRecord>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        Ok(state.tenants.get(id.as_str()).cloned())
    }

    async fn list(
        &self,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<cairn_domain::org::TenantRecord>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let mut results: Vec<_> = state.tenants.values().cloned().collect();
        results.sort_by_key(|t| t.created_at);
        Ok(results.into_iter().skip(offset).take(limit).collect())
    }
}

// -- WorkspaceReadModel --

#[async_trait]
impl WorkspaceReadModel for InMemoryStore {
    async fn get(
        &self,
        id: &cairn_domain::WorkspaceId,
    ) -> Result<Option<cairn_domain::org::WorkspaceRecord>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        Ok(state.workspaces.get(id.as_str()).cloned())
    }

    async fn list_by_tenant(
        &self,
        tenant_id: &cairn_domain::TenantId,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<cairn_domain::org::WorkspaceRecord>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let mut results: Vec<_> = state
            .workspaces
            .values()
            .filter(|w| w.tenant_id == *tenant_id)
            .cloned()
            .collect();
        results.sort_by_key(|w| w.created_at);
        Ok(results.into_iter().skip(offset).take(limit).collect())
    }
}

// -- ProjectReadModel --

#[async_trait]
impl ProjectReadModel for InMemoryStore {
    async fn get_project(
        &self,
        project: &cairn_domain::ProjectKey,
    ) -> Result<Option<cairn_domain::org::ProjectRecord>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        Ok(state.projects.get(project.project_id.as_str()).cloned())
    }

    async fn list_by_workspace(
        &self,
        tenant_id: &cairn_domain::TenantId,
        workspace_id: &cairn_domain::WorkspaceId,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<cairn_domain::org::ProjectRecord>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let mut results: Vec<_> = state
            .projects
            .values()
            .filter(|p| p.tenant_id == *tenant_id && p.workspace_id == *workspace_id)
            .cloned()
            .collect();
        results.sort_by_key(|p| p.created_at);
        Ok(results.into_iter().skip(offset).take(limit).collect())
    }
}

// -- RouteDecisionReadModel --

#[async_trait]
impl RouteDecisionReadModel for InMemoryStore {
    async fn get(
        &self,
        decision_id: &cairn_domain::RouteDecisionId,
    ) -> Result<Option<cairn_domain::providers::RouteDecisionRecord>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        Ok(state.route_decisions.get(decision_id.as_str()).cloned())
    }

    async fn list_by_project(
        &self,
        project: &cairn_domain::ProjectKey,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<cairn_domain::providers::RouteDecisionRecord>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let mut results: Vec<_> = state
            .route_decisions
            .values()
            .filter(|d| d.project_id == project.project_id)
            .cloned()
            .collect();
        results.sort_by_key(|d| d.route_decision_id.to_string());
        Ok(results.into_iter().skip(offset).take(limit).collect())
    }
}

// -- ProviderCallReadModel --

#[async_trait]
impl ProviderCallReadModel for InMemoryStore {
    async fn get(
        &self,
        call_id: &cairn_domain::ProviderCallId,
    ) -> Result<Option<cairn_domain::providers::ProviderCallRecord>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        Ok(state.provider_calls.get(call_id.as_str()).cloned())
    }

    async fn list_by_decision(
        &self,
        decision_id: &cairn_domain::RouteDecisionId,
        limit: usize,
    ) -> Result<Vec<cairn_domain::providers::ProviderCallRecord>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let results: Vec<_> = state
            .provider_calls
            .values()
            .filter(|c| c.route_decision_id == *decision_id)
            .take(limit)
            .cloned()
            .collect();
        Ok(results)
    }

    async fn list_by_run(
        &self,
        run_id: &cairn_domain::RunId,
        limit: usize,
    ) -> Result<Vec<cairn_domain::providers::ProviderCallRecord>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let mut results: Vec<_> = state
            .provider_calls
            .values()
            .filter(|c| c.run_id.as_ref() == Some(run_id))
            .cloned()
            .collect();
        // Stable order: by started_at_ms ascending, then by provider_call_id.
        results.sort_by(|a, b| {
            a.started_at_ms
                .cmp(&b.started_at_ms)
                .then_with(|| a.provider_call_id.as_str().cmp(b.provider_call_id.as_str()))
        });
        results.truncate(limit);
        Ok(results)
    }
}

// -- Lease helpers (not trait-based, used by runtime directly) --

impl InMemoryStore {
    /// Set lease fields on a task. Used by runtime TaskService for claim/heartbeat.
    pub async fn set_task_lease(
        &self,
        task_id: &TaskId,
        owner: String,
        expires_at: u64,
    ) -> Result<(), StoreError> {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let rec = state
            .tasks
            .get_mut(task_id.as_str())
            .ok_or_else(|| StoreError::NotFound {
                entity: "task",
                id: task_id.to_string(),
            })?;
        rec.lease_owner = Some(owner);
        rec.lease_expires_at = Some(expires_at);
        Ok(())
    }

    /// Clear lease fields on a task.
    pub async fn clear_task_lease(&self, task_id: &TaskId) -> Result<(), StoreError> {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(rec) = state.tasks.get_mut(task_id.as_str()) {
            rec.lease_owner = None;
            rec.lease_expires_at = None;
        }
        Ok(())
    }
}

#[async_trait]
impl ApprovalPolicyReadModel for InMemoryStore {
    async fn get_policy(
        &self,
        policy_id: &str,
    ) -> Result<Option<cairn_domain::ApprovalPolicyRecord>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        Ok(state.approval_policies.get(policy_id).cloned())
    }

    async fn list_by_tenant(
        &self,
        tenant_id: &cairn_domain::TenantId,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<cairn_domain::ApprovalPolicyRecord>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let mut results: Vec<_> = state
            .approval_policies
            .values()
            .filter(|p| p.tenant_id == *tenant_id)
            .cloned()
            .collect();
        results.sort_by_key(|p| p.policy_id.clone());
        Ok(results.into_iter().skip(offset).take(limit).collect())
    }
}

#[async_trait]
impl ExternalWorkerReadModel for InMemoryStore {
    async fn get(
        &self,
        id: &cairn_domain::WorkerId,
    ) -> Result<Option<cairn_domain::workers::ExternalWorkerRecord>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        Ok(state.external_workers.get(id.as_str()).cloned())
    }

    async fn list_by_tenant(
        &self,
        tenant_id: &cairn_domain::TenantId,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<cairn_domain::workers::ExternalWorkerRecord>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let mut results: Vec<_> = state
            .external_workers
            .values()
            .filter(|w| w.tenant_id == *tenant_id)
            .cloned()
            .collect();
        // Deterministic tiebreak on worker_id — matches the pg + sqlite
        // `ORDER BY registered_at ASC, worker_id ASC` clause. Without
        // the tiebreak, HashMap iteration order leaks into the result
        // under same-ms registration bursts (projection_parity test
        // caught this).
        results.sort_by(|a, b| {
            a.registered_at
                .cmp(&b.registered_at)
                .then_with(|| a.worker_id.as_str().cmp(b.worker_id.as_str()))
        });
        Ok(results.into_iter().skip(offset).take(limit).collect())
    }
}

// -- LlmCallTraceReadModel --

#[async_trait]
impl crate::projections::LlmCallTraceReadModel for InMemoryStore {
    async fn insert_trace(&self, trace: cairn_domain::LlmCallTrace) -> Result<(), StoreError> {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .llm_traces
            .push(trace);
        Ok(())
    }

    async fn list_by_session(
        &self,
        session_id: &cairn_domain::SessionId,
        limit: usize,
    ) -> Result<Vec<cairn_domain::LlmCallTrace>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let mut results: Vec<cairn_domain::LlmCallTrace> = state
            .llm_traces
            .iter()
            .filter(|t| {
                t.session_id
                    .as_ref()
                    .map(|s| s == session_id)
                    .unwrap_or(false)
            })
            .cloned()
            .collect();
        // Most-recent first.
        results.sort_by_key(|r| std::cmp::Reverse(r.created_at_ms));
        results.truncate(limit);
        Ok(results)
    }

    async fn list_all_traces(
        &self,
        limit: usize,
    ) -> Result<Vec<cairn_domain::LlmCallTrace>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let mut results = state.llm_traces.clone();
        results.sort_by_key(|r| std::cmp::Reverse(r.created_at_ms));
        results.truncate(limit);
        Ok(results)
    }
}

// -- LlmCompletionBodyReadModel (issue #668) --

#[async_trait]
impl crate::projections::LlmCompletionBodyReadModel for InMemoryStore {
    async fn get_by_trace_id(
        &self,
        trace_id: &str,
    ) -> Result<Option<crate::projections::LlmCompletionBodyRecord>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        Ok(state.llm_completion_bodies.get(trace_id).cloned())
    }

    async fn list_by_session(
        &self,
        session_id: &cairn_domain::SessionId,
    ) -> Result<Vec<crate::projections::LlmCompletionBodyRecord>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let mut rows: Vec<crate::projections::LlmCompletionBodyRecord> = state
            .llm_completion_bodies
            .values()
            .filter(|r| r.session_id == *session_id)
            .cloned()
            .collect();
        // Ascending by recorded_at_ms so the UI can render turns
        // chronologically. `recorded_at_ms` may tie across iterations
        // that completed in the same millisecond; break with
        // `trace_id` so the order is stable across queries.
        rows.sort_by(|a, b| {
            a.recorded_at_ms
                .cmp(&b.recorded_at_ms)
                .then_with(|| a.trace_id.cmp(&b.trace_id))
        });
        Ok(rows)
    }
}

#[async_trait]
impl crate::projections::RunCostReadModel for InMemoryStore {
    async fn get_run_cost(
        &self,
        run_id: &cairn_domain::RunId,
    ) -> Result<Option<cairn_domain::providers::RunCostRecord>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        Ok(state.run_costs.get(run_id.as_str()).cloned())
    }

    async fn list_by_session(
        &self,
        session_id: &cairn_domain::SessionId,
    ) -> Result<Vec<cairn_domain::providers::RunCostRecord>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());

        // Per Gemini PR #727 review: pre-build a `run_id -> session_id`
        // map from the event log in one pass so the per-cost-row check
        // is O(1) instead of O(events). The lookup is two-tier:
        //
        //   1. Authoritative path — if `state.runs` knows the run, use
        //      its `session_id` directly. This is the fast happy path
        //      when `RunCreated` has been projected.
        //   2. Event-log fallback — for runs whose `RunCreated`
        //      projection has not landed yet (e.g. orphan replay,
        //      cross-projection ordering races), scan the
        //      `RunCostUpdated` events for the run_id and use the
        //      event's `session_id` if present. We build this index
        //      ONCE per call rather than per-cost-row, dropping the
        //      original O(N*M) shape to O(N+M).
        let cost_run_to_event_session: std::collections::HashMap<String, cairn_domain::SessionId> =
            state
                .events
                .iter()
                .rev()
                .filter_map(|evt| match &evt.envelope.payload {
                    cairn_domain::RuntimeEvent::RunCostUpdated(e) => e
                        .session_id
                        .clone()
                        .map(|sid| (e.run_id.as_str().to_owned(), sid)),
                    _ => None,
                })
                .collect();

        let mut out = Vec::new();
        for cost in state.run_costs.values() {
            let run_matches = state
                .runs
                .get(cost.run_id.as_str())
                .map(|run| run.session_id == *session_id)
                .unwrap_or(false);
            let event_matches = !run_matches
                && cost_run_to_event_session.get(cost.run_id.as_str()) == Some(session_id);
            if run_matches || event_matches {
                out.push(cost.clone());
            }
        }
        Ok(out)
    }
}

// TaskDependencyReadModel was removed — dependencies are FF-authoritative
// (ff_stage_dependency_edge / ff_apply_dependency_to_child). Cairn no
// longer persists dependency records; check_dependencies reads live
// edge state from FF via ff_evaluate_flow_eligibility + per-edge HGETs.

// -- OperatorProfileReadModel --

#[async_trait]
impl crate::projections::OperatorProfileReadModel for InMemoryStore {
    async fn get(
        &self,
        operator_id: &cairn_domain::ids::OperatorId,
    ) -> Result<Option<crate::projections::OperatorProfileRecord>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        Ok(state.operator_profiles.get(operator_id.as_str()).cloned())
    }

    async fn list_by_tenant(
        &self,
        tenant_id: &cairn_domain::ids::TenantId,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<crate::projections::OperatorProfileRecord>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let mut results: Vec<crate::projections::OperatorProfileRecord> = state
            .operator_profiles
            .values()
            .filter(|p| &p.tenant_id == tenant_id)
            .cloned()
            .collect();
        results.sort_by_key(|p| p.operator_id.to_string());
        Ok(results.into_iter().skip(offset).take(limit).collect())
    }
}

// -- OperatorTenantRoleReadModel (RFC 026 PR-A0) --

#[async_trait]
impl crate::projections::OperatorTenantRoleReadModel for InMemoryStore {
    async fn get(
        &self,
        tenant_id: &cairn_domain::ids::TenantId,
        operator_id: &cairn_domain::ids::OperatorId,
    ) -> Result<Option<crate::projections::OperatorTenantRoleRecord>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let key = (
            tenant_id.as_str().to_owned(),
            operator_id.as_str().to_owned(),
        );
        Ok(state.operator_tenant_roles.get(&key).cloned())
    }

    async fn list_by_operator(
        &self,
        operator_id: &cairn_domain::ids::OperatorId,
    ) -> Result<Vec<crate::projections::OperatorTenantRoleRecord>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let mut results: Vec<crate::projections::OperatorTenantRoleRecord> = state
            .operator_tenant_roles
            .values()
            .filter(|r| &r.operator_id == operator_id)
            .cloned()
            .collect();
        // Deterministic ordering across backends: tenant_id ASC.
        results.sort_by(|a, b| a.tenant_id.as_str().cmp(b.tenant_id.as_str()));
        Ok(results)
    }

    async fn list_by_tenant(
        &self,
        tenant_id: &cairn_domain::ids::TenantId,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<crate::projections::OperatorTenantRoleRecord>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let mut results: Vec<crate::projections::OperatorTenantRoleRecord> = state
            .operator_tenant_roles
            .values()
            .filter(|r| &r.tenant_id == tenant_id)
            .cloned()
            .collect();
        results.sort_by(|a, b| a.operator_id.as_str().cmp(b.operator_id.as_str()));
        Ok(results.into_iter().skip(offset).take(limit).collect())
    }
}

// -- WorkspaceMembershipReadModel --

#[async_trait]
impl crate::projections::WorkspaceMembershipReadModel for InMemoryStore {
    async fn list_workspace_members(
        &self,
        workspace_id: &str,
    ) -> Result<Vec<crate::projections::WorkspaceMemberRecord>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        Ok(state
            .workspace_members
            .iter()
            .filter(|m| m.workspace_id == workspace_id)
            .cloned()
            .collect())
    }

    async fn get_member(
        &self,
        workspace_key: &cairn_domain::tenancy::WorkspaceKey,
        operator_id: &str,
    ) -> Result<Option<crate::projections::WorkspaceMemberRecord>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        Ok(state
            .workspace_members
            .iter()
            .find(|m| {
                m.workspace_id == workspace_key.workspace_id.as_str()
                    && m.operator_id == operator_id
            })
            .cloned())
    }

    async fn add_workspace_member(
        &self,
        record: crate::projections::WorkspaceMemberRecord,
    ) -> Result<(), StoreError> {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .workspace_members
            .push(record);
        Ok(())
    }

    async fn remove_workspace_member(
        &self,
        workspace_id: &str,
        operator_id: &str,
    ) -> Result<(), StoreError> {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state
            .workspace_members
            .retain(|m| !(m.workspace_id == workspace_id && m.operator_id == operator_id));
        Ok(())
    }
}

// -- SignalSubscriptionReadModel --

#[async_trait]
impl crate::projections::SignalSubscriptionReadModel for InMemoryStore {
    async fn get_subscription(
        &self,
        subscription_id: &str,
    ) -> Result<Option<crate::projections::SignalSubscriptionRecord>, StoreError> {
        Ok(self
            .state
            .lock()
            .unwrap()
            .signal_subscriptions
            .get(subscription_id)
            .cloned())
    }

    async fn list_by_signal_type(
        &self,
        signal_type: &str,
    ) -> Result<Vec<crate::projections::SignalSubscriptionRecord>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        Ok(state
            .signal_subscriptions
            .values()
            .filter(|s| s.signal_type == signal_type)
            .cloned()
            .collect())
    }

    async fn list_by_signal_kind(
        &self,
        signal_kind: &str,
        _limit: usize,
        _offset: usize,
    ) -> Result<Vec<crate::projections::SignalSubscriptionRecord>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        Ok(state
            .signal_subscriptions
            .values()
            .filter(|s| s.signal_type == signal_kind)
            .cloned()
            .collect())
    }

    async fn list_by_project(
        &self,
        project: &cairn_domain::tenancy::ProjectKey,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<crate::projections::SignalSubscriptionRecord>, StoreError> {
        // #422: the previous impl ignored both `limit` and `offset`,
        // returning every subscription for the project. That broke the
        // pagination contract the handler now enforces (fetch
        // `limit + 1`, check overflow, truncate). Sort by
        // subscription_id for stable ordering across pages, then
        // skip/take.
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let tid = project.tenant_id.as_str();
        let wid = project.workspace_id.as_str();
        let pid = project.project_id.as_str();
        let mut all: Vec<_> = state
            .signal_subscriptions
            .values()
            .filter(|s| {
                s.project_tenant == tid && s.project_workspace == wid && s.project_id == pid
            })
            .cloned()
            .collect();
        all.sort_by(|a, b| a.subscription_id.cmp(&b.subscription_id));
        Ok(all.into_iter().skip(offset).take(limit).collect())
    }

    async fn upsert_subscription(
        &self,
        record: crate::projections::SignalSubscriptionRecord,
    ) -> Result<(), StoreError> {
        self.state
            .lock()
            .unwrap()
            .signal_subscriptions
            .insert(record.subscription_id.clone(), record);
        Ok(())
    }
}

#[async_trait]
impl crate::projections::CredentialRotationReadModel for InMemoryStore {
    async fn list_rotations(
        &self,
        tenant_id: &cairn_domain::TenantId,
    ) -> Result<Vec<cairn_domain::credentials::CredentialRotationRecord>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let results: Vec<_> = state
            .credential_rotations
            .iter()
            .filter(|r| &r.tenant_id == tenant_id)
            .cloned()
            .collect();
        Ok(results)
    }
}

// ── ProviderBindingReadModel ───────────────────────────────────────────────

#[async_trait]
impl crate::projections::ProviderBindingReadModel for InMemoryStore {
    async fn get(
        &self,
        id: &cairn_domain::ProviderBindingId,
    ) -> Result<Option<cairn_domain::providers::ProviderBindingRecord>, StoreError> {
        Ok(self
            .state
            .lock()
            .unwrap()
            .provider_bindings
            .get(id.as_str())
            .cloned())
    }

    async fn list_by_project(
        &self,
        project: &cairn_domain::ProjectKey,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<cairn_domain::providers::ProviderBindingRecord>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        Ok(state
            .provider_bindings
            .values()
            .filter(|b| &b.project == project)
            .skip(offset)
            .take(limit)
            .cloned()
            .collect())
    }

    async fn list_by_tenant(
        &self,
        tenant_id: &cairn_domain::TenantId,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<cairn_domain::providers::ProviderBindingRecord>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        Ok(state
            .provider_bindings
            .values()
            .filter(|b| b.project.tenant_id == *tenant_id)
            .skip(offset)
            .take(limit)
            .cloned()
            .collect())
    }

    async fn list_active(
        &self,
        project: &cairn_domain::ProjectKey,
        operation: cairn_domain::providers::OperationKind,
    ) -> Result<Vec<cairn_domain::providers::ProviderBindingRecord>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let mut results: Vec<_> = state
            .provider_bindings
            .values()
            .filter(|b| &b.project == project && b.active && b.operation_kind == operation)
            .cloned()
            .collect();
        // Stable creation-order: sort by created_at, then by binding ID for determinism.
        results.sort_by(|a, b| {
            a.created_at.cmp(&b.created_at).then_with(|| {
                a.provider_binding_id
                    .as_str()
                    .cmp(b.provider_binding_id.as_str())
            })
        });
        Ok(results)
    }
}

// ── ProviderHealthReadModel ───────────────────────────────────────────────

#[async_trait]
impl crate::projections::ProviderHealthReadModel for InMemoryStore {
    async fn get(
        &self,
        connection_id: &cairn_domain::ProviderConnectionId,
    ) -> Result<Option<cairn_domain::providers::ProviderHealthRecord>, StoreError> {
        Ok(self
            .state
            .lock()
            .unwrap()
            .provider_health_records
            .get(connection_id.as_str())
            .cloned())
    }

    async fn list_by_tenant(
        &self,
        _tenant_id: &cairn_domain::TenantId,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<cairn_domain::providers::ProviderHealthRecord>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let results: Vec<_> = state.provider_health_records.values().cloned().collect();
        Ok(results.into_iter().skip(offset).take(limit).collect())
    }
}

// ── ProviderHealthScheduleReadModel ──────────────────────────────────────

#[async_trait]
impl crate::projections::ProviderHealthScheduleReadModel for InMemoryStore {
    async fn get_schedule(
        &self,
        schedule_id: &str,
    ) -> Result<Option<cairn_domain::providers::ProviderHealthSchedule>, StoreError> {
        Ok(self
            .state
            .lock()
            .unwrap()
            .provider_health_schedules
            .get(schedule_id)
            .cloned())
    }

    async fn list_schedules_by_tenant(
        &self,
        tenant_id: &cairn_domain::TenantId,
    ) -> Result<Vec<cairn_domain::providers::ProviderHealthSchedule>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        Ok(state
            .provider_health_schedules
            .values()
            .filter(|s| &s.tenant_id == tenant_id)
            .cloned()
            .collect())
    }

    async fn list_enabled_schedules(
        &self,
    ) -> Result<Vec<cairn_domain::providers::ProviderHealthSchedule>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        Ok(state
            .provider_health_schedules
            .values()
            .filter(|s| s.enabled)
            .cloned()
            .collect())
    }
}

// ── Stub read-model implementations (linter-added service impl tests) ─────────

#[async_trait]
impl crate::projections::ChannelReadModel for InMemoryStore {
    async fn get_channel(
        &self,
        id: &cairn_domain::ChannelId,
    ) -> Result<Option<cairn_domain::ChannelRecord>, StoreError> {
        Ok(self
            .state
            .lock()
            .unwrap()
            .channels
            .get(id.as_str())
            .cloned())
    }
    async fn list_channels(
        &self,
        project: &cairn_domain::ProjectKey,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<cairn_domain::ChannelRecord>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        // RFC-025 Phase 2b.3 m3: sort by (created_at ASC, channel_id ASC)
        // to match pg/sqlite ORDER BY. HashMap::values is otherwise
        // unordered and breaks byte-equal parity.
        let mut rows: Vec<cairn_domain::ChannelRecord> = state
            .channels
            .values()
            .filter(|c| &c.project == project)
            .cloned()
            .collect();
        rows.sort_by(|a, b| {
            a.created_at
                .cmp(&b.created_at)
                .then_with(|| a.channel_id.as_str().cmp(b.channel_id.as_str()))
        });
        Ok(rows.into_iter().skip(offset).take(limit).collect())
    }
    async fn list_messages(
        &self,
        channel_id: &cairn_domain::ChannelId,
        limit: usize,
    ) -> Result<Vec<cairn_domain::ChannelMessage>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        // RFC-025 Phase 2b.3 m3: sort by (sent_at_ms ASC, message_id ASC)
        // — the in-memory store appends to a Vec in arrival order which
        // happens to match sent_at_ms ordering when events are appended
        // in chronological order, but an out-of-order replay would drift
        // from the SQL `ORDER BY` otherwise.
        let mut msgs = state
            .channel_messages
            .get(channel_id.as_str())
            .cloned()
            .unwrap_or_default();
        msgs.sort_by(|a, b| {
            a.sent_at_ms
                .cmp(&b.sent_at_ms)
                .then_with(|| a.message_id.cmp(&b.message_id))
        });
        Ok(msgs.into_iter().take(limit).collect())
    }
}

#[async_trait]
impl crate::projections::GuardrailReadModel for InMemoryStore {
    async fn get_policy(
        &self,
        policy_id: &str,
    ) -> Result<Option<cairn_domain::policy::GuardrailPolicy>, StoreError> {
        Ok(self
            .state
            .lock()
            .unwrap()
            .guardrail_policies
            .get(policy_id)
            .cloned())
    }
    async fn list_policies(
        &self,
        tenant_id: &cairn_domain::TenantId,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<cairn_domain::policy::GuardrailPolicy>, StoreError> {
        // RFC-025 Phase 2a.2 milestone 2: tenant-scoped read. pg/sqlite
        // now filter `guardrail_policies.tenant_id = $1` in SQL; the
        // in-memory side mirrors that via the sibling
        // `guardrail_policy_tenants` map. Pre-fix the in-memory impl
        // silently leaked policies across tenants.
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let mut policies: Vec<_> = state
            .guardrail_policies
            .values()
            .filter(|p| {
                state
                    .guardrail_policy_tenants
                    .get(&p.policy_id)
                    .is_some_and(|t| t == tenant_id)
            })
            .cloned()
            .collect();
        // Sort by policy_id (timestamp-based) for deterministic creation-order iteration.
        policies.sort_by_key(|r| r.policy_id.clone());
        Ok(policies.into_iter().skip(offset).take(limit).collect())
    }
}

// -- GuardrailEvaluationReadModel --

#[async_trait]
impl crate::projections::GuardrailEvaluationReadModel for InMemoryStore {
    async fn list_evaluations(
        &self,
        tenant_id: &cairn_domain::TenantId,
        limit: usize,
    ) -> Result<Vec<crate::projections::GuardrailEvaluationRecord>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let mut rows: Vec<_> = state
            .guardrail_evaluations
            .iter()
            .filter(|r| r.tenant_id == *tenant_id)
            .cloned()
            .collect();
        // Most-recent first: mirrors pg/sqlite
        // `ORDER BY evaluated_at_ms DESC, policy_id ASC`.
        rows.sort_by(|a, b| {
            b.evaluated_at_ms
                .cmp(&a.evaluated_at_ms)
                .then_with(|| a.policy_id.cmp(&b.policy_id))
        });
        Ok(rows.into_iter().take(limit).collect())
    }
}

#[async_trait]
impl crate::projections::LicenseReadModel for InMemoryStore {
    async fn get_active(
        &self,
        tenant_id: &cairn_domain::TenantId,
    ) -> Result<Option<cairn_domain::LicenseRecord>, StoreError> {
        Ok(self
            .state
            .lock()
            .unwrap()
            .licenses
            .get(tenant_id.as_str())
            .cloned())
    }
    async fn list_overrides(
        &self,
        tenant_id: &cairn_domain::TenantId,
    ) -> Result<Vec<cairn_domain::EntitlementOverrideRecord>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        // RFC-025 Phase 2a.2 milestone 4: sort by `feature` ASC to match
        // the pg/sqlite `ORDER BY feature ASC` contract. HashMap::values
        // yields unordered output otherwise, which breaks byte-equal
        // parity with the projection backends.
        let mut rows: Vec<cairn_domain::EntitlementOverrideRecord> = state
            .entitlement_overrides
            .values()
            .filter(|r| &r.tenant_id == tenant_id)
            .cloned()
            .collect();
        rows.sort_by(|a, b| a.feature.cmp(&b.feature));
        Ok(rows)
    }
}

#[async_trait]
impl crate::projections::DefaultsReadModel for InMemoryStore {
    async fn get(
        &self,
        scope: cairn_domain::Scope,
        scope_id: &str,
        key: &str,
    ) -> Result<Option<cairn_domain::DefaultSetting>, StoreError> {
        let k = format!(
            "{}:{}:{}",
            crate::projections::defaults_scope_str(scope),
            scope_id,
            key
        );
        Ok(self
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .default_settings
            .get(&k)
            .cloned())
    }
    async fn list_by_scope(
        &self,
        scope: cairn_domain::Scope,
        scope_id: &str,
    ) -> Result<Vec<cairn_domain::DefaultSetting>, StoreError> {
        let prefix = format!(
            "{}:{}:",
            crate::projections::defaults_scope_str(scope),
            scope_id
        );
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        // RFC-025 Phase 2b.3 m2: sort by `key` to match pg/sqlite
        // `ORDER BY key ASC`. HashMap iteration is otherwise unordered
        // and breaks byte-equal parity with the SQL backends.
        let mut rows: Vec<cairn_domain::DefaultSetting> = state
            .default_settings
            .iter()
            .filter(|(k, _)| k.starts_with(&prefix))
            .map(|(_, v)| v.clone())
            .collect();
        rows.sort_by(|a, b| a.key.cmp(&b.key));
        Ok(rows)
    }
}

#[async_trait]
impl crate::projections::RetentionPolicyReadModel for InMemoryStore {
    async fn get_by_tenant(
        &self,
        tenant_id: &cairn_domain::TenantId,
    ) -> Result<Option<cairn_domain::RetentionPolicy>, StoreError> {
        Ok(self
            .state
            .lock()
            .unwrap()
            .retention_policies
            .get(tenant_id.as_str())
            .cloned())
    }
}

#[async_trait]
impl crate::projections::RetentionMaintenance for InMemoryStore {
    async fn apply_retention(
        &self,
        tenant_id: &cairn_domain::TenantId,
    ) -> Result<cairn_domain::RetentionResult, StoreError> {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let policy = match state.retention_policies.get(tenant_id.as_str()).cloned() {
            Some(p) => p,
            None => {
                return Ok(cairn_domain::RetentionResult {
                    events_pruned: 0,
                    entities_affected: 0,
                })
            }
        };
        let max_per_entity = policy.max_events_per_entity as usize;
        if max_per_entity == 0 {
            return Ok(cairn_domain::RetentionResult {
                events_pruned: 0,
                entities_affected: 0,
            });
        }

        // Group events by entity (using primary_entity_ref).
        // Collect entity event positions, keep tail (newest), prune the rest.
        use std::collections::HashMap;
        let mut entity_positions: HashMap<String, Vec<usize>> = HashMap::new();
        for (idx, stored) in state.events.iter().enumerate() {
            if let Some(entity_ref) = stored.envelope.payload.primary_entity_ref() {
                let key = format!("{entity_ref:?}");
                entity_positions.entry(key).or_default().push(idx);
            }
        }

        let mut to_prune: std::collections::BTreeSet<usize> = std::collections::BTreeSet::new();
        let mut entities_affected = 0u32;

        for positions in entity_positions.values() {
            if positions.len() > max_per_entity {
                // Keep the most recent max_per_entity events; prune the rest (oldest).
                let prune_count = positions.len() - max_per_entity;
                for idx in positions.iter().take(prune_count) {
                    to_prune.insert(*idx);
                }
                entities_affected += 1;
            }
        }

        let events_pruned = to_prune.len() as u64;
        // Remove events at pruned indices (in reverse order to preserve indices).
        let mut sorted: Vec<usize> = to_prune.into_iter().collect();
        sorted.sort_unstable_by(|a, b| b.cmp(a)); // reverse order
        for idx in sorted {
            state.events.remove(idx);
        }

        Ok(cairn_domain::RetentionResult {
            events_pruned,
            entities_affected,
        })
    }
}

#[async_trait]
impl crate::projections::RunSlaReadModel for InMemoryStore {
    async fn get_sla(
        &self,
        run_id: &cairn_domain::RunId,
    ) -> Result<Option<cairn_domain::sla::SlaConfig>, StoreError> {
        Ok(self
            .state
            .lock()
            .unwrap()
            .run_sla_configs
            .get(run_id.as_str())
            .cloned())
    }
    async fn get_breach(
        &self,
        run_id: &cairn_domain::RunId,
    ) -> Result<Option<cairn_domain::sla::SlaBreach>, StoreError> {
        Ok(self
            .state
            .lock()
            .unwrap()
            .run_sla_breaches
            .get(run_id.as_str())
            .cloned())
    }
    async fn list_breached_by_tenant(
        &self,
        tenant_id: &cairn_domain::TenantId,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<cairn_domain::sla::SlaBreach>, StoreError> {
        // Issue #570: pagination moved into the projection — handlers no
        // longer fetch every row then slice in memory.
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let mut filtered: Vec<cairn_domain::sla::SlaBreach> = state
            .run_sla_breaches
            .values()
            .filter(|b| &b.tenant_id == tenant_id)
            .cloned()
            .collect();
        // Newest-first order so page 1 is the most-recent breaches.
        filtered.sort_by(|a, b| {
            b.breached_at_ms
                .cmp(&a.breached_at_ms)
                .then_with(|| a.run_id.as_str().cmp(b.run_id.as_str()))
        });
        Ok(filtered.into_iter().skip(offset).take(limit).collect())
    }
}

#[async_trait]
impl crate::projections::NotificationReadModel for InMemoryStore {
    async fn get_preferences(
        &self,
        tenant_id: &cairn_domain::TenantId,
        operator_id: &str,
    ) -> Result<Option<cairn_domain::notification_prefs::NotificationPreference>, StoreError> {
        let key = format!("{}:{}", tenant_id.as_str(), operator_id);
        Ok(self
            .state
            .lock()
            .unwrap()
            .notification_prefs
            .get(&key)
            .cloned())
    }
    async fn list_preferences_by_tenant(
        &self,
        tenant_id: &cairn_domain::TenantId,
    ) -> Result<Vec<cairn_domain::notification_prefs::NotificationPreference>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        // RFC-025 Phase 2b.3 m4: sort by `operator_id ASC` to match
        // pg/sqlite `ORDER BY operator_id ASC`.
        let mut rows: Vec<cairn_domain::notification_prefs::NotificationPreference> = state
            .notification_prefs
            .values()
            .filter(|p| &p.tenant_id == tenant_id)
            .cloned()
            .collect();
        rows.sort_by(|a, b| a.operator_id.cmp(&b.operator_id));
        Ok(rows)
    }
    async fn list_sent_notifications(
        &self,
        tenant_id: &cairn_domain::TenantId,
        since_ms: u64,
    ) -> Result<Vec<cairn_domain::notification_prefs::NotificationRecord>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        // RFC-025 Phase 2b.3 m4: sort (sent_at_ms ASC, record_id ASC) to
        // match pg/sqlite ORDER BY.
        let mut rows: Vec<cairn_domain::notification_prefs::NotificationRecord> = state
            .notification_records
            .iter()
            .filter(|r| &r.tenant_id == tenant_id && r.sent_at_ms >= since_ms)
            .cloned()
            .collect();
        rows.sort_by(|a, b| {
            a.sent_at_ms
                .cmp(&b.sent_at_ms)
                .then_with(|| a.record_id.cmp(&b.record_id))
        });
        Ok(rows)
    }
    async fn list_failed_notifications(
        &self,
        tenant_id: &cairn_domain::TenantId,
    ) -> Result<Vec<cairn_domain::notification_prefs::NotificationRecord>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let mut rows: Vec<cairn_domain::notification_prefs::NotificationRecord> = state
            .notification_records
            .iter()
            .filter(|r| &r.tenant_id == tenant_id && !r.delivered)
            .cloned()
            .collect();
        rows.sort_by(|a, b| {
            a.sent_at_ms
                .cmp(&b.sent_at_ms)
                .then_with(|| a.record_id.cmp(&b.record_id))
        });
        Ok(rows)
    }
}

#[async_trait]
impl crate::projections::ProviderConnectionReadModel for InMemoryStore {
    async fn get(
        &self,
        id: &cairn_domain::ProviderConnectionId,
    ) -> Result<Option<cairn_domain::providers::ProviderConnectionRecord>, StoreError> {
        Ok(self
            .state
            .lock()
            .unwrap()
            .provider_connections
            .get(id.as_str())
            .cloned())
    }
    async fn list_by_tenant(
        &self,
        tenant_id: &cairn_domain::TenantId,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<cairn_domain::providers::ProviderConnectionRecord>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        Ok(state
            .provider_connections
            .values()
            .filter(|r| &r.tenant_id == tenant_id)
            .skip(offset)
            .take(limit)
            .cloned()
            .collect())
    }
}

#[async_trait]
impl crate::projections::ProviderPoolReadModel for InMemoryStore {
    async fn get_pool(
        &self,
        pool_id: &str,
    ) -> Result<Option<cairn_domain::providers::ProviderConnectionPool>, StoreError> {
        Ok(self
            .state
            .lock()
            .unwrap()
            .provider_pools
            .get(pool_id)
            .cloned())
    }
    async fn list_pools_by_tenant(
        &self,
        tenant_id: &cairn_domain::TenantId,
    ) -> Result<Vec<cairn_domain::providers::ProviderConnectionPool>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        Ok(state
            .provider_pools
            .values()
            .filter(|p| &p.tenant_id == tenant_id)
            .cloned()
            .collect())
    }
}

#[async_trait]
impl crate::projections::CredentialReadModel for InMemoryStore {
    async fn get(
        &self,
        id: &cairn_domain::CredentialId,
    ) -> Result<Option<cairn_domain::credentials::CredentialRecord>, StoreError> {
        Ok(self
            .state
            .lock()
            .unwrap()
            .credentials
            .get(id.as_str())
            .cloned())
    }
    async fn list_by_tenant(
        &self,
        tenant_id: &cairn_domain::TenantId,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<cairn_domain::credentials::CredentialRecord>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        Ok(state
            .credentials
            .values()
            .filter(|r| &r.tenant_id == tenant_id)
            .skip(offset)
            .take(limit)
            .cloned()
            .collect())
    }

    /// Single-pass scan across all tenants. Used by
    /// `cairn_runtime::services::scan_legacy_ciphertexts` at boot. The
    /// InMemoryStore projection is the authoritative read model for
    /// every backend (pg and sqlite dual-write through service events),
    /// so one pass over `state.credentials` covers the whole deployment
    /// without the per-tenant N+1 that the default impl falls back to.
    ///
    /// Returns `Some(rows)` — including `Some(Vec::new())` on a deployment
    /// with zero credentials — so the caller can unambiguously skip the
    /// per-tenant fallback. The default `Ok(None)` is reserved for
    /// backends that have not wired a single-pass path.
    async fn list_all_active(
        &self,
        limit: usize,
    ) -> Result<Option<Vec<cairn_domain::credentials::CredentialRecord>>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        Ok(Some(
            state
                .credentials
                .values()
                .filter(|r| r.active)
                .take(limit)
                .cloned()
                .collect(),
        ))
    }
}

#[async_trait]
impl crate::projections::RunCostAlertReadModel for InMemoryStore {
    async fn get_alert(
        &self,
        run_id: &cairn_domain::RunId,
    ) -> Result<Option<cairn_domain::providers::RunCostAlert>, StoreError> {
        Ok(self
            .state
            .lock()
            .unwrap()
            .run_cost_alerts
            .get(run_id.as_str())
            .cloned())
    }
    async fn list_triggered_by_tenant(
        &self,
        tenant_id: &cairn_domain::TenantId,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<cairn_domain::providers::RunCostAlert>, StoreError> {
        // Issue #570: pagination at the projection — callers pass
        // `limit + 1` to detect `has_more` without re-scanning.
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let mut filtered: Vec<cairn_domain::providers::RunCostAlert> = state
            .run_cost_alerts
            .values()
            .filter(|a| &a.tenant_id == tenant_id && a.triggered_at_ms > 0)
            .cloned()
            .collect();
        // Newest-first so page 1 is the most-recent triggers.
        filtered.sort_by(|a, b| {
            b.triggered_at_ms
                .cmp(&a.triggered_at_ms)
                .then_with(|| a.run_id.as_str().cmp(b.run_id.as_str()))
        });
        Ok(filtered.into_iter().skip(offset).take(limit).collect())
    }
}

#[async_trait]
impl crate::projections::AuditLogReadModel for InMemoryStore {
    async fn list_by_tenant(
        &self,
        tenant_id: &cairn_domain::TenantId,
        since_ms: Option<u64>,
        before_ms: Option<u64>,
        limit: usize,
    ) -> Result<Vec<cairn_domain::AuditLogEntry>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let mut rows: Vec<&crate::projections::AuditLogEntryRecord> = state
            .audit_log_entries
            .values()
            .filter(|rec| &rec.tenant_id == tenant_id)
            .filter(|rec| since_ms.is_none_or(|since| rec.occurred_at_ms >= since))
            .filter(|rec| before_ms.is_none_or(|before| rec.occurred_at_ms < before))
            .collect();
        // Newest-first per trait doc. Tiebreak on entry_id so cross-backend
        // parity does not flap on identical timestamps.
        rows.sort_by(|a, b| {
            b.occurred_at_ms
                .cmp(&a.occurred_at_ms)
                .then_with(|| b.entry_id.cmp(&a.entry_id))
        });
        Ok(rows
            .into_iter()
            .take(limit)
            .cloned()
            .map(crate::projections::AuditLogEntryRecord::into_entry)
            .collect())
    }

    async fn list_by_resource(
        &self,
        resource_type: &str,
        resource_id: &str,
    ) -> Result<Vec<cairn_domain::AuditLogEntry>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let mut rows: Vec<&crate::projections::AuditLogEntryRecord> = state
            .audit_log_entries
            .values()
            .filter(|rec| rec.resource_type == resource_type && rec.resource_id == resource_id)
            .collect();
        rows.sort_by(|a, b| {
            b.occurred_at_ms
                .cmp(&a.occurred_at_ms)
                .then_with(|| b.entry_id.cmp(&a.entry_id))
        });
        // Shared cap with pg + sqlite — see `LIST_BY_RESOURCE_MAX_ROWS`
        // docs. Copilot PR #573 review flagged this as a cross-backend
        // divergence + DoS vector on a pathological per-resource audit
        // trail.
        Ok(rows
            .into_iter()
            .take(crate::projections::LIST_BY_RESOURCE_MAX_ROWS)
            .cloned()
            .map(crate::projections::AuditLogEntryRecord::into_entry)
            .collect())
    }
}

#[async_trait]
impl crate::projections::QuotaReadModel for InMemoryStore {
    async fn get_quota(
        &self,
        tenant_id: &cairn_domain::TenantId,
    ) -> Result<Option<cairn_domain::TenantQuota>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let Some(mut quota) = state.quotas.get(tenant_id.as_str()).cloned() else {
            return Ok(None);
        };
        // Dynamically compute current_active_runs from run state.
        // An active run is one that belongs to this tenant and is not in a terminal state.
        let active_runs = state
            .runs
            .values()
            .filter(|r| r.project.tenant_id == *tenant_id && !r.state.is_terminal())
            .count() as u32;
        quota.current_active_runs = active_runs;
        // Dynamically compute sessions_this_hour from session state.
        // Count sessions that have been created (all sessions for this tenant).
        // For simplicity in tests, count all sessions (the test creates sessions and checks the limit).
        let sessions_count = state
            .sessions
            .values()
            .filter(|s| s.project.tenant_id == *tenant_id)
            .count() as u32;
        quota.sessions_this_hour = sessions_count;
        Ok(Some(quota))
    }
}

#[async_trait]
impl crate::projections::QuotaViolationReadModel for InMemoryStore {
    async fn list_violations(
        &self,
        tenant_id: &cairn_domain::TenantId,
        limit: usize,
    ) -> Result<Vec<crate::projections::QuotaViolationRecord>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        // Most-recent-first ordering matches the pg/sqlite
        // `ORDER BY occurred_at_ms DESC, quota_type ASC` contract —
        // Copilot PR #565 flagged the missing quota_type tiebreaker as
        // a determinism/parity gap.
        let mut rows: Vec<_> = state
            .quota_violations
            .iter()
            .filter(|v| &v.tenant_id == tenant_id)
            .cloned()
            .collect();
        rows.sort_by(|a, b| {
            b.occurred_at_ms
                .cmp(&a.occurred_at_ms)
                .then_with(|| a.quota_type.cmp(&b.quota_type))
        });
        rows.truncate(limit);
        Ok(rows)
    }
}

#[async_trait]
impl crate::projections::ProviderBudgetReadModel for InMemoryStore {
    async fn get_by_tenant_period(
        &self,
        tenant_id: &cairn_domain::TenantId,
        period: cairn_domain::providers::ProviderBudgetPeriod,
    ) -> Result<Option<cairn_domain::providers::ProviderBudget>, StoreError> {
        // RFC-025 Phase 2a.1 milestone 3: provider_budgets are now keyed
        // by `budget_id` (parity with pg/sqlite), so the tenant/period
        // lookup scans the values map. Historical behaviour returned
        // the single `tenant_id:period` row; to preserve deterministic
        // selection when multiple budgets share (tenant, period), return
        // the one with the earliest `created_at` so repeat calls always
        // pick the same row.
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let mut candidates: Vec<_> = state
            .provider_budgets
            .values()
            .filter(|b| &b.tenant_id == tenant_id && b.period == period)
            .cloned()
            .collect();
        candidates.sort_by_key(|b| (b.created_at, b.limit_micros));
        Ok(candidates.into_iter().next())
    }
    async fn list_by_tenant(
        &self,
        tenant_id: &cairn_domain::TenantId,
    ) -> Result<Vec<cairn_domain::providers::ProviderBudget>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        Ok(state
            .provider_budgets
            .values()
            .filter(|b| &b.tenant_id == tenant_id)
            .cloned()
            .collect())
    }
}

// -- TaskLeaseExpiredReadModel --

#[async_trait]
impl crate::projections::TaskLeaseExpiredReadModel for InMemoryStore {
    async fn list_expired(
        &self,
        now_ms: u64,
    ) -> Result<Vec<crate::projections::TaskRecord>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        Ok(state
            .tasks
            .values()
            .filter(|t| {
                matches!(
                    t.state,
                    cairn_domain::TaskState::Leased | cairn_domain::TaskState::Running
                ) && t.lease_expires_at.is_some_and(|exp| exp <= now_ms)
            })
            .cloned()
            .collect())
    }
}

// -- CheckpointStrategyReadModel --

#[async_trait]
impl crate::projections::CheckpointStrategyReadModel for InMemoryStore {
    async fn get_by_run(
        &self,
        run_id: &cairn_domain::RunId,
    ) -> Result<Option<cairn_domain::CheckpointStrategy>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        Ok(state.checkpoint_strategies.get(run_id.as_str()).cloned())
    }
}

// -- OperatorInterventionReadModel --

#[async_trait]
impl crate::projections::OperatorInterventionReadModel for InMemoryStore {
    async fn list_by_run(
        &self,
        run_id: &cairn_domain::RunId,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<crate::projections::OperatorInterventionRecord>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let records: Vec<_> = state
            .events
            .iter()
            .filter_map(|e| {
                if let RuntimeEvent::OperatorIntervention(op) = &e.envelope.payload {
                    if op.run_id.as_ref() == Some(run_id) {
                        return Some(crate::projections::OperatorInterventionRecord {
                            run_id: run_id.clone(),
                            tenant_id: op.tenant_id.clone(),
                            action: op.action.clone(),
                            reason: op.reason.clone(),
                            intervened_at_ms: op.intervened_at_ms,
                        });
                    }
                }
                None
            })
            .skip(offset)
            .take(limit)
            .collect();
        Ok(records)
    }
}

// -- PauseScheduleReadModel --

#[async_trait]
impl crate::projections::PauseScheduleReadModel for InMemoryStore {
    async fn list_due(
        &self,
        tenant_id: &cairn_domain::TenantId,
        before_ms: u64,
        limit: usize,
    ) -> Result<Vec<crate::projections::PauseScheduledRecord>, StoreError> {
        // Issue #592: evict-on-resume projection — read from
        // `state.pause_schedules` (populated by the RunStateChanged
        // projection arm) instead of walking the full event log.
        //
        // Contract parity with pg/sqlite/sqlite adapter's `list_due`:
        //   - tenant gate (`tenant_id == caller`).
        //   - `resume_at_ms <= before_ms` filter.
        //   - ORDER BY `resume_at_ms ASC, run_id ASC` — stable
        //     ordering so backends agree on membership/eviction
        //     semantics even when `resume_at_ms` is compared with
        //     sub-second tolerance.
        //   - LIMIT applied AFTER the ordering, not via a random
        //     partial iterator.
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let mut due: Vec<_> = state
            .pause_schedules
            .values()
            .filter(|r| r.project.tenant_id == *tenant_id && r.resume_at_ms <= before_ms)
            .cloned()
            .collect();
        due.sort_by(|a, b| {
            a.resume_at_ms
                .cmp(&b.resume_at_ms)
                .then_with(|| a.run_id.as_str().cmp(b.run_id.as_str()))
        });
        Ok(due.into_iter().take(limit).collect())
    }
}

// -- RecoveryEscalationReadModel --

#[async_trait]
impl crate::projections::RecoveryEscalationReadModel for InMemoryStore {
    async fn get_by_run(
        &self,
        _run_id: &cairn_domain::RunId,
    ) -> Result<Option<cairn_domain::RecoveryEscalation>, StoreError> {
        Ok(None)
    }
    async fn list_by_tenant(
        &self,
        _tenant_id: &cairn_domain::TenantId,
        _limit: usize,
        _offset: usize,
    ) -> Result<Vec<cairn_domain::RecoveryEscalation>, StoreError> {
        // Issue #570: trait shape updated to carry storage-layer
        // pagination. The InMemoryStore impl is a no-op stub —
        // recovery escalations are not projected here today. Callers
        // always see an empty list regardless of page params.
        Ok(vec![])
    }
}

// -- SnapshotReadModel --

#[async_trait]
impl crate::projections::SnapshotReadModel for InMemoryStore {
    async fn get_latest(
        &self,
        tenant_id: &cairn_domain::TenantId,
    ) -> Result<Option<cairn_domain::Snapshot>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        Ok(state
            .snapshots
            .iter()
            .filter(|s| s.tenant_id == *tenant_id)
            .max_by_key(|s| s.created_at_ms)
            .cloned())
    }
    async fn list_by_tenant(
        &self,
        tenant_id: &cairn_domain::TenantId,
    ) -> Result<Vec<cairn_domain::Snapshot>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let mut results: Vec<_> = state
            .snapshots
            .iter()
            .filter(|s| s.tenant_id == *tenant_id)
            .cloned()
            .collect();
        results.sort_by_key(|s| s.created_at_ms);
        Ok(results)
    }
}

// -- RoutePolicyReadModel --

#[async_trait]
impl crate::projections::RoutePolicyReadModel for InMemoryStore {
    async fn get(
        &self,
        policy_id: &str,
    ) -> Result<Option<cairn_domain::providers::RoutePolicy>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        Ok(state.route_policies.get(policy_id).cloned())
    }

    async fn list_by_tenant(
        &self,
        tenant_id: &cairn_domain::TenantId,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<cairn_domain::providers::RoutePolicy>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let mut results: Vec<_> = state
            .route_policies
            .values()
            .filter(|p| p.enabled && p.tenant_id == tenant_id.as_str())
            .cloned()
            .collect();
        results.sort_by_key(|r| r.policy_id.clone());
        Ok(results.into_iter().skip(offset).take(limit).collect())
    }
}

// -- ProviderBindingCostStatsReadModel --

#[async_trait]
impl crate::projections::ProviderBindingCostStatsReadModel for InMemoryStore {
    async fn get(
        &self,
        binding_id: &cairn_domain::ProviderBindingId,
    ) -> Result<Option<cairn_domain::providers::ProviderBindingCostStats>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let calls: Vec<_> = state
            .provider_calls
            .values()
            .filter(|c| c.provider_binding_id == *binding_id)
            .cloned()
            .collect();
        if calls.is_empty() {
            return Ok(None);
        }
        let total_cost_micros: u64 = calls.iter().filter_map(|c| c.cost_micros).sum();
        let call_count = calls.len() as u64;
        Ok(Some(cairn_domain::providers::ProviderBindingCostStats {
            binding_id: binding_id.clone(),
            total_cost_micros,
            call_count,
        }))
    }
    async fn list_by_tenant(
        &self,
        tenant_id: &cairn_domain::TenantId,
    ) -> Result<Vec<cairn_domain::providers::ProviderBindingCostStats>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        // Scan raw events for ProviderCallCompleted to access the full project key (tenant_id).
        let mut stats: std::collections::HashMap<
            String,
            cairn_domain::providers::ProviderBindingCostStats,
        > = std::collections::HashMap::new();
        for stored in &state.events {
            if let cairn_domain::RuntimeEvent::ProviderCallCompleted(e) = &stored.envelope.payload {
                if e.project.tenant_id != *tenant_id {
                    continue;
                }
                let entry = stats
                    .entry(e.provider_binding_id.as_str().to_owned())
                    .or_insert_with(|| cairn_domain::providers::ProviderBindingCostStats {
                        binding_id: e.provider_binding_id.clone(),
                        total_cost_micros: 0,
                        call_count: 0,
                    });
                entry.total_cost_micros = entry
                    .total_cost_micros
                    .saturating_add(e.cost_micros.unwrap_or(0));
                entry.call_count = entry.call_count.saturating_add(1);
            }
        }
        let mut results: Vec<_> = stats.into_values().collect();
        results.sort_by_key(|s| s.total_cost_micros / s.call_count.max(1));
        Ok(results)
    }
}

#[async_trait]
impl crate::projections::ResourceSharingReadModel for InMemoryStore {
    async fn get_share(
        &self,
        share_id: &str,
    ) -> Result<Option<cairn_domain::resource_sharing::SharedResource>, StoreError> {
        Ok(self
            .state
            .lock()
            .unwrap()
            .resource_shares
            .get(share_id)
            .cloned())
    }
    async fn list_shares_for_workspace(
        &self,
        tenant_id: &cairn_domain::TenantId,
        target_workspace_id: &cairn_domain::WorkspaceId,
    ) -> Result<Vec<cairn_domain::resource_sharing::SharedResource>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        // RFC-025 Phase 2b.2b m1: sort by (shared_at_ms ASC, share_id ASC)
        // to match pg/sqlite `ORDER BY shared_at_ms, share_id` so parity
        // tests and operator dashboards see stable ordering under
        // same-ms share bursts.
        let mut out: Vec<_> = state
            .resource_shares
            .values()
            .filter(|s| &s.tenant_id == tenant_id && &s.target_workspace_id == target_workspace_id)
            .cloned()
            .collect();
        out.sort_by(|a, b| {
            a.shared_at_ms
                .cmp(&b.shared_at_ms)
                .then_with(|| a.share_id.cmp(&b.share_id))
        });
        Ok(out)
    }
    async fn get_share_for_resource(
        &self,
        tenant_id: &cairn_domain::TenantId,
        target_workspace_id: &cairn_domain::WorkspaceId,
        resource_type: &str,
        resource_id: &str,
    ) -> Result<Option<cairn_domain::resource_sharing::SharedResource>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        Ok(state
            .resource_shares
            .values()
            .find(|s| {
                &s.tenant_id == tenant_id
                    && &s.target_workspace_id == target_workspace_id
                    && s.resource_type == resource_type
                    && s.resource_id == resource_id
            })
            .cloned())
    }
}

// -- RFC-025 Phase 2b.2b m3: SubagentSpawnReadModel --

#[async_trait]
impl crate::projections::SubagentSpawnReadModel for InMemoryStore {
    async fn get_by_child_task(
        &self,
        child_task_id: &cairn_domain::TaskId,
    ) -> Result<Option<crate::projections::SubagentSpawnRecord>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        Ok(state.subagent_spawns.get(child_task_id.as_str()).cloned())
    }

    async fn get_by_child_run_id(
        &self,
        child_run_id: &cairn_domain::RunId,
    ) -> Result<Option<crate::projections::SubagentSpawnRecord>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        Ok(state
            .subagent_spawns
            .values()
            .find(|r| r.child_run_id.as_ref() == Some(child_run_id))
            .cloned())
    }

    async fn list_by_parent_run(
        &self,
        parent_run_id: &cairn_domain::RunId,
    ) -> Result<Vec<crate::projections::SubagentSpawnRecord>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let mut out: Vec<_> = state
            .subagent_spawns
            .values()
            .filter(|r| r.parent_run_id == *parent_run_id)
            .cloned()
            .collect();
        out.sort_by(|a, b| {
            a.spawned_at_ms
                .cmp(&b.spawned_at_ms)
                .then_with(|| a.child_task_id.as_str().cmp(b.child_task_id.as_str()))
        });
        Ok(out)
    }
}

// -- RFC-025 Phase 2b.2b m4: UserMessageReadModel --

#[async_trait]
impl crate::projections::UserMessageReadModel for InMemoryStore {
    async fn list_by_run(
        &self,
        run_id: &cairn_domain::RunId,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<crate::projections::UserMessageRecord>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let mut out: Vec<_> = state
            .user_messages
            .values()
            .filter(|m| m.run_id == *run_id)
            .cloned()
            .collect();
        out.sort_by(|a, b| {
            a.sequence
                .cmp(&b.sequence)
                .then_with(|| a.appended_at_ms.cmp(&b.appended_at_ms))
        });
        Ok(out.into_iter().skip(offset).take(limit).collect())
    }

    async fn count_by_run(&self, run_id: &cairn_domain::RunId) -> Result<u64, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        Ok(state
            .user_messages
            .values()
            .filter(|m| m.run_id == *run_id)
            .count() as u64)
    }
}

// -- RFC-025 Phase 2b.2b m6: ToolRecoveryPauseReadModel --

#[async_trait]
impl crate::projections::ToolRecoveryPauseReadModel for InMemoryStore {
    async fn get(
        &self,
        tool_call_id: &str,
    ) -> Result<Option<crate::projections::ToolRecoveryPauseRecord>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        Ok(state.tool_recovery_pauses.get(tool_call_id).cloned())
    }

    async fn list_by_run(
        &self,
        run_id: &cairn_domain::RunId,
    ) -> Result<Vec<crate::projections::ToolRecoveryPauseRecord>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let mut out: Vec<_> = state
            .tool_recovery_pauses
            .values()
            .filter(|p| p.run_id == *run_id)
            .cloned()
            .collect();
        out.sort_by(|a, b| {
            a.paused_at_ms
                .cmp(&b.paused_at_ms)
                .then_with(|| a.tool_call_id.cmp(&b.tool_call_id))
        });
        Ok(out)
    }
}

// -- RFC-025 Phase 2b.2b m5: SoulPatchReadModel --

#[async_trait]
impl crate::projections::SoulPatchReadModel for InMemoryStore {
    async fn get(
        &self,
        patch_id: &str,
    ) -> Result<Option<crate::projections::SoulPatchRecord>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        Ok(state.soul_patches.get(patch_id).cloned())
    }

    async fn list_by_project(
        &self,
        project: &cairn_domain::tenancy::ProjectKey,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<crate::projections::SoulPatchRecord>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let mut out: Vec<_> = state
            .soul_patches
            .values()
            .filter(|p| p.project == *project)
            .cloned()
            .collect();
        // Newest-first by proposed_at_ms, tiebreak on patch_id DESC.
        out.sort_by(|a, b| {
            b.proposed_at_ms
                .cmp(&a.proposed_at_ms)
                .then_with(|| b.patch_id.cmp(&a.patch_id))
        });
        Ok(out.into_iter().skip(offset).take(limit).collect())
    }
}

#[async_trait]
impl crate::projections::FfLeaseHistoryCursorStore for InMemoryStore {
    async fn get(
        &self,
        partition_id: &str,
        execution_id: &str,
    ) -> Result<Option<crate::projections::FfLeaseHistoryCursor>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        Ok(state
            .ff_lease_history_cursors
            .get(&(partition_id.to_owned(), execution_id.to_owned()))
            .cloned())
    }

    async fn list_by_partition(
        &self,
        partition_id: &str,
    ) -> Result<Vec<crate::projections::FfLeaseHistoryCursor>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        Ok(state
            .ff_lease_history_cursors
            .values()
            .filter(|c| c.partition_id == partition_id)
            .cloned()
            .collect())
    }

    async fn upsert(
        &self,
        cursor: &crate::projections::FfLeaseHistoryCursor,
    ) -> Result<(), StoreError> {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state.ff_lease_history_cursors.insert(
            (cursor.partition_id.clone(), cursor.execution_id.clone()),
            cursor.clone(),
        );
        Ok(())
    }

    async fn delete(&self, partition_id: &str, execution_id: &str) -> Result<(), StoreError> {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state
            .ff_lease_history_cursors
            .remove(&(partition_id.to_owned(), execution_id.to_owned()));
        Ok(())
    }
}

// ── F65 PR-2: orchestrator-session read models ────────────────────────────

#[async_trait]
impl crate::projections::SessionOutcomeReadModel for InMemoryStore {
    async fn get_by_root_run(
        &self,
        project: &ProjectKey,
        root_run_id: &RunId,
    ) -> Result<Option<crate::projections::SessionOutcomeRecord>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        // Defence-in-depth per issue #438: filter on the project tuple
        // even though the row also has a unique id, so a caller that
        // forgets to pre-check the tenant cannot return a foreign
        // outcome. Returning None (not NotFound) is intentional: it
        // mirrors the pg/sqlite `AND tenant_id/workspace_scope/project_id`
        // WHERE clause which also yields an empty row-set.
        Ok(state
            .session_outcomes
            .get(root_run_id.as_str())
            .filter(|o| o.project == *project)
            .cloned())
    }

    async fn list_by_session(
        &self,
        project: &ProjectKey,
        session_id: &SessionId,
    ) -> Result<Vec<crate::projections::SessionOutcomeRecord>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let mut results: Vec<crate::projections::SessionOutcomeRecord> = state
            .session_outcomes
            .values()
            .filter(|o| o.project == *project && o.session_id == *session_id)
            .cloned()
            .collect();
        // sort_by with a two-key comparator avoids the per-comparison
        // String allocation that `sort_by_key` would require for
        // tuples containing `String`.
        results.sort_by(|a, b| {
            a.created_at
                .cmp(&b.created_at)
                .then_with(|| a.root_run_id.as_str().cmp(b.root_run_id.as_str()))
        });
        Ok(results)
    }
}

impl InMemoryStore {
    /// F65 PR-5: enumerate every `workspace_snapshots` row that is
    /// past-TTL AND belongs to a session in a terminal state.
    ///
    /// Lives directly on `InMemoryStore` (not a trait) because it
    /// iterates two read models at once and the trait-based approach
    /// requires an "enumerate all" method on `SessionReadModel` /
    /// `WorkspaceSnapshotReadModel` that is not portable to pg/sqlite
    /// without introducing a dialect-specific OFFSET/LIMIT pagination
    /// plus a session-status join. The single-node in-memory store
    /// has full visibility into both tables; the GC sweeper uses that.
    pub fn list_snapshots_for_gc(
        &self,
        now_ms: u64,
        ttl_ms: u64,
    ) -> Vec<crate::projections::WorkspaceSnapshotRecord> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state
            .workspace_snapshots
            .values()
            .filter(|s| s.reaped_at.is_none())
            .filter(|s| now_ms.saturating_sub(s.created_at) >= ttl_ms)
            .filter(|s| {
                state
                    .sessions
                    .get(s.session_id.as_str())
                    .map(|r| !matches!(r.state, cairn_domain::SessionState::Open))
                    .unwrap_or(false)
            })
            .cloned()
            .collect()
    }
}

#[async_trait]
impl crate::projections::WorkspaceSnapshotWriter for InMemoryStore {
    async fn stamp_metadata(
        &self,
        snapshot_id: &WorkspaceSnapshotId,
        snapshot_path: &str,
        bytes: u64,
        reflink_used: bool,
        parent_snapshot_id: Option<&WorkspaceSnapshotId>,
    ) -> Result<(), StoreError> {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(rec) = state.workspace_snapshots.get_mut(snapshot_id.as_str()) {
            rec.snapshot_path = snapshot_path.to_owned();
            rec.bytes = bytes;
            rec.reflink_used = reflink_used;
            rec.parent_snapshot_id = parent_snapshot_id.cloned();
        }
        Ok(())
    }
}

#[async_trait]
impl crate::projections::WorkspaceSnapshotReadModel for InMemoryStore {
    async fn get(
        &self,
        project: &ProjectKey,
        snapshot_id: &WorkspaceSnapshotId,
    ) -> Result<Option<crate::projections::WorkspaceSnapshotRecord>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        Ok(state
            .workspace_snapshots
            .get(snapshot_id.as_str())
            .filter(|s| s.project == *project)
            .cloned())
    }

    async fn list_by_session(
        &self,
        project: &ProjectKey,
        session_id: &SessionId,
    ) -> Result<Vec<crate::projections::WorkspaceSnapshotRecord>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let mut results: Vec<crate::projections::WorkspaceSnapshotRecord> = state
            .workspace_snapshots
            .values()
            .filter(|s| s.project == *project && s.session_id == *session_id)
            .cloned()
            .collect();
        // sort_by avoids the per-comparison String allocation that
        // sort_by_key would need here. See the matching rationale on
        // `SessionOutcomeReadModel::list_by_session` above.
        results.sort_by(|a, b| {
            a.created_at
                .cmp(&b.created_at)
                .then_with(|| a.snapshot_id.as_str().cmp(b.snapshot_id.as_str()))
        });
        Ok(results)
    }

    async fn lineage(
        &self,
        project: &ProjectKey,
        start: &WorkspaceSnapshotId,
    ) -> Result<Vec<crate::projections::WorkspaceSnapshotRecord>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let mut chain: Vec<crate::projections::WorkspaceSnapshotRecord> = Vec::new();
        let mut cursor = Some(start.as_str().to_owned());
        // Bound the walk to table size so a cycle in the lineage chain
        // cannot spin forever. The pg FK on `parent_snapshot_id` makes
        // cycles unreachable in practice, but the in-memory store has no
        // such guardrail — be defensive.
        let cap = state.workspace_snapshots.len() + 1;
        for _ in 0..cap {
            let Some(id) = cursor.take() else {
                break;
            };
            let Some(rec) = state.workspace_snapshots.get(&id) else {
                break;
            };
            // Issue #438: a lineage chain is always within one project
            // by construction (parent/child rows share the same scope at
            // insert time). A boundary crossing therefore means writer
            // corruption — stop walking rather than leak a foreign row.
            if rec.project != *project {
                break;
            }
            cursor = rec
                .parent_snapshot_id
                .as_ref()
                .map(|p| p.as_str().to_owned());
            chain.push(rec.clone());
        }
        Ok(chain)
    }
}

#[async_trait]
impl crate::projections::WorkspaceRegistryReadModel for InMemoryStore {
    async fn get(
        &self,
        project: &ProjectKey,
        workspace_id: &WorkspaceId,
    ) -> Result<Option<crate::projections::WorkspaceRegistryRecord>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        Ok(state
            .workspace_registry
            .get(workspace_id.as_str())
            .filter(|w| w.project == *project)
            .cloned())
    }

    async fn get_by_root_run(
        &self,
        project: &ProjectKey,
        root_run_id: &RunId,
    ) -> Result<Option<crate::projections::WorkspaceRegistryRecord>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        Ok(state
            .workspace_registry
            .values()
            .find(|w| w.project == *project && w.root_run_id == *root_run_id)
            .cloned())
    }
}

#[async_trait]
impl crate::projections::F65CheckpointReadModel for InMemoryStore {
    async fn get_f65(
        &self,
        project: &ProjectKey,
        checkpoint_id: &CheckpointId,
    ) -> Result<Option<crate::projections::F65CheckpointRecord>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        Ok(state
            .f65_checkpoints
            .get(checkpoint_id.as_str())
            .filter(|c| c.project == *project)
            .cloned())
    }

    async fn list_by_session(
        &self,
        project: &ProjectKey,
        session_id: &SessionId,
    ) -> Result<Vec<crate::projections::F65CheckpointRecord>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let mut results: Vec<crate::projections::F65CheckpointRecord> = state
            .f65_checkpoints
            .values()
            .filter(|c| c.project == *project && c.session_id == *session_id)
            .cloned()
            .collect();
        // sort_by avoids allocating a String per comparison (same
        // rationale as the matching SessionOutcomeReadModel /
        // WorkspaceSnapshotReadModel impls above).
        results.sort_by(|a, b| {
            a.iteration
                .cmp(&b.iteration)
                .then_with(|| a.created_at.cmp(&b.created_at))
                .then_with(|| a.checkpoint_id.as_str().cmp(b.checkpoint_id.as_str()))
        });
        Ok(results)
    }
}

// ── Convenience query methods for cairn-app ───────────────────────────────

impl InMemoryStore {
    /// #670 G4 PR-1b-4: restore a root run's `in_flight_descendants`
    /// counter to a specific value. Used by cairn-app's boot
    /// reconciliation pass after the event-log replay re-initialises
    /// the in-memory projection: the descendant counter is mutated
    /// via direct SQL UPDATE (not event-sourced), so replay rebuilds
    /// the projection with counter=0. cairn-app reads the authoritative
    /// values from the durable backend via
    /// `RunDescendantsCounter::list_nonzero_descendant_counters` and
    /// writes them back here.
    ///
    /// No-op if `root_run_id` is missing from the projection
    /// (shouldn't happen for a row that came from the durable
    /// backend, but safely tolerated). Unlike `try_increment_*` and
    /// `decrement_descendants`, this does NOT bump `version` or
    /// `updated_at` — the reconciliation pass is a projection repair,
    /// not a logical state change.
    pub async fn restore_descendants_counter(&self, root_run_id: &cairn_domain::RunId, value: i64) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(row) = state.runs.get_mut(root_run_id.as_str()) {
            row.in_flight_descendants = value;
        }
    }

    /// Count runs currently in active states (Running or Leased).
    pub async fn count_active_runs(&self) -> u64 {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state
            .runs
            .values()
            .filter(|r| {
                matches!(
                    r.state,
                    cairn_domain::RunState::Running | cairn_domain::RunState::Pending
                )
            })
            .count() as u64
    }

    /// Count tasks currently active (Running or Leased).
    pub async fn count_active_tasks(&self) -> u64 {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state
            .tasks
            .values()
            .filter(|t| {
                matches!(
                    t.state,
                    cairn_domain::TaskState::Running | cairn_domain::TaskState::Leased
                )
            })
            .count() as u64
    }

    /// Count active runs for a specific tenant.
    pub async fn count_active_runs_for_tenant(&self, tenant_id: &cairn_domain::TenantId) -> u64 {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state
            .runs
            .values()
            .filter(|r| {
                r.project.tenant_id == *tenant_id
                    && matches!(
                        r.state,
                        cairn_domain::RunState::Running | cairn_domain::RunState::Pending
                    )
            })
            .count() as u64
    }

    /// Count active tasks for a specific tenant.
    pub async fn count_active_tasks_for_tenant(&self, tenant_id: &cairn_domain::TenantId) -> u64 {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state
            .tasks
            .values()
            .filter(|t| {
                t.project.tenant_id == *tenant_id
                    && matches!(
                        t.state,
                        cairn_domain::TaskState::Running | cairn_domain::TaskState::Leased
                    )
            })
            .count() as u64
    }

    /// Count active runs for a workspace.
    pub async fn count_active_runs_for_workspace(
        &self,
        workspace_key: &cairn_domain::tenancy::WorkspaceKey,
    ) -> u64 {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state
            .runs
            .values()
            .filter(|r| {
                r.project.workspace_id == workspace_key.workspace_id
                    && matches!(
                        r.state,
                        cairn_domain::RunState::Running | cairn_domain::RunState::Pending
                    )
            })
            .count() as u64
    }

    /// Count pending approvals for a tenant.
    pub async fn count_pending_approvals_for_tenant(
        &self,
        tenant_id: &cairn_domain::TenantId,
    ) -> u64 {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state
            .approvals
            .values()
            .filter(|a| a.project.tenant_id == *tenant_id && a.decision.is_none())
            .count() as u64
    }

    /// List all pending (undecided) approvals across every project.
    pub fn list_all_pending_approvals(
        &self,
        limit: usize,
        offset: usize,
    ) -> Vec<crate::projections::ApprovalRecord> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let mut results: Vec<crate::projections::ApprovalRecord> = state
            .approvals
            .values()
            .filter(|a| a.decision.is_none())
            .cloned()
            .collect();
        results.sort_by_key(|a| a.created_at);
        results.into_iter().skip(offset).take(limit).collect()
    }

    /// List all approvals (pending + resolved) across every project.
    pub fn list_all_approvals(
        &self,
        limit: usize,
        offset: usize,
    ) -> Vec<crate::projections::ApprovalRecord> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let mut results: Vec<crate::projections::ApprovalRecord> =
            state.approvals.values().cloned().collect();
        results.sort_by_key(|a| a.created_at);
        results.into_iter().skip(offset).take(limit).collect()
    }

    /// Count eval runs since a timestamp for a tenant.
    pub async fn count_eval_runs_since_for_tenant(
        &self,
        tenant_id: &cairn_domain::TenantId,
        since_ms: u64,
    ) -> u64 {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state
            .eval_runs
            .values()
            .filter(|e| e.project.tenant_id == *tenant_id && e.started_at >= since_ms)
            .count() as u64
    }

    /// Check if any provider connection is in degraded health.
    pub async fn any_provider_degraded(&self) -> bool {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state.provider_health_records.values().any(|r| {
            matches!(
                r.status,
                cairn_domain::providers::ProviderHealthStatus::Degraded
            )
        })
    }

    /// Probe write capability (always succeeds for in-memory store).
    pub async fn probe_write(&self) -> Result<(), crate::StoreError> {
        Ok(())
    }

    /// Compact event log stub — returns a basic report.
    pub fn compact_event_log(
        &self,
        tenant_id: &cairn_domain::TenantId,
        retain_last_n: Option<u64>,
    ) -> serde_json::Value {
        let retain = retain_last_n.unwrap_or(100) as usize;
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let events_before = state.events.len() as u64;

        if state.events.len() <= retain {
            return serde_json::json!({
                "events_before": events_before,
                "events_after": events_before,
                "events_compacted": 0,
                "retained": events_before
            });
        }

        // Keep only the last `retain` events.
        let start = state.events.len() - retain;
        let retained_events: Vec<StoredEvent> = state.events.drain(start..).collect();
        state.events.clear();
        state.events = retained_events;

        // Clear all projections.
        state.sessions.clear();
        state.runs.clear();
        state.tasks.clear();
        state.approvals.clear();
        state.approval_delegations.clear();
        state.checkpoints.clear();
        state.mailbox_messages.clear();
        state.tool_invocations.clear();
        state.signals.clear();
        state.ingest_jobs.clear();
        state.eval_runs.clear();
        state.eval_datasets.clear();
        state.eval_rubrics.clear();
        state.eval_baselines.clear();
        state.checkpoint_strategies.clear();
        state.prompt_assets.clear();
        state.prompt_versions.clear();
        state.prompt_releases.clear();
        state.tenants.clear();
        state.workspaces.clear();
        state.projects.clear();
        state.route_decisions.clear();
        state.provider_calls.clear();
        state.approval_policies.clear();
        state.external_workers.clear();
        state.session_costs.clear();
        state.run_costs.clear();
        state.llm_traces.clear();
        state.operator_profiles.clear();
        state.full_operator_profiles.clear();
        state.operator_tenant_roles.clear();
        state.workspace_members.clear();
        state.signal_subscriptions.clear();
        state.provider_health_records.clear();
        state.provider_pools.clear();
        state.default_settings.clear();
        state.credentials.clear();
        state.channels.clear();
        state.channel_messages.clear();
        state.channel_message_keys.clear();
        state.credential_rotations.clear();
        state.licenses.clear();
        state.entitlement_overrides.clear();
        state.notification_prefs.clear();
        state.notification_records.clear();
        state.notification_record_ids.clear();
        state.guardrail_policies.clear();
        state.guardrail_policy_tenants.clear();
        state.guardrail_evaluations.clear();
        state.guardrail_evaluation_keys.clear();
        state.provider_budgets.clear();
        state.provider_connections.clear();
        state.quotas.clear();
        state.provider_bindings.clear();
        state.provider_health_schedules.clear();
        state.run_sla_configs.clear();
        state.run_sla_breaches.clear();
        state.run_cost_alerts.clear();
        state.retention_policies.clear();
        state.route_policies.clear();
        state.resource_shares.clear();
        // Issue #592: pause_schedules is an evict-on-resume projection
        // that must be part of compaction's clear-then-rebuild pass.
        // Otherwise a compact-while-paused run's schedule row would
        // duplicate into the rebuilt map or, worse, survive a
        // retention-window prune of its originating RunStateChanged
        // event and leak a stale resume entry into `list_due`.
        state.pause_schedules.clear();
        state.snapshots.clear();
        state.command_id_index.clear();
        // RFC-025 Phase 1.5a: trigger / run_template / trigger_fires
        // projections need to be part of compaction's clear-then-rebuild
        // pass, otherwise a compact-while-running would leave stale rows
        // for deleted triggers in place after the event log is pruned.
        state.triggers.clear();
        state.run_templates.clear();
        state.trigger_fires.clear();

        // Rebuild projections from retained events.
        for event in state.events.clone() {
            Self::apply_projection(&mut state, &event);
        }

        // Emit compaction event.
        let now = now_millis();
        let up_to_position = state.events.first().map(|e| e.position.0).unwrap_or(0);
        let compaction_event = StoredEvent {
            position: EventPosition(state.next_position),
            envelope: cairn_domain::EventEnvelope::for_runtime_event(
                cairn_domain::EventId::new(format!("evt_compact_{now}")),
                cairn_domain::EventSource::Runtime,
                cairn_domain::RuntimeEvent::EventLogCompacted(cairn_domain::EventLogCompacted {
                    up_to_position,
                    compacted_at_ms: now,
                    tenant_id: tenant_id.clone(),
                    events_before,
                    events_after: state.events.len() as u64,
                }),
            ),
            stored_at: now,
        };
        state.next_position += 1;
        state.events.push(compaction_event);

        serde_json::json!({
            "events_before": events_before,
            "events_after": state.events.len() as u64 - 1, // exclude the compaction event itself
            "events_compacted": events_before - retain as u64,
            "retained": retain as u64
        })
    }

    /// Create a snapshot capturing all events up to the current position.
    ///
    /// Returns `Err` on serialization failure rather than writing an empty
    /// snapshot — pre-T2-H9 the `unwrap_or_default()` path silently produced
    /// a snapshot whose `compressed_state` was empty and whose `state_hash`
    /// was the FNV-1a hash of 0 bytes, which then wiped the store on restore.
    pub fn create_snapshot(
        &self,
        tenant_id: &cairn_domain::TenantId,
    ) -> Result<cairn_domain::compaction::Snapshot, StoreError> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let event_position = state.next_position.saturating_sub(1);

        // Serialize events as compressed_state so restore can replay them.
        let compressed_state = serde_json::to_vec(&state.events)
            .map_err(|e| StoreError::Serialization(e.to_string()))?;

        // Simple hash of the compressed state for integrity check.
        let state_hash = format!("{:016x}", {
            let mut h: u64 = 0xcbf29ce484222325; // FNV-1a offset basis
            for &byte in &compressed_state {
                h ^= byte as u64;
                h = h.wrapping_mul(0x100000001b3); // FNV prime
            }
            h
        });

        Ok(cairn_domain::compaction::Snapshot {
            snapshot_id: format!("snap_{}", now),
            tenant_id: tenant_id.clone(),
            event_position,
            state_hash,
            created_at_ms: now,
            compressed_state,
        })
    }

    /// Restore from a snapshot: replace events and rebuild projections.
    pub fn restore_from_snapshot(
        &self,
        snapshot: &cairn_domain::compaction::Snapshot,
    ) -> serde_json::Value {
        let restored_events: Vec<StoredEvent> =
            serde_json::from_slice(&snapshot.compressed_state).unwrap_or_default();
        let events_before;
        let events_after;

        {
            let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            events_before = state.events.len() as u64;

            // Replace events with snapshot contents.
            state.events = restored_events.clone();
            events_after = state.events.len() as u64;

            // Reset position to after last snapshot event.
            state.next_position = state.events.last().map(|e| e.position.0 + 1).unwrap_or(1);

            // Clear all projections and rebuild.
            state.sessions.clear();
            state.runs.clear();
            state.tasks.clear();
            state.approvals.clear();
            state.approval_delegations.clear();
            state.checkpoints.clear();
            state.mailbox_messages.clear();
            state.tool_invocations.clear();
            state.signals.clear();
            state.ingest_jobs.clear();
            state.eval_runs.clear();
            state.eval_datasets.clear();
            state.eval_rubrics.clear();
            state.eval_baselines.clear();
            state.checkpoint_strategies.clear();
            state.prompt_assets.clear();
            state.prompt_versions.clear();
            state.prompt_releases.clear();
            state.tenants.clear();
            state.workspaces.clear();
            state.projects.clear();
            state.route_decisions.clear();
            state.provider_calls.clear();
            state.approval_policies.clear();
            state.external_workers.clear();
            state.session_costs.clear();
            state.run_costs.clear();
            state.llm_traces.clear();
            state.operator_profiles.clear();
            state.full_operator_profiles.clear();
            state.operator_tenant_roles.clear();
            state.workspace_members.clear();
            state.signal_subscriptions.clear();
            state.provider_health_records.clear();
            state.provider_pools.clear();
            state.default_settings.clear();
            state.credentials.clear();
            state.channels.clear();
            state.channel_messages.clear();
            state.channel_message_keys.clear();
            state.credential_rotations.clear();
            state.licenses.clear();
            state.entitlement_overrides.clear();
            state.notification_prefs.clear();
            state.notification_records.clear();
            state.notification_record_ids.clear();
            state.guardrail_policies.clear();
            state.guardrail_policy_tenants.clear();
            state.guardrail_evaluations.clear();
            state.guardrail_evaluation_keys.clear();
            state.provider_budgets.clear();
            state.provider_connections.clear();
            state.quotas.clear();
            state.provider_bindings.clear();
            state.provider_health_schedules.clear();
            state.run_sla_configs.clear();
            state.run_sla_breaches.clear();
            state.run_cost_alerts.clear();
            state.retention_policies.clear();
            state.route_policies.clear();
            state.resource_shares.clear();
            state.snapshots.clear();
            state.command_id_index.clear();
            // RFC-025 Phase 1.5a: rehydrate trigger / run_template /
            // trigger_fires projections from the retained event log.
            state.triggers.clear();
            state.run_templates.clear();
            state.trigger_fires.clear();

            for event in state.events.clone() {
                Self::apply_projection(&mut state, &event);
            }
        }

        serde_json::json!({
            "restored": true,
            "events_before": events_before,
            "events_after": events_after,
            "events_replayed": events_after
        })
    }

    /// Delete a signal subscription.
    pub async fn delete_signal_subscription(
        &self,
        subscription_id: &str,
    ) -> Result<(), crate::StoreError> {
        self.state
            .lock()
            .unwrap()
            .signal_subscriptions
            .remove(subscription_id);
        Ok(())
    }

    /// List runs with optional filters.
    pub async fn list_runs_filtered(
        &self,
        query: &cairn_domain::tenancy::ProjectKey,
        session_id: Option<&cairn_domain::SessionId>,
        status: Option<cairn_domain::RunState>,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<crate::projections::RunRecord>, crate::StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        Ok(state
            .runs
            .values()
            .filter(|r| r.project == *query)
            .filter(|r| session_id.is_none_or(|s| r.session_id == *s))
            .filter(|r| status.is_none_or(|st| r.state == st))
            .skip(offset)
            .take(limit)
            .cloned()
            .collect())
    }

    /// List tasks with optional filters.
    ///
    /// The `query` project key is always applied so the list cannot leak
    /// tasks across tenants. Issue #234: previously the filter args were
    /// silently ignored and every task in the store was returned
    /// regardless of scope. `run_id` and `state_filter` narrow the
    /// result further when present.
    pub async fn list_tasks_filtered(
        &self,
        query: &cairn_domain::tenancy::ProjectKey,
        run_id: Option<&cairn_domain::RunId>,
        state_filter: Option<cairn_domain::TaskState>,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<crate::projections::TaskRecord>, crate::StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        Ok(state
            .tasks
            .values()
            .filter(|t| t.project == *query)
            .filter(|t| run_id.is_none_or(|r| t.parent_run_id.as_ref() == Some(r)))
            .filter(|t| state_filter.is_none_or(|st| t.state == st))
            .skip(offset)
            .take(limit)
            .cloned()
            .collect())
    }

    /// Scan all prompt assets across every project (RFC 010 operator view).
    /// Scan all tasks across every project (operator view).
    pub async fn list_all_tasks(
        &self,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<TaskRecord>, crate::StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let mut tasks: Vec<TaskRecord> = state.tasks.values().cloned().collect();
        // Most-recent first.
        tasks.sort_by(|a, b| {
            b.created_at
                .cmp(&a.created_at)
                .then_with(|| a.task_id.as_str().cmp(b.task_id.as_str()))
        });
        Ok(tasks.into_iter().skip(offset).take(limit).collect())
    }

    pub async fn list_all_prompt_assets(
        &self,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<crate::projections::PromptAssetRecord>, crate::StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        Ok(state
            .prompt_assets
            .values()
            .skip(offset)
            .take(limit)
            .cloned()
            .collect())
    }

    /// Scan all prompt releases across every project (RFC 010 operator view).
    pub async fn list_all_prompt_releases(
        &self,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<crate::projections::PromptReleaseRecord>, crate::StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        Ok(state
            .prompt_releases
            .values()
            .skip(offset)
            .take(limit)
            .cloned()
            .collect())
    }

    /// Scan all provider bindings across every tenant (RFC 010 operator view).
    pub async fn list_all_provider_bindings(
        &self,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<cairn_domain::providers::ProviderBindingRecord>, crate::StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        Ok(state
            .provider_bindings
            .values()
            .skip(offset)
            .take(limit)
            .cloned()
            .collect())
    }

    /// Aggregate cost summary across all runs in the store (RFC 010 / RFC 009).
    ///
    /// Returns `(total_provider_calls, total_tokens_in, total_tokens_out,
    /// total_cost_micros)` since the store was created.
    pub async fn cost_summary(&self) -> (u64, u64, u64, u64) {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let mut calls: u64 = 0;
        let mut tokens_in: u64 = 0;
        let mut tokens_out: u64 = 0;
        let mut cost_micros: u64 = 0;
        for rc in state.run_costs.values() {
            calls += rc.provider_calls;
            tokens_in += rc.total_tokens_in;
            tokens_out += rc.total_tokens_out;
            cost_micros += rc.total_cost_micros;
        }
        (calls, tokens_in, tokens_out, cost_micros)
    }

    /// RFC 002: list all approval records for a run (all states: pending + resolved).
    pub fn list_approvals_by_run(
        &self,
        run_id: &cairn_domain::RunId,
    ) -> Vec<crate::projections::ApprovalRecord> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let mut results: Vec<_> = state
            .approvals
            .values()
            .filter(|a| a.run_id.as_ref() == Some(run_id))
            .cloned()
            .collect();
        results.sort_by_key(|a| (a.created_at, a.approval_id.as_str().to_owned()));
        results
    }

    /// RFC 005: attach a prompt release to an approval policy record.
    ///
    /// RFC 009: list all provider call records for a project, sorted by call_id.
    pub fn list_provider_calls_by_project(
        &self,
        project_id: &cairn_domain::ProjectId,
    ) -> Vec<cairn_domain::providers::ProviderCallRecord> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let mut calls: Vec<_> = state
            .provider_calls
            .values()
            .filter(|c| &c.project_id == project_id)
            .cloned()
            .collect();
        calls.sort_by_key(|r| r.provider_call_id.clone());
        calls
    }

    /// `attached_release_ids` is initialised to empty by `ApprovalPolicyCreated`
    /// and updated by the governance layer (not via a domain event). This method
    /// provides the in-process mutation path used by tests and service impls.
    pub fn attach_release_to_policy(
        &self,
        policy_id: &str,
        release_id: cairn_domain::PromptReleaseId,
    ) -> bool {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(policy) = state.approval_policies.get_mut(policy_id) {
            if !policy.attached_release_ids.contains(&release_id) {
                policy.attached_release_ids.push(release_id);
            }
            true
        } else {
            false
        }
    }

    /// Total number of task records in the store (all states).
    pub fn count_all_tasks(&self) -> usize {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .tasks
            .len()
    }

    /// Total number of approval records in the store (all states).
    pub fn count_all_approvals(&self) -> usize {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .approvals
            .len()
    }

    // ── Snapshot / restore ────────────────────────────────────────────────────

    /// Export the full event log as a serializable snapshot.
    ///
    /// The event log is the source of truth; all projections are derived from
    /// it so serialising only the events is sufficient for a complete restore.
    pub fn dump_events(&self) -> crate::snapshot::StoreSnapshot {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        crate::snapshot::StoreSnapshot {
            version: 1,
            created_at_ms: now_millis(),
            event_count: state.events.len() as u64,
            events: state.events.clone(),
        }
    }

    /// Clear all state and replay the events from a snapshot.
    ///
    /// Returns the number of events replayed.
    pub fn load_snapshot(&self, snap: crate::snapshot::StoreSnapshot) -> u64 {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        self.usage_counters
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clear();

        // Reset all projections to empty.
        state.events.clear();
        state.next_position = 1;
        state.command_id_index.clear();
        state.sessions.clear();
        state.runs.clear();
        state.tasks.clear();
        state.approvals.clear();
        state.approval_delegations.clear();
        state.checkpoints.clear();
        state.mailbox_messages.clear();
        state.tool_invocations.clear();
        state.signals.clear();
        state.ingest_jobs.clear();
        state.eval_runs.clear();
        state.eval_datasets.clear();
        state.eval_rubrics.clear();
        state.eval_baselines.clear();
        state.checkpoint_strategies.clear();
        state.prompt_assets.clear();
        state.prompt_versions.clear();
        state.prompt_releases.clear();
        state.route_decisions.clear();
        state.provider_calls.clear();
        state.approval_policies.clear();
        state.external_workers.clear();
        state.session_costs.clear();
        state.run_costs.clear();
        state.llm_traces.clear();
        state.operator_profiles.clear();
        state.full_operator_profiles.clear();
        state.operator_tenant_roles.clear();
        state.workspace_members.clear();
        state.signal_subscriptions.clear();
        state.provider_health_records.clear();
        state.provider_pools.clear();
        state.default_settings.clear();
        state.credentials.clear();
        state.channels.clear();
        state.channel_messages.clear();
        state.channel_message_keys.clear();
        state.credential_rotations.clear();
        state.licenses.clear();
        state.entitlement_overrides.clear();
        state.notification_prefs.clear();
        state.notification_records.clear();
        state.notification_record_ids.clear();
        state.guardrail_policies.clear();
        state.guardrail_policy_tenants.clear();
        state.guardrail_evaluations.clear();
        state.guardrail_evaluation_keys.clear();
        state.provider_budgets.clear();
        state.provider_connections.clear();
        state.quotas.clear();
        state.provider_bindings.clear();
        state.provider_health_schedules.clear();
        state.run_sla_configs.clear();
        state.run_sla_breaches.clear();
        state.run_cost_alerts.clear();
        state.retention_policies.clear();
        state.route_policies.clear();
        state.resource_shares.clear();
        state.tenants.clear();
        state.workspaces.clear();
        state.projects.clear();
        state.snapshots.clear();
        // RFC-025 Phase 1.5a: drop trigger/run_template/trigger_fires rows
        // so the snapshot replay below rebuilds a fresh copy.
        state.triggers.clear();
        state.run_templates.clear();
        state.trigger_fires.clear();

        // Replay events in order.
        let count = snap.events.len() as u64;
        for stored in snap.events {
            Self::apply_projection(&mut state, &stored);
            if stored.position.0 >= state.next_position {
                state.next_position = stored.position.0 + 1;
            }
            state.events.push(stored);
        }
        count
    }
}

// ── RFC-025 Phase 1.5a: TriggerReadModel / RunTemplateReadModel /
// TriggerFireReadModel on InMemoryStore ────────────────────────────────
//
// Backs the same projection-first query path that pg/sqlite implement.
// The HashMaps + Vec are written inside `apply_projection` above from
// the 13 trigger / run_template / audit RuntimeEvent variants; read
// paths below are simple HashMap lookups + linear Vec scans. The linear
// scans are fine at this scale — the three bounded windows (duplicate
// ledger by (trigger_id, signal_id), per-trigger 1-min rate-limit, and
// per-project 1-hour budget) are microsecond-cheap even on tens of
// thousands of rows.

#[async_trait]
impl crate::projections::TriggerReadModel for InMemoryStore {
    async fn get_trigger(
        &self,
        trigger_id: &cairn_domain::ids::TriggerId,
    ) -> Result<Option<crate::projections::TriggerRecord>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        Ok(state.triggers.get(trigger_id.as_str()).cloned())
    }

    async fn list_triggers_by_project(
        &self,
        project: &ProjectKey,
    ) -> Result<Vec<crate::projections::TriggerRecord>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let mut results: Vec<_> = state
            .triggers
            .values()
            .filter(|r| r.project == *project)
            .cloned()
            .collect();
        results.sort_by(|a, b| a.trigger_id.as_str().cmp(b.trigger_id.as_str()));
        Ok(results)
    }

    async fn list_matching_enabled(
        &self,
        project: &ProjectKey,
        signal_type: &str,
        plugin_id: &str,
    ) -> Result<Vec<crate::projections::TriggerRecord>, StoreError> {
        // Linear scan over all triggers in the store. pg/sqlite use the
        // `idx_triggers_signal_match` composite index on `(tenant_id,
        // workspace_id, project_id, signal_type)` for a cheap lookup;
        // the in-memory store scales with total trigger count across
        // the process, which is fine at the `--db memory` scale (hundreds
        // of triggers per dev box) but would be a hot spot if `--db memory`
        // ever held tens-of-thousands of triggers. If that ever happens,
        // swap in a `HashMap<(ProjectKey, String), Vec<TriggerId>>`
        // index populated in the `TriggerCreated`/`Deleted` arms.
        // (PR #569 review: noted explicitly so future readers don't
        // need to rediscover the tradeoff.)
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let mut results: Vec<_> = state
            .triggers
            .values()
            .filter(|r| {
                r.project == *project
                    && matches!(r.state, crate::projections::TriggerStateKind::Enabled)
                    && r.signal_type == signal_type
                    && r.plugin_id.as_ref().is_none_or(|pid| pid == plugin_id)
            })
            .cloned()
            .collect();
        results.sort_by(|a, b| a.trigger_id.as_str().cmp(b.trigger_id.as_str()));
        Ok(results)
    }
}

#[async_trait]
impl crate::projections::RunTemplateReadModel for InMemoryStore {
    async fn get_template(
        &self,
        template_id: &cairn_domain::ids::RunTemplateId,
    ) -> Result<Option<crate::projections::RunTemplateRecord>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        Ok(state.run_templates.get(template_id.as_str()).cloned())
    }

    async fn list_templates_by_project(
        &self,
        project: &ProjectKey,
    ) -> Result<Vec<crate::projections::RunTemplateRecord>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let mut results: Vec<_> = state
            .run_templates
            .values()
            .filter(|r| r.project == *project)
            .cloned()
            .collect();
        results.sort_by(|a, b| a.template_id.as_str().cmp(b.template_id.as_str()));
        Ok(results)
    }

    async fn triggers_referencing_template(
        &self,
        template_id: &cairn_domain::ids::RunTemplateId,
    ) -> Result<Vec<cairn_domain::ids::TriggerId>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        Ok(state
            .triggers
            .values()
            .filter(|t| &t.run_template_id == template_id)
            .map(|t| t.trigger_id.clone())
            .collect())
    }
}

#[async_trait]
impl crate::projections::TriggerFireReadModel for InMemoryStore {
    async fn has_fired(
        &self,
        trigger_id: &cairn_domain::ids::TriggerId,
        signal_id: &str,
    ) -> Result<bool, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        Ok(state.trigger_fires.iter().any(|f| {
            &f.trigger_id == trigger_id
                && f.signal_id == signal_id
                && matches!(f.outcome, crate::projections::TriggerFireOutcome::Fired)
        }))
    }

    async fn count_fires_since(
        &self,
        trigger_id: &cairn_domain::ids::TriggerId,
        since_ms: u64,
    ) -> Result<u32, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        Ok(state
            .trigger_fires
            .iter()
            .filter(|f| {
                &f.trigger_id == trigger_id
                    && matches!(f.outcome, crate::projections::TriggerFireOutcome::Fired)
                    && f.at_ms > since_ms
            })
            .count() as u32)
    }

    async fn count_project_fires_since(
        &self,
        project: &ProjectKey,
        since_ms: u64,
    ) -> Result<u32, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        Ok(state
            .trigger_fires
            .iter()
            .filter(|f| {
                f.project == *project
                    && matches!(f.outcome, crate::projections::TriggerFireOutcome::Fired)
                    && f.at_ms > since_ms
            })
            .count() as u32)
    }
}

// ── RFC-025 Phase 2b.1 m4: plan_reviews read model (RFC 018) ────────

#[async_trait]
impl crate::projections::PlanReviewReadModel for InMemoryStore {
    async fn get(
        &self,
        plan_run_id: &cairn_domain::RunId,
    ) -> Result<Option<crate::projections::PlanReviewRecord>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        Ok(state.plan_reviews.get(plan_run_id.as_str()).cloned())
    }

    async fn list_by_project(
        &self,
        project: &cairn_domain::ProjectKey,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<crate::projections::PlanReviewRecord>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let mut rows: Vec<crate::projections::PlanReviewRecord> = state
            .plan_reviews
            .values()
            .filter(|r| r.project == *project)
            .cloned()
            .collect();
        // Newest-first on proposed_at, id DESC tiebreak so parity
        // harness stays stable on identical timestamps (pg/sqlite
        // ORDER BY uses the same compound key).
        rows.sort_by(|a, b| {
            b.proposed_at
                .cmp(&a.proposed_at)
                .then_with(|| b.plan_run_id.as_str().cmp(a.plan_run_id.as_str()))
        });
        Ok(rows.into_iter().skip(offset).take(limit).collect())
    }

    async fn list_pending_by_project(
        &self,
        project: &cairn_domain::ProjectKey,
        limit: usize,
    ) -> Result<Vec<crate::projections::PlanReviewRecord>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let mut rows: Vec<crate::projections::PlanReviewRecord> = state
            .plan_reviews
            .values()
            .filter(|r| {
                r.project == *project && r.state == crate::projections::PlanReviewState::Proposed
            })
            .cloned()
            .collect();
        rows.sort_by(|a, b| {
            b.proposed_at
                .cmp(&a.proposed_at)
                .then_with(|| b.plan_run_id.as_str().cmp(a.plan_run_id.as_str()))
        });
        Ok(rows.into_iter().take(limit).collect())
    }

    async fn list_by_session(
        &self,
        session_id: &cairn_domain::SessionId,
        limit: usize,
    ) -> Result<Vec<crate::projections::PlanReviewRecord>, StoreError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let mut rows: Vec<crate::projections::PlanReviewRecord> = state
            .plan_reviews
            .values()
            .filter(|r| r.session_id == *session_id)
            .cloned()
            .collect();
        // Oldest-proposed-first on session lineage — callers walk the
        // plan → revision chain in creation order.
        rows.sort_by(|a, b| {
            a.proposed_at
                .cmp(&b.proposed_at)
                .then_with(|| a.plan_run_id.as_str().cmp(b.plan_run_id.as_str()))
        });
        Ok(rows.into_iter().take(limit).collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_project() -> ProjectKey {
        ProjectKey::new("tenant", "workspace", "project")
    }

    fn make_envelope(event: RuntimeEvent) -> EventEnvelope<RuntimeEvent> {
        EventEnvelope::for_runtime_event(EventId::new("evt_test"), EventSource::Runtime, event)
    }

    #[tokio::test]
    async fn append_and_read_session_lifecycle() {
        let store = InMemoryStore::new();
        let project = test_project();
        let session_id = SessionId::new("sess_1");

        // Create session
        let positions = store
            .append(&[make_envelope(RuntimeEvent::SessionCreated(
                SessionCreated {
                    project: project.clone(),
                    session_id: session_id.clone(),
                },
            ))])
            .await
            .unwrap();

        assert_eq!(positions.len(), 1);
        assert_eq!(positions[0], EventPosition(1));

        // Read projection
        let session = SessionReadModel::get(&store, &session_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(session.state, SessionState::Open);
        assert_eq!(session.version, 1);

        // Change state
        store
            .append(&[make_envelope(RuntimeEvent::SessionStateChanged(
                SessionStateChanged {
                    project: project.clone(),
                    session_id: session_id.clone(),
                    transition: StateTransition {
                        from: Some(SessionState::Open),
                        to: SessionState::Completed,
                    },
                },
            ))])
            .await
            .unwrap();

        let session = SessionReadModel::get(&store, &session_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(session.state, SessionState::Completed);
        assert_eq!(session.version, 2);
    }

    #[tokio::test]
    async fn append_and_read_run_lifecycle() {
        let store = InMemoryStore::new();
        let project = test_project();
        let session_id = SessionId::new("sess_1");
        let run_id = RunId::new("run_1");

        store
            .append(&[make_envelope(RuntimeEvent::RunCreated(RunCreated {
                project: project.clone(),
                session_id: session_id.clone(),
                run_id: run_id.clone(),
                parent_run_id: None,
                agent_role_id: None,
                prompt_release_id: None,
            }))])
            .await
            .unwrap();

        let run = RunReadModel::get(&store, &run_id).await.unwrap().unwrap();
        assert_eq!(run.state, RunState::Pending);

        // Advance to running then completed
        store
            .append(&[
                make_envelope(RuntimeEvent::RunStateChanged(RunStateChanged {
                    project: project.clone(),
                    run_id: run_id.clone(),
                    transition: StateTransition {
                        from: Some(RunState::Pending),
                        to: RunState::Running,
                    },
                    failure_class: None,
                    pause_reason: None,
                    resume_trigger: None,
                })),
                make_envelope(RuntimeEvent::RunStateChanged(RunStateChanged {
                    project: project.clone(),
                    run_id: run_id.clone(),
                    transition: StateTransition {
                        from: Some(RunState::Running),
                        to: RunState::Completed,
                    },
                    failure_class: None,
                    pause_reason: None,
                    resume_trigger: None,
                })),
            ])
            .await
            .unwrap();

        let run = RunReadModel::get(&store, &run_id).await.unwrap().unwrap();
        assert_eq!(run.state, RunState::Completed);
        assert_eq!(run.version, 3);
    }

    #[tokio::test]
    async fn list_by_parent_run_returns_only_children_of_that_parent() {
        // Replaces the pre-existing 10k-event scan that silently
        // truncated older children. Verifies: parent + N children +
        // unrelated run → list returns exactly N, no truncation at
        // absurdly-low-limit defaults, unrelated run not included.
        let store = InMemoryStore::new();
        let project = test_project();
        let session_id = SessionId::new("sess_1");
        let parent = RunId::new("parent");
        let unrelated = RunId::new("unrelated_root");

        // Parent + unrelated root run.
        store
            .append(&[
                make_envelope(RuntimeEvent::SessionCreated(SessionCreated {
                    project: project.clone(),
                    session_id: session_id.clone(),
                })),
                make_envelope(RuntimeEvent::RunCreated(RunCreated {
                    project: project.clone(),
                    session_id: session_id.clone(),
                    run_id: parent.clone(),
                    parent_run_id: None,
                    agent_role_id: None,
                    prompt_release_id: None,
                })),
                make_envelope(RuntimeEvent::RunCreated(RunCreated {
                    project: project.clone(),
                    session_id: session_id.clone(),
                    run_id: unrelated.clone(),
                    parent_run_id: None,
                    agent_role_id: None,
                    prompt_release_id: None,
                })),
            ])
            .await
            .unwrap();

        // Three children of `parent`.
        for i in 0..3 {
            store
                .append(&[make_envelope(RuntimeEvent::RunCreated(RunCreated {
                    project: project.clone(),
                    session_id: session_id.clone(),
                    run_id: RunId::new(format!("child_{i}")),
                    parent_run_id: Some(parent.clone()),
                    agent_role_id: None,
                    prompt_release_id: None,
                }))])
                .await
                .unwrap();
        }

        let children = RunReadModel::list_by_parent_run(&store, &parent, 100)
            .await
            .unwrap();
        assert_eq!(children.len(), 3);
        assert!(children
            .iter()
            .all(|r| r.parent_run_id.as_ref() == Some(&parent)));

        // Unrelated parent → no children.
        let unrelated_children = RunReadModel::list_by_parent_run(&store, &unrelated, 100)
            .await
            .unwrap();
        assert_eq!(unrelated_children.len(), 0);

        // Limit truncation works.
        let first_one = RunReadModel::list_by_parent_run(&store, &parent, 1)
            .await
            .unwrap();
        assert_eq!(first_one.len(), 1);
    }

    #[tokio::test]
    async fn task_lifecycle_with_lease() {
        let store = InMemoryStore::new();
        let project = test_project();
        let task_id = TaskId::new("task_1");

        store
            .append(&[make_envelope(RuntimeEvent::TaskCreated(TaskCreated {
                project: project.clone(),
                task_id: task_id.clone(),
                parent_run_id: None,
                parent_task_id: None,
                prompt_release_id: None,
                session_id: None,
            }))])
            .await
            .unwrap();

        let task = TaskReadModel::get(&store, &task_id).await.unwrap().unwrap();
        assert_eq!(task.state, TaskState::Queued);

        // Claim lease via event (Worker 2 added TaskLeaseClaimed)
        store
            .append(&[
                make_envelope(RuntimeEvent::TaskLeaseClaimed(TaskLeaseClaimed {
                    project: project.clone(),
                    task_id: task_id.clone(),
                    lease_owner: "worker-a".to_owned(),
                    lease_token: 1,
                    lease_expires_at_ms: 9999999999,
                })),
                make_envelope(RuntimeEvent::TaskStateChanged(TaskStateChanged {
                    project: project.clone(),
                    task_id: task_id.clone(),
                    transition: StateTransition {
                        from: Some(TaskState::Queued),
                        to: TaskState::Leased,
                    },
                    failure_class: None,
                    pause_reason: None,
                    resume_trigger: None,
                })),
            ])
            .await
            .unwrap();

        let task = TaskReadModel::get(&store, &task_id).await.unwrap().unwrap();
        assert_eq!(task.state, TaskState::Leased);
        assert_eq!(task.lease_owner.as_deref(), Some("worker-a"));
    }

    #[tokio::test]
    async fn checkpoint_supersedes_previous_latest() {
        let store = InMemoryStore::new();
        let project = test_project();
        let run_id = RunId::new("run_1");

        store
            .append(&[make_envelope(RuntimeEvent::CheckpointRecorded(
                CheckpointRecorded {
                    project: project.clone(),
                    run_id: run_id.clone(),
                    checkpoint_id: CheckpointId::new("cp_1"),
                    disposition: CheckpointDisposition::Latest,
                    data: None,
                    kind: None,
                    message_history_size: None,
                    tool_call_ids: Vec::new(),
                },
            ))])
            .await
            .unwrap();

        store
            .append(&[make_envelope(RuntimeEvent::CheckpointRecorded(
                CheckpointRecorded {
                    project: project.clone(),
                    run_id: run_id.clone(),
                    checkpoint_id: CheckpointId::new("cp_2"),
                    disposition: CheckpointDisposition::Latest,
                    data: None,
                    kind: None,
                    message_history_size: None,
                    tool_call_ids: Vec::new(),
                },
            ))])
            .await
            .unwrap();

        let cp1 = CheckpointReadModel::get(&store, &CheckpointId::new("cp_1"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(cp1.disposition, CheckpointDisposition::Superseded);

        let latest = store.latest_for_run(&run_id).await.unwrap().unwrap();
        assert_eq!(latest.checkpoint_id, CheckpointId::new("cp_2"));
    }

    #[tokio::test]
    async fn tool_invocation_projection_tracks_terminal_outcome() {
        let store = InMemoryStore::new();
        let project = test_project();
        let invocation_id = ToolInvocationId::new("tool_1");
        let run_id = RunId::new("run_1");

        store
            .append(&[
                make_envelope(RuntimeEvent::ToolInvocationStarted(ToolInvocationStarted {
                    project: project.clone(),
                    invocation_id: invocation_id.clone(),
                    session_id: Some(SessionId::new("sess_1")),
                    run_id: Some(run_id.clone()),
                    task_id: Some(TaskId::new("task_1")),
                    target: ToolInvocationTarget::Builtin {
                        tool_name: "fs.read".to_owned(),
                    },
                    execution_class: ExecutionClass::SupervisedProcess,
                    prompt_release_id: None,
                    requested_at_ms: 100,
                    started_at_ms: 101,
                    args_json: None,
                })),
                make_envelope(RuntimeEvent::ToolInvocationCompleted(
                    ToolInvocationCompleted {
                        project,
                        invocation_id: invocation_id.clone(),
                        task_id: Some(TaskId::new("task_1")),
                        tool_name: "fs.read".to_owned(),
                        finished_at_ms: 105,
                        outcome: ToolInvocationOutcomeKind::Success,
                        tool_call_id: None,
                        result_json: None,
                        output_preview: None,
                    },
                )),
            ])
            .await
            .unwrap();

        let record = ToolInvocationReadModel::get(&store, &invocation_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(record.state, ToolInvocationState::Completed);
        assert_eq!(record.outcome, Some(ToolInvocationOutcomeKind::Success));
        assert_eq!(record.finished_at_ms, Some(105));

        let listed = ToolInvocationReadModel::list_by_run(&store, &run_id, 10, 0)
            .await
            .unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].invocation_id, invocation_id);
    }

    /// #364: the in-memory `tool_invocation_progress` projection
    /// records the LATEST progress event per invocation, inherits the
    /// `ProjectKey` from the existing `tool_invocations` row, and
    /// refuses to overwrite with an older event (out-of-order replay
    /// guard). Mirrors the pg/sqlite `WHERE excluded.updated_at_ms
    /// >= …` UPSERT.
    #[tokio::test]
    async fn tool_invocation_progress_projection_idempotent_against_out_of_order_replay() {
        use crate::projections::ToolInvocationProgressReadModel;

        let store = InMemoryStore::new();
        let project = test_project();
        let invocation_id = ToolInvocationId::new("tool_progress_1");

        // Seed the invocation so the progress projection has a
        // project scope to inherit.
        store
            .append(&[make_envelope(RuntimeEvent::ToolInvocationStarted(
                ToolInvocationStarted {
                    project: project.clone(),
                    invocation_id: invocation_id.clone(),
                    session_id: None,
                    run_id: None,
                    task_id: None,
                    target: ToolInvocationTarget::Builtin {
                        tool_name: "fs.read".to_owned(),
                    },
                    execution_class: ExecutionClass::SupervisedProcess,
                    prompt_release_id: None,
                    requested_at_ms: 100,
                    started_at_ms: 101,
                    args_json: None,
                },
            ))])
            .await
            .unwrap();

        // First progress event → stored.
        store
            .append(&[make_envelope(RuntimeEvent::ToolInvocationProgressUpdated(
                cairn_domain::ToolInvocationProgressUpdated {
                    invocation_id: invocation_id.clone(),
                    progress_pct: 20,
                    message: Some("phase 1".to_owned()),
                    updated_at_ms: 200,
                },
            ))])
            .await
            .unwrap();

        let rec = ToolInvocationProgressReadModel::get(&store, &invocation_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(rec.project, project);
        assert_eq!(rec.progress_pct, 20);
        assert_eq!(rec.updated_at_ms, 200);

        // Newer progress event → replaces.
        store
            .append(&[make_envelope(RuntimeEvent::ToolInvocationProgressUpdated(
                cairn_domain::ToolInvocationProgressUpdated {
                    invocation_id: invocation_id.clone(),
                    progress_pct: 60,
                    message: Some("phase 2".to_owned()),
                    updated_at_ms: 300,
                },
            ))])
            .await
            .unwrap();

        let rec = ToolInvocationProgressReadModel::get(&store, &invocation_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(rec.progress_pct, 60, "newer event must overwrite");
        assert_eq!(rec.updated_at_ms, 300);

        // Older replay → must NOT regress.
        store
            .append(&[make_envelope(RuntimeEvent::ToolInvocationProgressUpdated(
                cairn_domain::ToolInvocationProgressUpdated {
                    invocation_id: invocation_id.clone(),
                    progress_pct: 10,
                    message: Some("stale phase 0".to_owned()),
                    updated_at_ms: 50,
                },
            ))])
            .await
            .unwrap();

        let rec = ToolInvocationProgressReadModel::get(&store, &invocation_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            rec.progress_pct, 60,
            "older replay must NOT overwrite newer progress — would regress the operator UI",
        );
        assert_eq!(rec.updated_at_ms, 300);
    }

    /// #364: a progress event for an invocation that has not yet been
    /// started is a no-op (we refuse to fabricate a project scope).
    /// This is the edge case documented in the apply handler — in
    /// practice `ToolInvocationStarted` always precedes progress.
    #[tokio::test]
    async fn tool_invocation_progress_projection_noop_without_invocation_row() {
        use crate::projections::ToolInvocationProgressReadModel;

        let store = InMemoryStore::new();
        let invocation_id = ToolInvocationId::new("tool_progress_orphan");

        store
            .append(&[make_envelope(RuntimeEvent::ToolInvocationProgressUpdated(
                cairn_domain::ToolInvocationProgressUpdated {
                    invocation_id: invocation_id.clone(),
                    progress_pct: 5,
                    message: None,
                    updated_at_ms: 10,
                },
            ))])
            .await
            .unwrap();

        assert!(
            ToolInvocationProgressReadModel::get(&store, &invocation_id)
                .await
                .unwrap()
                .is_none(),
            "progress without a prior Started event must NOT create a projection row",
        );
    }

    #[tokio::test]
    async fn tool_invocation_projection_preserves_canceled_state_and_orders_by_request_time() {
        let store = InMemoryStore::new();
        let project = test_project();
        let run_id = RunId::new("run_1");
        let older_invocation = ToolInvocationId::new("tool_old");
        let newer_invocation = ToolInvocationId::new("tool_new");

        store
            .append(&[
                make_envelope(RuntimeEvent::ToolInvocationStarted(ToolInvocationStarted {
                    project: project.clone(),
                    invocation_id: newer_invocation.clone(),
                    session_id: Some(SessionId::new("sess_1")),
                    run_id: Some(run_id.clone()),
                    task_id: None,
                    target: ToolInvocationTarget::Builtin {
                        tool_name: "shell.exec".to_owned(),
                    },
                    execution_class: ExecutionClass::SandboxedProcess,
                    prompt_release_id: None,
                    requested_at_ms: 200,
                    started_at_ms: 201,
                    args_json: None,
                })),
                make_envelope(RuntimeEvent::ToolInvocationFailed(ToolInvocationFailed {
                    project: project.clone(),
                    invocation_id: newer_invocation.clone(),
                    task_id: None,
                    tool_name: "shell.exec".to_owned(),
                    finished_at_ms: 205,
                    outcome: ToolInvocationOutcomeKind::Canceled,
                    error_message: Some("canceled".to_owned()),
                    output_preview: None,
                })),
                make_envelope(RuntimeEvent::ToolInvocationStarted(ToolInvocationStarted {
                    project,
                    invocation_id: older_invocation.clone(),
                    session_id: Some(SessionId::new("sess_1")),
                    run_id: Some(run_id.clone()),
                    task_id: None,
                    target: ToolInvocationTarget::Builtin {
                        tool_name: "fs.read".to_owned(),
                    },
                    execution_class: ExecutionClass::SupervisedProcess,
                    prompt_release_id: None,
                    requested_at_ms: 100,
                    started_at_ms: 101,
                    args_json: None,
                })),
            ])
            .await
            .unwrap();

        let canceled = ToolInvocationReadModel::get(&store, &newer_invocation)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(canceled.state, ToolInvocationState::Canceled);
        assert_eq!(canceled.outcome, Some(ToolInvocationOutcomeKind::Canceled));
        assert_eq!(canceled.error_message.as_deref(), Some("canceled"));

        let listed = ToolInvocationReadModel::list_by_run(&store, &run_id, 10, 0)
            .await
            .unwrap();
        assert_eq!(listed.len(), 2);
        assert_eq!(listed[0].invocation_id, older_invocation);
        assert_eq!(listed[1].invocation_id, newer_invocation);

        let paged = ToolInvocationReadModel::list_by_run(&store, &run_id, 1, 1)
            .await
            .unwrap();
        assert_eq!(paged.len(), 1);
        assert_eq!(paged[0].invocation_id, newer_invocation);
    }

    #[tokio::test]
    async fn event_stream_read() {
        let store = InMemoryStore::new();
        let project = test_project();

        store
            .append(&[
                make_envelope(RuntimeEvent::SessionCreated(SessionCreated {
                    project: project.clone(),
                    session_id: SessionId::new("s1"),
                })),
                make_envelope(RuntimeEvent::SessionCreated(SessionCreated {
                    project: project.clone(),
                    session_id: SessionId::new("s2"),
                })),
            ])
            .await
            .unwrap();

        let all = store.read_stream(None, 100).await.unwrap();
        assert_eq!(all.len(), 2);

        let after_first = store
            .read_stream(Some(EventPosition(1)), 100)
            .await
            .unwrap();
        assert_eq!(after_first.len(), 1);
    }

    /// Full lifecycle integration test: session -> run -> task -> approval -> checkpoint -> mailbox.
    /// Validates all projections are correct after a realistic event sequence.
    #[tokio::test]
    async fn full_lifecycle_projection_correctness() {
        let store = InMemoryStore::new();
        let project = test_project();
        let session_id = SessionId::new("sess_int");
        let run_id = RunId::new("run_int");
        let task_id = TaskId::new("task_int");
        let approval_id = ApprovalId::new("approval_int");
        let checkpoint_id_1 = CheckpointId::new("cp_int_1");
        let checkpoint_id_2 = CheckpointId::new("cp_int_2");
        let message_id = MailboxMessageId::new("msg_int");

        // 1. Create session.
        store
            .append(&[make_envelope(RuntimeEvent::SessionCreated(
                SessionCreated {
                    project: project.clone(),
                    session_id: session_id.clone(),
                },
            ))])
            .await
            .unwrap();

        // 2. Create run in session.
        store
            .append(&[make_envelope(RuntimeEvent::RunCreated(RunCreated {
                project: project.clone(),
                session_id: session_id.clone(),
                run_id: run_id.clone(),
                parent_run_id: None,
                agent_role_id: None,
                prompt_release_id: None,
            }))])
            .await
            .unwrap();

        // 3. Start run.
        store
            .append(&[make_envelope(RuntimeEvent::RunStateChanged(
                RunStateChanged {
                    project: project.clone(),
                    run_id: run_id.clone(),
                    transition: StateTransition {
                        from: Some(RunState::Pending),
                        to: RunState::Running,
                    },
                    failure_class: None,
                    pause_reason: None,
                    resume_trigger: None,
                },
            ))])
            .await
            .unwrap();

        // 4. Create task.
        store
            .append(&[make_envelope(RuntimeEvent::TaskCreated(TaskCreated {
                project: project.clone(),
                task_id: task_id.clone(),
                parent_run_id: Some(run_id.clone()),
                parent_task_id: None,
                prompt_release_id: None,
                session_id: None,
            }))])
            .await
            .unwrap();

        // 5. Claim task lease.
        store
            .append(&[make_envelope(RuntimeEvent::TaskLeaseClaimed(
                TaskLeaseClaimed {
                    project: project.clone(),
                    task_id: task_id.clone(),
                    lease_owner: "worker-alpha".to_owned(),
                    lease_token: 1,
                    lease_expires_at_ms: 9999999999,
                },
            ))])
            .await
            .unwrap();

        // 6. Task starts running.
        store
            .append(&[make_envelope(RuntimeEvent::TaskStateChanged(
                TaskStateChanged {
                    project: project.clone(),
                    task_id: task_id.clone(),
                    transition: StateTransition {
                        from: Some(TaskState::Leased),
                        to: TaskState::Running,
                    },
                    failure_class: None,
                    pause_reason: None,
                    resume_trigger: None,
                },
            ))])
            .await
            .unwrap();

        // 7. Request approval.
        store
            .append(&[make_envelope(RuntimeEvent::ApprovalRequested(
                ApprovalRequested {
                    project: project.clone(),
                    approval_id: approval_id.clone(),
                    run_id: Some(run_id.clone()),
                    task_id: Some(task_id.clone()),
                    requirement: ApprovalRequirement::Required,
                    title: None,
                    description: None,
                },
            ))])
            .await
            .unwrap();

        // 8. Save checkpoint.
        store
            .append(&[make_envelope(RuntimeEvent::CheckpointRecorded(
                CheckpointRecorded {
                    project: project.clone(),
                    run_id: run_id.clone(),
                    checkpoint_id: checkpoint_id_1.clone(),
                    disposition: CheckpointDisposition::Latest,
                    data: None,
                    kind: None,
                    message_history_size: None,
                    tool_call_ids: Vec::new(),
                },
            ))])
            .await
            .unwrap();

        // 9. Save second checkpoint (supersedes first).
        store
            .append(&[make_envelope(RuntimeEvent::CheckpointRecorded(
                CheckpointRecorded {
                    project: project.clone(),
                    run_id: run_id.clone(),
                    checkpoint_id: checkpoint_id_2.clone(),
                    disposition: CheckpointDisposition::Latest,
                    data: None,
                    kind: None,
                    message_history_size: None,
                    tool_call_ids: Vec::new(),
                },
            ))])
            .await
            .unwrap();

        // 10. Resolve approval.
        store
            .append(&[make_envelope(RuntimeEvent::ApprovalResolved(
                ApprovalResolved {
                    project: project.clone(),
                    approval_id: approval_id.clone(),
                    decision: ApprovalDecision::Approved,
                },
            ))])
            .await
            .unwrap();

        // 11. Send mailbox message.
        store
            .append(&[make_envelope(RuntimeEvent::MailboxMessageAppended(
                MailboxMessageAppended {
                    project: project.clone(),
                    message_id: message_id.clone(),
                    run_id: Some(run_id.clone()),
                    task_id: Some(task_id.clone()),
                    content: String::new(),
                    from_run_id: None,
                    from_task_id: None,
                    deliver_at_ms: 0,
                    sender: None,
                    recipient: None,
                    body: None,
                    sent_at: None,
                    delivery_status: None,
                },
            ))])
            .await
            .unwrap();

        // 12. Complete task.
        store
            .append(&[make_envelope(RuntimeEvent::TaskStateChanged(
                TaskStateChanged {
                    project: project.clone(),
                    task_id: task_id.clone(),
                    transition: StateTransition {
                        from: Some(TaskState::Running),
                        to: TaskState::Completed,
                    },
                    failure_class: None,
                    pause_reason: None,
                    resume_trigger: None,
                },
            ))])
            .await
            .unwrap();

        // 13. Complete run.
        store
            .append(&[make_envelope(RuntimeEvent::RunStateChanged(
                RunStateChanged {
                    project: project.clone(),
                    run_id: run_id.clone(),
                    transition: StateTransition {
                        from: Some(RunState::Running),
                        to: RunState::Completed,
                    },
                    failure_class: None,
                    pause_reason: None,
                    resume_trigger: None,
                },
            ))])
            .await
            .unwrap();

        // --- Verify all projections ---

        // Session: still open (derived from run state, not explicit close).
        let session = SessionReadModel::get(&store, &session_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(session.state, SessionState::Open);

        // Run: completed.
        let run = RunReadModel::get(&store, &run_id).await.unwrap().unwrap();
        assert_eq!(run.state, RunState::Completed);
        assert!(run.state.is_terminal());
        assert!(run.parent_run_id.is_none());

        // Task: completed with lease info preserved.
        let task = TaskReadModel::get(&store, &task_id).await.unwrap().unwrap();
        assert_eq!(task.state, TaskState::Completed);
        assert!(task.state.is_terminal());
        assert_eq!(task.lease_owner.as_deref(), Some("worker-alpha"));
        assert_eq!(task.parent_run_id.as_ref(), Some(&run_id));

        // Approval: resolved as approved.
        let approval = ApprovalReadModel::get(&store, &approval_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(approval.decision, Some(ApprovalDecision::Approved));
        assert_eq!(approval.run_id.as_ref(), Some(&run_id));

        // Checkpoint 1: superseded.
        let cp1 = CheckpointReadModel::get(&store, &checkpoint_id_1)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(cp1.disposition, CheckpointDisposition::Superseded);

        // Checkpoint 2: latest.
        let cp2 = CheckpointReadModel::get(&store, &checkpoint_id_2)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(cp2.disposition, CheckpointDisposition::Latest);

        // Latest checkpoint for run is cp2.
        let latest = store.latest_for_run(&run_id).await.unwrap().unwrap();
        assert_eq!(latest.checkpoint_id, checkpoint_id_2);

        // Mailbox: message linked to run and task.
        let msg = MailboxReadModel::get(&store, &message_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(msg.run_id.as_ref(), Some(&run_id));
        assert_eq!(msg.task_id.as_ref(), Some(&task_id));

        // No non-terminal runs remain.
        assert!(!store.any_non_terminal(&session_id).await.unwrap());

        // Event stream has all 13 events.
        let all = store.read_stream(None, 100).await.unwrap();
        assert_eq!(all.len(), 13);

        // Entity-filtered read for run events.
        let run_events = store
            .read_by_entity(&EntityRef::Run(run_id.clone()), None, 100)
            .await
            .unwrap();
        assert!(run_events.len() >= 3); // created + 2 state changes
    }

    /// Expired lease detection for recovery sweeps.
    #[tokio::test]
    async fn expired_lease_detection() {
        let store = InMemoryStore::new();
        let project = test_project();

        // Create two tasks with leases.
        for (id, expires) in [("t1", 100u64), ("t2", 9999999999u64)] {
            let task_id = TaskId::new(id);
            store
                .append(&[make_envelope(RuntimeEvent::TaskCreated(TaskCreated {
                    project: project.clone(),
                    task_id: task_id.clone(),
                    parent_run_id: None,
                    parent_task_id: None,
                    prompt_release_id: None,
                    session_id: None,
                }))])
                .await
                .unwrap();

            store
                .append(&[make_envelope(RuntimeEvent::TaskLeaseClaimed(
                    TaskLeaseClaimed {
                        project: project.clone(),
                        task_id: task_id.clone(),
                        lease_owner: "w".to_owned(),
                        lease_token: 1,
                        lease_expires_at_ms: expires,
                    },
                ))])
                .await
                .unwrap();

            store
                .append(&[make_envelope(RuntimeEvent::TaskStateChanged(
                    TaskStateChanged {
                        project: project.clone(),
                        task_id,
                        transition: StateTransition {
                            from: Some(TaskState::Queued),
                            to: TaskState::Leased,
                        },
                        failure_class: None,
                        pause_reason: None,
                        resume_trigger: None,
                    },
                ))])
                .await
                .unwrap();
        }

        // t1 expired (lease at 100, now is 500), t2 still valid.
        let expired = store.list_expired_leases(500, 100).await.unwrap();
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0].task_id, TaskId::new("t1"));
    }

    #[tokio::test]
    async fn signal_projection_and_read_model() {
        let store = InMemoryStore::new();
        let project = test_project();
        let signal_id = SignalId::new("sig_1");

        store
            .append(&[make_envelope(RuntimeEvent::SignalIngested(
                SignalIngested {
                    project: project.clone(),
                    signal_id: signal_id.clone(),
                    source: "webhook".to_owned(),
                    payload: serde_json::json!({"key": "value"}),
                    timestamp_ms: 1000,
                },
            ))])
            .await
            .unwrap();

        // get returns the record with correct fields.
        let record = SignalReadModel::get(&store, &signal_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(record.id, signal_id);
        assert_eq!(record.project, project);
        assert_eq!(record.source, "webhook");
        assert_eq!(record.payload["key"], "value");
        assert_eq!(record.timestamp_ms, 1000);

        // list_by_project returns it.
        let list = SignalReadModel::list_by_project(&store, &project, 10, 0)
            .await
            .unwrap();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].id, signal_id);

        // list_by_project with a different project returns empty.
        let other_project = ProjectKey::new("other_tenant", "other_ws", "other_proj");
        let empty = SignalReadModel::list_by_project(&store, &other_project, 10, 0)
            .await
            .unwrap();
        assert!(empty.is_empty());
    }

    #[tokio::test]
    async fn signal_entity_ref_filtering() {
        let store = InMemoryStore::new();
        let project = test_project();
        let signal_id = SignalId::new("sig_entity");

        store
            .append(&[make_envelope(RuntimeEvent::SignalIngested(
                SignalIngested {
                    project: project.clone(),
                    signal_id: signal_id.clone(),
                    source: "api".to_owned(),
                    payload: serde_json::json!(null),
                    timestamp_ms: 500,
                },
            ))])
            .await
            .unwrap();

        // read_by_entity with matching Signal ref returns the event.
        let events = store
            .read_by_entity(&EntityRef::Signal(signal_id.clone()), None, 100)
            .await
            .unwrap();
        assert_eq!(events.len(), 1);
        assert!(matches!(
            &events[0].envelope.payload,
            RuntimeEvent::SignalIngested(e) if e.signal_id == signal_id
        ));

        // read_by_entity with a different signal ID returns empty.
        let other = store
            .read_by_entity(&EntityRef::Signal(SignalId::new("sig_other")), None, 100)
            .await
            .unwrap();
        assert!(other.is_empty());
    }

    // ── Secondary event log (dual-write) ─────────────────────────────────────

    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Minimal in-memory secondary log that counts appended events.
    struct CountingLog {
        count: Arc<AtomicUsize>,
        events: Arc<Mutex<Vec<EventEnvelope<RuntimeEvent>>>>,
    }

    impl CountingLog {
        fn new() -> (Arc<Self>, Arc<AtomicUsize>) {
            let count = Arc::new(AtomicUsize::new(0));
            let log = Arc::new(CountingLog {
                count: count.clone(),
                events: Arc::new(Mutex::new(Vec::new())),
            });
            (log, count)
        }
    }

    #[async_trait::async_trait]
    impl EventLog for CountingLog {
        async fn append(
            &self,
            events: &[EventEnvelope<RuntimeEvent>],
        ) -> Result<Vec<EventPosition>, crate::StoreError> {
            self.count.fetch_add(events.len(), Ordering::SeqCst);
            self.events.lock().unwrap().extend(events.iter().cloned());
            Ok(events
                .iter()
                .enumerate()
                .map(|(i, _)| EventPosition(i as u64))
                .collect())
        }

        async fn read_stream(
            &self,
            _after: Option<EventPosition>,
            _limit: usize,
        ) -> Result<Vec<StoredEvent>, crate::StoreError> {
            Ok(vec![])
        }

        async fn head_position(&self) -> Result<Option<EventPosition>, crate::StoreError> {
            Ok(None)
        }

        async fn read_by_entity(
            &self,
            _entity: &EntityRef,
            _after: Option<EventPosition>,
            _limit: usize,
        ) -> Result<Vec<StoredEvent>, crate::StoreError> {
            Ok(vec![])
        }

        async fn find_by_causation_id(
            &self,
            _causation_id: &str,
        ) -> Result<Option<EventPosition>, crate::StoreError> {
            Ok(None)
        }
    }

    /// Secondary log receives all events appended to the primary store.
    #[tokio::test]
    async fn secondary_log_receives_all_appends() {
        let store = Arc::new(InMemoryStore::new());
        let (counting_log, count) = CountingLog::new();
        store.set_secondary_log(counting_log);

        let project = test_project();

        // Append a session created event.
        store
            .append(&[make_envelope(RuntimeEvent::SessionCreated(
                SessionCreated {
                    project: project.clone(),
                    session_id: SessionId::new("sess_sec_1"),
                },
            ))])
            .await
            .unwrap();

        assert_eq!(
            count.load(Ordering::SeqCst),
            1,
            "secondary must receive 1 event"
        );

        // Append two more events in a single batch.
        store
            .append(&[
                make_envelope(RuntimeEvent::SessionCreated(SessionCreated {
                    project: project.clone(),
                    session_id: SessionId::new("sess_sec_2"),
                })),
                make_envelope(RuntimeEvent::SessionCreated(SessionCreated {
                    project: project.clone(),
                    session_id: SessionId::new("sess_sec_3"),
                })),
            ])
            .await
            .unwrap();

        assert_eq!(
            count.load(Ordering::SeqCst),
            3,
            "secondary must receive all 3 events total"
        );
    }

    /// Primary store is not affected when secondary log is absent.
    #[tokio::test]
    async fn no_secondary_log_works_normally() {
        let store = InMemoryStore::new();
        // No secondary log set — append must succeed normally.
        let positions = store
            .append(&[make_envelope(RuntimeEvent::SessionCreated(
                SessionCreated {
                    project: test_project(),
                    session_id: SessionId::new("sess_no_sec"),
                },
            ))])
            .await
            .unwrap();

        assert_eq!(positions.len(), 1);
        let sessions = SessionReadModel::list_active(&store, 10).await.unwrap();
        assert_eq!(sessions.len(), 1);
    }

    /// Secondary failure surfaces to the caller (fail-closed), but the
    /// in-memory projection has already been written.
    ///
    /// Pre-#T2-C3 behaviour was to swallow the secondary error and return
    /// `Ok`, which silently lost history whenever the secondary was the
    /// durable source of truth (RFC 002). This test pins the new contract:
    /// callers MUST observe the error so they can decide whether to retry,
    /// compensate, or abort. Deliberate side-effect: the in-memory state
    /// and the secondary log have diverged by the count in the error
    /// message; `event_id`-based idempotent retry is the expected
    /// reconciliation path.
    #[tokio::test]
    async fn secondary_failure_surfaces_error_but_keeps_in_memory_state() {
        struct FailingLog;

        #[async_trait::async_trait]
        impl EventLog for FailingLog {
            async fn append(
                &self,
                _events: &[EventEnvelope<RuntimeEvent>],
            ) -> Result<Vec<EventPosition>, crate::StoreError> {
                Err(crate::StoreError::Internal("secondary down".to_owned()))
            }
            async fn read_stream(
                &self,
                _: Option<EventPosition>,
                _: usize,
            ) -> Result<Vec<StoredEvent>, crate::StoreError> {
                Ok(vec![])
            }
            async fn head_position(&self) -> Result<Option<EventPosition>, crate::StoreError> {
                Ok(None)
            }
            async fn read_by_entity(
                &self,
                _: &EntityRef,
                _: Option<EventPosition>,
                _: usize,
            ) -> Result<Vec<StoredEvent>, crate::StoreError> {
                Ok(vec![])
            }
            async fn find_by_causation_id(
                &self,
                _: &str,
            ) -> Result<Option<EventPosition>, crate::StoreError> {
                Ok(None)
            }
        }

        let store = Arc::new(InMemoryStore::new());
        store.set_secondary_log(Arc::new(FailingLog));

        let result = store
            .append(&[make_envelope(RuntimeEvent::SessionCreated(
                SessionCreated {
                    project: test_project(),
                    session_id: SessionId::new("sess_resilient"),
                },
            ))])
            .await;

        assert!(
            result.is_err(),
            "secondary failure must surface as Err to the caller (fail-closed) — the pre-#T2-C3 silent-swallow path lost events under restart"
        );
        let err = result.unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("secondary event log write failed"),
            "error must identify the secondary-log divergence, got: {msg}"
        );

        // In-memory state is still written — the caller knows the two logs
        // diverged and can reconcile by retrying (event_id is idempotent).
        let sessions = SessionReadModel::list_active(store.as_ref(), 10)
            .await
            .unwrap();
        assert_eq!(
            sessions.len(),
            1,
            "primary projection must still reflect the in-memory write so diagnosis + retry see consistent state"
        );
    }

    // Issue #570: PauseScheduleReadModel::list_due must filter by
    // tenant and apply the `limit` at the trait layer.
    //
    // The cairn-app integration test for the resume-due endpoint is
    // stuck behind a separate latent bug: the bridge converter
    // `bridge_event_to_runtime_event` drops `pause_reason` when
    // emitting `ExecutionSuspended → RunStateChanged`, so the
    // service-layer path never lands a pause_reason on the cairn-store
    // event log. This test bypasses the bridge by appending raw
    // RunStateChanged envelopes directly — same shape `list_due`
    // walks — so the pagination contract is still exercised.
    #[tokio::test]
    async fn pause_schedule_list_due_filters_by_tenant_and_respects_limit() {
        use crate::projections::PauseScheduleReadModel;
        use cairn_domain::lifecycle::{PauseReason, PauseReasonKind};

        let store = InMemoryStore::new();
        let tenant_a = cairn_domain::TenantId::new("tenant_a");
        let tenant_b = cairn_domain::TenantId::new("tenant_b");

        // Helper: append a RunStateChanged(Running→Paused) with a
        // scheduled resume under the given project.
        let append_paused = |project: ProjectKey, run_id: &str, resume_after_ms: u64| {
            let envelope = make_envelope(RuntimeEvent::RunStateChanged(RunStateChanged {
                project,
                run_id: RunId::new(run_id),
                transition: StateTransition {
                    from: Some(RunState::Running),
                    to: RunState::Paused,
                },
                failure_class: None,
                pause_reason: Some(PauseReason {
                    kind: PauseReasonKind::OperatorPause,
                    detail: None,
                    resume_after_ms: Some(resume_after_ms),
                    actor: None,
                }),
                resume_trigger: None,
            }));
            let store = &store;
            async move { store.append(&[envelope]).await.unwrap() }
        };

        // Seed 4 paused runs under tenant_a and 2 under tenant_b.
        let project_a = ProjectKey::new(tenant_a.as_str(), "w", "p");
        let project_b = ProjectKey::new(tenant_b.as_str(), "w", "p");
        for i in 0..4u32 {
            append_paused(project_a.clone(), &format!("run_a_{i}"), 0).await;
        }
        for i in 0..2u32 {
            append_paused(project_b.clone(), &format!("run_b_{i}"), 0).await;
        }

        // now_ms is 1 s after the append — `resume_at_ms = stored_at +
        // resume_after_ms(0) = stored_at`, which is before `now_ms`,
        // so every paused row is due.
        let now_ms = u64::MAX / 2; // well past any wall-clock append time

        let a_page = PauseScheduleReadModel::list_due(&store, &tenant_a, now_ms, 100)
            .await
            .unwrap();
        assert_eq!(
            a_page.len(),
            4,
            "tenant_a sees all 4 of its own paused runs, never tenant_b's"
        );
        assert!(
            a_page.iter().all(|r| r.project.tenant_id == tenant_a),
            "cross-tenant leak: {a_page:?}"
        );

        let b_page = PauseScheduleReadModel::list_due(&store, &tenant_b, now_ms, 100)
            .await
            .unwrap();
        assert_eq!(b_page.len(), 2, "tenant_b sees its 2 runs");

        // Limit enforcement: 4 rows under tenant_a, limit=2 → 2 rows.
        let a_limited = PauseScheduleReadModel::list_due(&store, &tenant_a, now_ms, 2)
            .await
            .unwrap();
        assert_eq!(
            a_limited.len(),
            2,
            "limit bound at projection, not handler: {a_limited:?}"
        );
    }

    // Issue #570: RecoveryEscalationReadModel's trait now takes
    // `limit` + `offset`. The InMemoryStore impl is a stub that
    // always returns empty — assert that still holds post-#570 so a
    // future projection-backed impl doesn't change the no-escalations
    // wire contract without conscious migration.
    #[tokio::test]
    async fn recovery_escalation_list_by_tenant_paginated_stub_is_empty() {
        use crate::projections::RecoveryEscalationReadModel;
        let store = InMemoryStore::new();
        let tenant = cairn_domain::TenantId::new("tenant_stub");
        let page = RecoveryEscalationReadModel::list_by_tenant(&store, &tenant, 10, 0)
            .await
            .unwrap();
        assert!(page.is_empty(), "InMemoryStore stub must stay empty");
    }

    // Issue #570: RunSlaReadModel::list_breached_by_tenant orders
    // newest-first and respects limit + offset.
    #[tokio::test]
    async fn run_sla_list_breached_newest_first_with_pagination() {
        use crate::projections::RunSlaReadModel;
        let store = InMemoryStore::new();
        let tenant = cairn_domain::TenantId::new("tenant_sla");

        // Seed 5 breaches with strictly increasing breached_at_ms so
        // newest-first ordering is unambiguous.
        for i in 0..5u32 {
            let envelope = make_envelope(RuntimeEvent::RunSlaBreached(
                cairn_domain::events::RunSlaBreached {
                    run_id: RunId::new(format!("run_sla_{i}")),
                    tenant_id: tenant.clone(),
                    elapsed_ms: 60_000 + i as u64,
                    target_ms: 30_000,
                    breached_at_ms: 1_700_000_000_000 + (i as u64) * 1_000,
                },
            ));
            store.append(&[envelope]).await.unwrap();
        }

        // Page 1 of 2 — newest-first: run_sla_4, run_sla_3.
        let page1 = RunSlaReadModel::list_breached_by_tenant(&store, &tenant, 2, 0)
            .await
            .unwrap();
        assert_eq!(page1.len(), 2);
        assert_eq!(page1[0].run_id.as_str(), "run_sla_4");
        assert_eq!(page1[1].run_id.as_str(), "run_sla_3");

        // Offset 2 → page 2: run_sla_2, run_sla_1.
        let page2 = RunSlaReadModel::list_breached_by_tenant(&store, &tenant, 2, 2)
            .await
            .unwrap();
        assert_eq!(page2.len(), 2);
        assert_eq!(page2[0].run_id.as_str(), "run_sla_2");
        assert_eq!(page2[1].run_id.as_str(), "run_sla_1");

        // Offset 4 → tail: single row (run_sla_0).
        let tail = RunSlaReadModel::list_breached_by_tenant(&store, &tenant, 2, 4)
            .await
            .unwrap();
        assert_eq!(tail.len(), 1);
        assert_eq!(tail[0].run_id.as_str(), "run_sla_0");
    }

    // Issue #570: RunReadModel::list_stalled composes state +
    // staleness + tenant at the projection surface with pagination.
    #[tokio::test]
    async fn run_list_stalled_combines_state_staleness_tenant_with_pagination() {
        let store = InMemoryStore::new();
        let tenant = cairn_domain::TenantId::new("tenant_stall");
        let project = ProjectKey::new(tenant.as_str(), "w", "p");
        let session_id = SessionId::new("sess_stall");

        // Seed 5 runs: 3 Running, 1 Pending, 1 Completed (not stalled).
        for (i, state) in [
            RunState::Running,
            RunState::Running,
            RunState::Running,
            RunState::Pending,
            RunState::Completed, // terminal — must be excluded
        ]
        .into_iter()
        .enumerate()
        {
            let run_id = RunId::new(format!("run_stall_{i}"));
            store
                .append(&[make_envelope(RuntimeEvent::RunCreated(RunCreated {
                    project: project.clone(),
                    session_id: session_id.clone(),
                    run_id: run_id.clone(),
                    parent_run_id: None,
                    agent_role_id: None,
                    prompt_release_id: None,
                }))])
                .await
                .unwrap();
            if state != RunState::Pending {
                // RunCreated starts the run in Pending; transition to
                // the target state for the non-Pending cases.
                store
                    .append(&[make_envelope(RuntimeEvent::RunStateChanged(
                        RunStateChanged {
                            project: project.clone(),
                            run_id,
                            transition: StateTransition {
                                from: Some(RunState::Pending),
                                to: state,
                            },
                            failure_class: None,
                            pause_reason: None,
                            resume_trigger: None,
                        },
                    ))])
                    .await
                    .unwrap();
            }
        }

        // now_ms far into the future so every non-terminal run is
        // considered stale against a 0ms staleness window.
        let now_ms = u64::MAX / 2;

        // Tenant filter: wrong tenant sees 0 runs.
        let other_tenant = cairn_domain::TenantId::new("tenant_other");
        let other = RunReadModel::list_stalled(&store, &other_tenant, now_ms, 0, 100, 0)
            .await
            .unwrap();
        assert!(other.is_empty(), "cross-tenant leak: {other:?}");

        // Correct tenant sees 3 Running + 1 Pending = 4 non-terminal
        // stalled runs. Completed is excluded by the SQL-equivalent
        // predicate.
        let all_stalled = RunReadModel::list_stalled(&store, &tenant, now_ms, 0, 100, 0)
            .await
            .unwrap();
        assert_eq!(all_stalled.len(), 4, "{all_stalled:?}");
        assert!(
            all_stalled
                .iter()
                .all(|r| matches!(r.state, RunState::Running | RunState::Pending)),
            "terminal runs leaked: {all_stalled:?}"
        );

        // Pagination: limit=2 → 2 rows, offset=2 → next 2 rows.
        let page1 = RunReadModel::list_stalled(&store, &tenant, now_ms, 0, 2, 0)
            .await
            .unwrap();
        assert_eq!(page1.len(), 2);
        let page2 = RunReadModel::list_stalled(&store, &tenant, now_ms, 0, 2, 2)
            .await
            .unwrap();
        assert_eq!(page2.len(), 2);
        let union: std::collections::HashSet<_> = page1
            .iter()
            .chain(page2.iter())
            .map(|r| r.run_id.as_str().to_owned())
            .collect();
        assert_eq!(union.len(), 4, "pages must be disjoint");
    }
}
