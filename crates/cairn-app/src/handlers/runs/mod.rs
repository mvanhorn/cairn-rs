//! Run HTTP handlers and request/response DTOs.
//!
//! This module is a thin re-export hub over domain-specific sub-modules.
//! The public-API surface (every symbol the crate's `handlers::runs::*`
//! glob used to expose) is preserved verbatim so `router.rs` and every
//! existing test compile without edits.
//!
//! Sub-modules:
//!
//! - [`lifecycle`] — list / get / create / cancel / claim / pause / resume /
//!   spawn-subagent / list-child / due-resumes / recover
//! - [`orchestrate`] — `POST /v1/runs/:id/orchestrate` + idempotency +
//!   breaker overrides (emitter + providers-exhausted card live in
//!   `orchestrate_emitter` and `orchestrate_exhaustion` respectively)
//! - [`plan`] — RFC 018 plan review (approve / reject / revise)
//! - [`intervene`] — operator force-complete / fail / restart / inject
//! - [`checkpoint`] — replay + replay-to-checkpoint + save-checkpoint alias
//! - [`cost`] — per-run cost + cost alerts + SLA + tenant-wide costs
//! - [`events`] — per-run durable event-log stream
//! - [`telemetry`] — stalled / escalated / diagnose / audit trail / run telemetry
//! - [`helpers`] — cross-cluster internals (stuck threshold, redactor,
//!   failure classifier, fail-flip finalizer)

mod checkpoint;
mod cost;
mod events;
mod helpers;
mod intervene;
mod lifecycle;
mod orchestrate;
mod orchestrate_emitter;
mod orchestrate_exhaustion;
mod plan;
mod telemetry;

// Re-export every pub(crate) symbol the pre-split module exposed so
// crate-level `use handlers::runs::*` keeps working without edits.
// `#[allow(unused_imports)]` on each `pub use` line because some items
// are only consumed via the glob-re-export in `lib.rs`.

#[allow(unused_imports)]
pub(crate) use checkpoint::{
    record_checkpoint_handler, replay_run_handler, replay_run_to_checkpoint_handler,
    ReplayToCheckpointQuery, RunReplayQuery,
};
#[allow(unused_imports)]
pub(crate) use cost::{
    get_run_cost_handler, get_run_sla_handler, list_run_cost_alerts_handler,
    list_sla_breached_handler, list_tenant_costs_handler, set_run_cost_alert_handler,
    set_run_sla_handler, RunCostAlertResponse, SetRunCostAlertRequest, SetRunSlaRequest,
};
#[allow(unused_imports)]
pub(crate) use events::{list_run_events_handler, EventSummary, EventsPage, EventsPageQuery};
#[allow(unused_imports)]
pub(crate) use helpers::{resolve_stuck_run_threshold_ms, STUCK_RUN_THRESHOLD_KEY};
#[allow(unused_imports)]
pub(crate) use intervene::{
    intervene_run_handler, list_run_interventions_handler, RunInterventionAction,
    RunInterventionRequest, RunInterventionResponse,
};
#[allow(unused_imports)]
pub(crate) use lifecycle::{
    cancel_orphan_run_handler, cancel_run_handler, claim_run_handler, create_run_handler,
    get_run_handler, list_child_runs_handler, list_due_run_resumes_handler, list_runs_handler,
    list_subagent_spawns_handler, pause_run_handler, process_scheduled_run_resumes_handler,
    recover_run_handler, resume_run_handler, spawn_subagent_run_handler, CreateRunRequest,
    PauseRunRequest, ResumeRunRequest, RunCompletion, RunDetailResponse, RunListQuery,
    ScheduledResumeProcessResponse, SpawnSubagentRunRequest, SpawnSubagentRunResponse,
};
// Utoipa's `#[utoipa::path(...)]` macro generates `__path_<fn>` helper
// structs consumed by `#[derive(OpenApi)]` at the router's scope via
// name-path resolution. The derive looks them up as siblings of the
// handler fn, so the re-export has to propagate them explicitly —
// `pub use ...::fn_name` alone does not carry the helper struct.
#[allow(unused_imports)]
pub(crate) use lifecycle::{__path_create_run_handler, __path_list_runs_handler};
#[allow(unused_imports)]
pub(crate) use orchestrate::{orchestrate_run_handler, BreakerOverrides, OrchestrateRequest};
#[allow(unused_imports)]
pub(crate) use plan::{
    approve_plan_handler, reject_plan_handler, revise_plan_handler, ApprovePlanRequest,
    RejectPlanRequest, RevisePlanRequest,
};
#[allow(unused_imports)]
pub(crate) use telemetry::{
    diagnose_run_handler, get_run_audit_trail_handler, get_run_telemetry_handler,
    list_escalated_runs_handler, list_stalled_runs_handler, AuditEntry, AuditTrail,
    StalledRunsQuery,
};
