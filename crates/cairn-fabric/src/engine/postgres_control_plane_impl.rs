//! Compile-only PostgreSQL backend stub for cairn-fabric.
//!
//! # Scope (PR-C3)
//!
//! This is a **compile-only** stub: every method on both
//! [`Engine`] and [`ControlPlaneBackend`] returns
//! `unimplemented!("PR-C4: <name>")`. The purpose is to prove the
//! symbol shape under the `fabric-postgres` feature without landing
//! any real Postgres logic. PR-C4 replaces the bodies with real
//! delegations to FF 0.13's [`EngineBackend`] trait (for methods that
//! have an upstream equivalent) and with Postgres-native bodies for
//! the rest.
//!
//! The struct carries a `backend: Arc<dyn EngineBackend + Send + Sync>`
//! field so PR-C4 can wire delegations without changing the public
//! type. Today the field is `#[allow(dead_code)]` — no method reads it
//! yet — but it is `pub` to let a PR-C4 follow-up construct the stub
//! directly from a `PostgresBackend::connect` handle without reshuffling
//! the struct.
//!
//! # Why one struct, two impls
//!
//! Mirrors [`ValkeyEngine`](super::valkey_impl::ValkeyEngine): one
//! concrete type carries both the [`Engine`] (read + tag writes) and
//! [`ControlPlaneBackend`] (FCALL-shaped mutations) trait impls. Callers
//! hold a single `Arc<PostgresControlPlane>` and cast to either trait
//! object. If the struct ever grows unwieldy we'll split, but at stub
//! size the split adds noise without benefit.

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use flowfabric::core::engine_backend::EngineBackend;
use flowfabric::core::types::{
    BudgetId, EdgeId, ExecutionId, FlowId, LaneId, QuotaPolicyId, WorkerId, WorkerInstanceId,
};

use crate::error::FabricError;

use super::control_plane::ControlPlaneBackend;
use super::control_plane_types::{
    AddExecutionToFlowInput, ApplyDependencyToChildInput, BudgetSpendOutcome, BudgetStatusSnapshot,
    CancelFlowInput, CancelRunInput, ClaimGrantOutcome, CompleteRunInput, CreateFlowInput,
    CreateRunExecutionInput, DeliverApprovalSignalInput, EligibilityResult, ExecutionCreated,
    ExpiredLease, FailExecutionOutcome, FailRunInput, FlowCancelOutcome, IssueGrantAndClaimInput,
    QuotaAdmission, RenewLeaseInput, ResumeRunInput, RotationOutcome, StageDependencyEdgeInput,
    StageDependencyOutcome, SubmitTaskInput, WorkerRegistration,
};
use super::snapshots::{EdgeSnapshot, ExecutionSnapshot, FlowSnapshot};
use super::Engine;

/// PostgreSQL-backed implementation of [`Engine`] + [`ControlPlaneBackend`].
///
/// Holds an `Arc<dyn EngineBackend>` produced by
/// `ff_backend_postgres::PostgresBackend::connect` (wired in PR-C4's
/// [`crate::postgres_boot::PostgresFabricRuntime::start`]). Every
/// method body is `unimplemented!("PR-C4: <name>")` at PR-C3; PR-C4
/// replaces them with real delegations / PG-native logic.
pub struct PostgresControlPlane {
    /// FF 0.13 typed backend handle. Populated by PR-C4's
    /// `PostgresFabricRuntime::start` via
    /// `ff_backend_postgres::PostgresBackend::connect`. Unread at PR-C3
    /// — every trait method stubs — but kept `pub` so a PR-C4 follow-up
    /// can construct the stub directly from a pre-built
    /// `Arc<dyn EngineBackend>`.
    #[allow(dead_code)]
    pub backend: Arc<dyn EngineBackend + Send + Sync>,
}

impl PostgresControlPlane {
    /// Construct a new stub. Takes an already-built
    /// `Arc<dyn EngineBackend>` — PR-C4's
    /// [`crate::postgres_boot::PostgresFabricRuntime::start`] calls
    /// `ff_backend_postgres::PostgresBackend::connect` and passes the
    /// result here.
    pub fn new(backend: Arc<dyn EngineBackend + Send + Sync>) -> Self {
        Self { backend }
    }
}

// ── Engine trait impl (13 methods, all stub) ──────────────────────────

#[async_trait]
impl Engine for PostgresControlPlane {
    async fn describe_execution(
        &self,
        _id: &ExecutionId,
    ) -> Result<Option<ExecutionSnapshot>, FabricError> {
        unimplemented!("PR-C4: PostgresControlPlane::describe_execution")
    }

    async fn describe_flow(&self, _id: &FlowId) -> Result<Option<FlowSnapshot>, FabricError> {
        unimplemented!("PR-C4: PostgresControlPlane::describe_flow")
    }

    async fn describe_edge(
        &self,
        _flow_id: &FlowId,
        _edge_id: &EdgeId,
    ) -> Result<Option<EdgeSnapshot>, FabricError> {
        unimplemented!("PR-C4: PostgresControlPlane::describe_edge")
    }

    async fn list_incoming_edges(
        &self,
        _execution_id: &ExecutionId,
    ) -> Result<Vec<EdgeSnapshot>, FabricError> {
        unimplemented!("PR-C4: PostgresControlPlane::list_incoming_edges")
    }

    async fn get_execution_tag(
        &self,
        _id: &ExecutionId,
        _key: &str,
    ) -> Result<Option<String>, FabricError> {
        unimplemented!("PR-C4: PostgresControlPlane::get_execution_tag")
    }

    async fn get_execution_lane_id(
        &self,
        _id: &ExecutionId,
    ) -> Result<Option<LaneId>, FabricError> {
        unimplemented!("PR-C4: PostgresControlPlane::get_execution_lane_id")
    }

    async fn set_execution_tag(
        &self,
        _id: &ExecutionId,
        _key: &str,
        _value: &str,
    ) -> Result<(), FabricError> {
        unimplemented!("PR-C4: PostgresControlPlane::set_execution_tag")
    }

    async fn set_flow_tag(
        &self,
        _id: &FlowId,
        _key: &str,
        _value: &str,
    ) -> Result<(), FabricError> {
        unimplemented!("PR-C4: PostgresControlPlane::set_flow_tag")
    }

    async fn set_flow_tags(
        &self,
        _id: &FlowId,
        _tags: &BTreeMap<String, String>,
    ) -> Result<(), FabricError> {
        unimplemented!("PR-C4: PostgresControlPlane::set_flow_tags")
    }

    async fn register_worker(
        &self,
        _worker_id: &WorkerId,
        _instance_id: &WorkerInstanceId,
        _capabilities: &[String],
    ) -> Result<WorkerRegistration, FabricError> {
        unimplemented!("PR-C4: PostgresControlPlane::register_worker")
    }

    async fn heartbeat_worker(&self, _instance_id: &WorkerInstanceId) -> Result<(), FabricError> {
        unimplemented!("PR-C4: PostgresControlPlane::heartbeat_worker")
    }

    async fn mark_worker_dead(&self, _instance_id: &WorkerInstanceId) -> Result<(), FabricError> {
        unimplemented!("PR-C4: PostgresControlPlane::mark_worker_dead")
    }

    async fn list_expired_leases(
        &self,
        _now_ms: u64,
        _limit: usize,
    ) -> Result<Vec<ExpiredLease>, FabricError> {
        unimplemented!("PR-C4: PostgresControlPlane::list_expired_leases")
    }
}

// ── ControlPlaneBackend trait impl (22 methods, all stub) ─────────────

#[async_trait]
impl ControlPlaneBackend for PostgresControlPlane {
    // ── Budget ────────────────────────────────────────────────────────

    async fn create_budget(
        &self,
        _scope_type: &str,
        _scope_id: &str,
        _dimensions: &[&str],
        _hard_limits: &[u64],
        _soft_limits: &[u64],
        _reset_interval_ms: u64,
        _enforcement_mode: &str,
    ) -> Result<BudgetId, FabricError> {
        unimplemented!("PR-C4: PostgresControlPlane::create_budget")
    }

    async fn record_spend(
        &self,
        _budget_id: &BudgetId,
        _execution_id: &ExecutionId,
        _dimension_deltas: &[(&str, u64)],
        _idempotency_key: &str,
    ) -> Result<BudgetSpendOutcome, FabricError> {
        unimplemented!("PR-C4: PostgresControlPlane::record_spend")
    }

    async fn release_budget(&self, _budget_id: &BudgetId) -> Result<(), FabricError> {
        unimplemented!("PR-C4: PostgresControlPlane::release_budget")
    }

    async fn get_budget_status(
        &self,
        _budget_id: &BudgetId,
    ) -> Result<Option<BudgetStatusSnapshot>, FabricError> {
        unimplemented!("PR-C4: PostgresControlPlane::get_budget_status")
    }

    // ── Quota ─────────────────────────────────────────────────────────

    async fn create_quota_policy(
        &self,
        _scope_type: &str,
        _scope_id: &str,
        _window_seconds: u64,
        _max_requests_per_window: u64,
        _max_concurrent: u64,
    ) -> Result<QuotaPolicyId, FabricError> {
        unimplemented!("PR-C4: PostgresControlPlane::create_quota_policy")
    }

    async fn check_admission(
        &self,
        _quota_policy_id: &QuotaPolicyId,
        _execution_id: &ExecutionId,
        _window_seconds: u64,
        _rate_limit: u64,
        _concurrency_cap: u64,
    ) -> Result<QuotaAdmission, FabricError> {
        unimplemented!("PR-C4: PostgresControlPlane::check_admission")
    }

    // ── Rotation ──────────────────────────────────────────────────────

    async fn rotate_waitpoint_hmac(
        &self,
        _new_kid: &str,
        _new_secret_hex: &str,
        _grace_ms: u64,
    ) -> RotationOutcome {
        unimplemented!("PR-C4: PostgresControlPlane::rotate_waitpoint_hmac")
    }

    // ── Run lifecycle ─────────────────────────────────────────────────

    async fn create_run_execution(
        &self,
        _input: CreateRunExecutionInput,
    ) -> Result<ExecutionCreated, FabricError> {
        unimplemented!("PR-C4: PostgresControlPlane::create_run_execution")
    }

    async fn complete_run_execution(&self, _input: CompleteRunInput) -> Result<(), FabricError> {
        unimplemented!("PR-C4: PostgresControlPlane::complete_run_execution")
    }

    async fn fail_run_execution(
        &self,
        _input: FailRunInput,
    ) -> Result<FailExecutionOutcome, FabricError> {
        unimplemented!("PR-C4: PostgresControlPlane::fail_run_execution")
    }

    async fn cancel_run_execution(&self, _input: CancelRunInput) -> Result<(), FabricError> {
        unimplemented!("PR-C4: PostgresControlPlane::cancel_run_execution")
    }

    async fn resume_run_execution(&self, _input: ResumeRunInput) -> Result<(), FabricError> {
        unimplemented!("PR-C4: PostgresControlPlane::resume_run_execution")
    }

    async fn deliver_approval_signal(
        &self,
        _input: DeliverApprovalSignalInput,
    ) -> Result<(), FabricError> {
        unimplemented!("PR-C4: PostgresControlPlane::deliver_approval_signal")
    }

    // ── Session lifecycle ─────────────────────────────────────────────

    async fn create_flow(&self, _input: CreateFlowInput) -> Result<(), FabricError> {
        unimplemented!("PR-C4: PostgresControlPlane::create_flow")
    }

    async fn cancel_flow(&self, _input: CancelFlowInput) -> Result<FlowCancelOutcome, FabricError> {
        unimplemented!("PR-C4: PostgresControlPlane::cancel_flow")
    }

    // ── Claim ─────────────────────────────────────────────────────────

    async fn issue_grant_and_claim(
        &self,
        _input: IssueGrantAndClaimInput,
    ) -> Result<ClaimGrantOutcome, FabricError> {
        unimplemented!("PR-C4: PostgresControlPlane::issue_grant_and_claim")
    }

    // ── Task lifecycle ────────────────────────────────────────────────

    async fn submit_task_execution(
        &self,
        _input: SubmitTaskInput,
    ) -> Result<ExecutionCreated, FabricError> {
        unimplemented!("PR-C4: PostgresControlPlane::submit_task_execution")
    }

    async fn add_execution_to_flow(
        &self,
        _input: AddExecutionToFlowInput,
    ) -> Result<(), FabricError> {
        unimplemented!("PR-C4: PostgresControlPlane::add_execution_to_flow")
    }

    async fn stage_dependency_edge(
        &self,
        _input: StageDependencyEdgeInput,
    ) -> Result<StageDependencyOutcome, FabricError> {
        unimplemented!("PR-C4: PostgresControlPlane::stage_dependency_edge")
    }

    async fn apply_dependency_to_child(
        &self,
        _input: ApplyDependencyToChildInput,
    ) -> Result<(), FabricError> {
        unimplemented!("PR-C4: PostgresControlPlane::apply_dependency_to_child")
    }

    async fn evaluate_flow_eligibility(
        &self,
        _execution_id: &ExecutionId,
    ) -> Result<EligibilityResult, FabricError> {
        unimplemented!("PR-C4: PostgresControlPlane::evaluate_flow_eligibility")
    }

    async fn renew_task_lease(&self, _input: RenewLeaseInput) -> Result<(), FabricError> {
        unimplemented!("PR-C4: PostgresControlPlane::renew_task_lease")
    }
}
