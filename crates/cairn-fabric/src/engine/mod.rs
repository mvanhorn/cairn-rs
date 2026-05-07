//! Cairn-side abstraction over FlowFabric's read-side state.
//!
//! # Why this exists
//!
//! Cairn services used to read FF's Valkey state directly via
//! `.client.hgetall(&ctx.core())` — pinning cairn to FF's storage
//! engine (Valkey), key layout (`ExecKeyContext::core()`), and hash
//! field names (`public_state`, `dependency_kind`, etc.). A field
//! rename or storage swap in FF would silently break cairn.
//!
//! The [`Engine`] trait confines every cairn-side read of FF state
//! to one trait boundary. Services call `engine.describe_execution(&eid)`
//! and get a typed [`ExecutionSnapshot`] back; they never see Valkey
//! keys or hash fields.
//!
//! The single implementation, [`valkey_impl::ValkeyEngine`], holds
//! the `ferriskey::Client` handle and performs the direct HGETALL /
//! SMEMBERS reads. When FF 0.3 ships the upstream `describe_*`
//! primitives ([FlowFabric#58](https://github.com/avifenesh/FlowFabric/issues/58)),
//! `ValkeyEngine` becomes a thin passthrough and the typed snapshot
//! structs will be replaced by re-exports from the `ff` umbrella
//! crate.
//!
//! # Scope (what this trait does and doesn't cover)
//!
//! **In scope**: reads of FF-owned state (executions, flows,
//! dependency edges). Reads that fed `TaskRecord` / `RunRecord` /
//! `SessionRecord` construction.
//!
//! **Not in scope (yet)**:
//! - FCALL ARGV pre-reads (Phase D). The ~12 `hget ctx.core(),
//!   "current_attempt_id"` sites before FCALLs stay for now.
//! - Typed error model (Phase E). FCALL errors continue arriving as
//!   `ferriskey::Value` envelopes parsed by `helpers::*`; typed
//!   [`EngineError`](crate::error::FabricError) absorbs them later.
//! - Cairn-owned state (worker/quota/budget keyspaces). Those
//!   `HSET`s are cairn's own data, not a layering violation.
//! - `instance_tag_backfill` one-shot scanner. It operates on raw
//!   `ff:exec:*:tags` scan keys, not typed [`ExecutionId`]s;
//!   routing it through the trait would require a raw-key
//!   escape-hatch that re-exposes the Valkey layout. The backfill
//!   is a migration utility with a finite lifetime — it keeps its
//!   direct `HSET` until its sunset.
//!
//! **Phase C (shipped)**: Tag writes. Cairn services no longer call
//! `client.hset(&fctx.core(), "cairn.*", …)` directly —
//! [`Engine::set_flow_tag`] and [`Engine::set_flow_tags`] own the
//! flow-core namespace writes, and [`Engine::set_execution_tag`]
//! owns the execution-tags-hash writes. Enforced by the workspace
//! `clippy.toml` disallowed-methods lint on `ferriskey::Client::hset`
//! outside `engine/valkey_impl.rs`.

pub mod control_plane;
pub mod control_plane_types;
pub mod snapshots;
// Valkey-specific implementations of `Engine` + `ControlPlaneBackend`.
// Gated behind `fabric-valkey` so the traits remain compilable under
// `--no-default-features` without dragging in `ferriskey` or
// `ff_backend_valkey`. PR-C will add a sibling `postgres_impl`
// gated behind `fabric-postgres`.
#[cfg(feature = "fabric-valkey")]
pub mod valkey_control_plane_impl;
#[cfg(feature = "fabric-valkey")]
pub mod valkey_impl;

// PR-C3: compile-only PostgreSQL stub. Gated on `fabric-postgres` so the
// default Valkey build neither compiles nor links it. PR-C4 replaces
// every `unimplemented!("PR-C4: …")` body with real delegations / PG
// bodies and adds a live-Postgres integration test suite.
#[cfg(feature = "fabric-postgres")]
pub mod postgres_control_plane_impl;

use std::collections::{BTreeMap, BTreeSet};

use async_trait::async_trait;

use crate::error::FabricError;

pub use control_plane::ControlPlaneBackend;
pub use control_plane_types::{
    AddExecutionToFlowInput, ApplyDependencyToChildInput, BudgetSpendOutcome, BudgetStatusSnapshot,
    CancelFlowInput, CancelRunInput, ClaimGrantOutcome, CompleteRunInput, CreateFlowInput,
    CreateRunExecutionInput, DeliverApprovalSignalInput, EligibilityResult, ExecutionCreated,
    ExecutionLeaseContext, ExpiredLease, FailExecutionOutcome, FailRunInput, FlowCancelOutcome,
    IssueGrantAndClaimInput, IssueReclaimGrantInput, IssueReclaimGrantOutcome, QuotaAdmission,
    ReclaimExecutionInput, ReclaimExecutionOutcome, ReclaimGrantHandle, RenewLeaseInput,
    ResumeRunInput, RotationFailure, RotationOutcome, StageDependencyEdgeInput,
    StageDependencyOutcome, SubmitTaskInput, WorkerRegistration, WorkerSummary,
};
// Re-export the FF contracts + state-vector types the `Engine` trait's
// `read_execution_info` surface depends on, plus the typed id surface
// the trait methods take by reference. Keeps downstream callers (e.g.
// `cairn_app::lease_keeper`) off a direct `flowfabric` dep while still
// letting them pattern-match the full state vector (#666) and
// instantiate `Engine` mocks in tests.
pub use flowfabric::core::contracts::ExecutionInfo;
pub use flowfabric::core::state::{
    AttemptState, BlockingReason, EligibilityState, LifecyclePhase, OwnershipState, PublicState,
    StateVector, TerminalOutcome,
};
pub use flowfabric::core::types::{
    EdgeId, ExecutionId, FlowId, LaneId, Namespace, WorkerId, WorkerInstanceId,
};
#[cfg(feature = "fabric-postgres")]
pub use postgres_control_plane_impl::PostgresControlPlane;
pub use snapshots::{
    AttemptSummary, EdgeSnapshot, EdgeState, ExecutionSnapshot, FlowSnapshot, LeaseSummary,
};
#[cfg(feature = "fabric-valkey")]
pub use valkey_impl::ValkeyEngine;

/// Cairn-side read abstraction over FF state.
///
/// Every method that returns `Option<_>` uses `None` for "not present
/// in FF" and `Err` only for transport / serialisation / malformed
/// data. Callers that need a typed not-found error wrap the `None`
/// with their cairn-specific entity name (e.g.
/// `.ok_or(FabricError::NotFound { entity: "task", id })`).
///
/// ## Why `describe_edge` takes a `flow_id`
///
/// FF's edge hash key is `ff:flow:{fp:N}:<flow_id>:edge:<edge_id>` —
/// the flow id is part of the key. Cairn cannot locate an edge from
/// `edge_id` alone without an FF-side edge→flow index, which doesn't
/// exist today (see FlowFabric#58). Callers that know the flow
/// (typically because they just issued a `stage_dependency_edge`
/// FCALL on it) pass the flow_id explicitly. When FF 0.3 ships an
/// `edge_id`-only lookup this parameter becomes optional.
#[async_trait]
pub trait Engine: Send + Sync {
    /// Read a single execution's snapshot. Returns `Ok(None)` when
    /// the execution is not in FF (typically because cairn minted an
    /// id for an entity that was never submitted, or because the
    /// entity was purged).
    async fn describe_execution(
        &self,
        id: &ExecutionId,
    ) -> Result<Option<ExecutionSnapshot>, FabricError>;

    /// Read a flow's snapshot. Returns `Ok(None)` when the flow does
    /// not exist in FF.
    async fn describe_flow(&self, id: &FlowId) -> Result<Option<FlowSnapshot>, FabricError>;

    /// Read a dependency edge's snapshot. The caller must supply
    /// `flow_id` because FF's edge storage is flow-scoped — see
    /// the type-level docs above. Returns `Ok(None)` when the edge
    /// does not exist on the given flow.
    async fn describe_edge(
        &self,
        flow_id: &FlowId,
        edge_id: &EdgeId,
    ) -> Result<Option<EdgeSnapshot>, FabricError>;

    /// Enumerate dependency edges where `execution_id` is the
    /// downstream endpoint. Empty vec means the execution has no
    /// incoming edges (either never had dependencies declared, or
    /// all are resolved).
    async fn list_incoming_edges(
        &self,
        execution_id: &ExecutionId,
    ) -> Result<Vec<EdgeSnapshot>, FabricError>;

    /// Fetch a single tag value from an execution's tag hash.
    ///
    /// Targeted read — cheaper than
    /// [`Self::describe_execution`](Engine::describe_execution) when
    /// the caller only needs one field (e.g. the `cairn.task_id`
    /// back-reference stamped on an upstream execution). Avoids the
    /// N+1 amplification that full snapshot reads would cause in
    /// loops like `check_dependencies`'s per-blocker resolve.
    ///
    /// Returns `Ok(None)` if the execution's tag hash doesn't exist
    /// or the tag is absent. Empty-string values are normalised to
    /// `None`.
    async fn get_execution_tag(
        &self,
        id: &ExecutionId,
        key: &str,
    ) -> Result<Option<String>, FabricError>;

    /// Fetch the `lane_id` stamped on an execution's core hash.
    ///
    /// Targeted read — cheaper than
    /// [`Self::describe_execution`](Engine::describe_execution) when
    /// the caller only needs the lane (e.g. `SignalBridge` assembling
    /// an FCALL that routes through a lane-scoped index). Avoids the
    /// full `HGETALL exec_core` + `HGETALL exec_tags` amplification
    /// paid on every signal delivery on the hot path.
    ///
    /// FF stamps `lane_id` on the core hash at
    /// `ff_create_flow` / `ff_create_execution` time and never
    /// rewrites it — callers can cache the result per-execution for
    /// the lifetime of the process without worrying about staleness.
    ///
    /// Returns `Ok(None)` if the execution's core hash doesn't exist
    /// or the field is absent. Empty-string values are normalised to
    /// `None` so callers can fall back to a default lane (cairn uses
    /// `"cairn"`) via `.unwrap_or_else(|| LaneId::new("cairn"))`.
    async fn get_execution_lane_id(&self, id: &ExecutionId) -> Result<Option<LaneId>, FabricError>;

    /// Set a single tag on an execution's tag hash.
    ///
    /// Namespace-guarded: `key` must match `^[a-z][a-z0-9_]*\.` —
    /// one lowercase alpha-underscore prefix, then a `.`, then
    /// anything. Cairn owns the `cairn.*` namespace; FF's own hash
    /// fields have no `.`, so the rule is a mechanical guard
    /// against accidental collision with FF-managed fields. Keys
    /// that fail the rule return [`FabricError::Validation`].
    ///
    /// Callers never see the Valkey hash layout — the impl
    /// constructs the execution's tag key internally from the
    /// `ExecutionId` + partition config.
    async fn set_execution_tag(
        &self,
        id: &ExecutionId,
        key: &str,
        value: &str,
    ) -> Result<(), FabricError>;

    /// Set a single tag on a flow's core hash.
    ///
    /// Namespace-guarded: see [`Self::set_execution_tag`] for the
    /// rule. Callers never see the Valkey hash layout — the impl
    /// constructs the flow's core key internally.
    async fn set_flow_tag(&self, id: &FlowId, key: &str, value: &str) -> Result<(), FabricError>;

    /// Bulk-set flow tags in a single round-trip.
    ///
    /// Validation is **all-or-nothing**: if any key in `tags`
    /// fails the namespace rule ([`Self::set_execution_tag`]) the
    /// entire batch is rejected with [`FabricError::Validation`]
    /// and no write is issued. This preserves the "no partial
    /// writes" guarantee cairn's session creation path relies on
    /// (both `cairn.project` and `cairn.session_id` must be
    /// present before the bridge event fires).
    ///
    /// An empty map is a no-op that returns `Ok(())`.
    async fn set_flow_tags(
        &self,
        id: &FlowId,
        tags: &BTreeMap<String, String>,
    ) -> Result<(), FabricError>;

    // ── Worker registry (RFC-025 — FF 0.14 trait-routed) ────────────────
    //
    // FF 0.14 shipped 5 new `EngineBackend` trait methods covering the
    // worker-pool lifecycle + live-worker readback + expired-lease
    // enumeration (RFC-025 Phase 1-6). Cairn's cairn-side `Engine`
    // trait mirrors them so services stay backend-agnostic. Both
    // in-tree impls — [`valkey_impl::ValkeyEngine`] and
    // [`postgres_control_plane_impl::PostgresControlPlane`] — route
    // directly to the FF trait method; no bespoke per-backend commands.

    /// Register (or idempotently refresh) a worker instance.
    ///
    /// Re-registering the same `instance_id` overwrites caps + lanes +
    /// TTL (RFC-025 §9.3). FF 0.14 rejects re-registering with a
    /// different `worker_id` under the same `instance_id` with
    /// `Validation(InvalidInput, "instance_id reassigned")`.
    ///
    /// `liveness_ttl_ms` is stored alongside the registration so
    /// `heartbeat_worker` refreshes to the same value without the
    /// caller re-supplying it.
    async fn register_worker(
        &self,
        worker_id: &WorkerId,
        instance_id: &WorkerInstanceId,
        namespace: &Namespace,
        lanes: &BTreeSet<LaneId>,
        capabilities: &BTreeSet<String>,
        liveness_ttl_ms: u64,
    ) -> Result<WorkerRegistration, FabricError>;

    /// Refresh the worker-instance liveness TTL. Returns
    /// `HeartbeatWorkerOutcome::NotRegistered` transparently via
    /// [`FabricError`] on the TTL-expired-between-heartbeats race.
    async fn heartbeat_worker(
        &self,
        instance_id: &WorkerInstanceId,
        namespace: &Namespace,
    ) -> Result<(), FabricError>;

    /// Operator-driven worker death (distinct from passive TTL expiry).
    /// `reason` is capped at 256 bytes and must not contain control
    /// characters; oversize / invalid reject with
    /// `EngineError::Validation`. Idempotent: marking an already-absent
    /// instance is a no-op success.
    async fn mark_worker_dead(
        &self,
        instance_id: &WorkerInstanceId,
        namespace: &Namespace,
        reason: &str,
    ) -> Result<(), FabricError>;

    /// Enumerate live workers (RFC-025 Phase 6, §9.4) in the given
    /// namespace. Pass `namespace = None` for a cross-namespace sweep
    /// (auth enforced at the cairn-app admin route, NOT the trait
    /// boundary).
    async fn list_workers(
        &self,
        namespace: Option<&Namespace>,
    ) -> Result<Vec<control_plane_types::WorkerSummary>, FabricError>;

    // ── Task lifecycle reads ────────────────────────────────────────────

    /// Enumerate executions whose active lease has expired as of
    /// `now_ms`, capped at `limit`. FF 0.14 routes this through
    /// `EngineBackend::list_expired_leases`; every in-tree backend
    /// ships a body (Valkey, Postgres, SQLite).
    ///
    /// FF's server-side lease_expiry scanner handles reclaim — this
    /// primitive exists so cairn can surface a projection of
    /// timed-out tasks for operator dashboards without duplicating
    /// FF's scan logic.
    ///
    /// Empty `Vec` means no expired leases across the configured
    /// partition count.
    async fn list_expired_leases(
        &self,
        now_ms: u64,
        limit: usize,
    ) -> Result<Vec<control_plane_types::ExpiredLease>, FabricError>;

    /// Read FF's full execution state vector for `id`.
    ///
    /// `Ok(None)` ⇒ no such execution in FF (id minted but never
    /// submitted, or purged). `Ok(Some(_))` returns the 7-dimension
    /// [`StateVector`](flowfabric::core::state::StateVector) that
    /// drives FF's FCALL gating rules.
    ///
    /// The cairn-side lease keeper (issue #666) uses this probe to
    /// classify the execution's `lifecycle_phase` / `attempt_state` /
    /// `ownership_state` before issuing `ff_renew_lease`. FF rejects
    /// renews on any `lifecycle_phase != "active"` or
    /// `attempt_state == "attempt_interrupted"`; without this probe
    /// cairn had to infer the phase from its own projection, which
    /// lags FF on rapid suspend/resume cycles (dogfood R5, 2026-05-03).
    ///
    /// FF 0.15 ships this on `EngineBackend` — both `valkey_impl` and
    /// `postgres_control_plane_impl` forward directly to
    /// `EngineBackend::read_execution_info`.
    async fn read_execution_info(
        &self,
        id: &ExecutionId,
    ) -> Result<Option<ExecutionInfo>, FabricError>;
}
