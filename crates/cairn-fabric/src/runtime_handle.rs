//! Backend-agnostic view into the cairn-fabric runtime.
//!
//! # Why this exists
//!
//! Before PR-C4c, every cairn-fabric service constructor
//! (`FabricRunService::new`, `FabricTaskService::new`,
//! `FabricSessionService::new`, `FabricQuotaService::new`,
//! `FabricSchedulerService::new`, `SignalBridge::new`) took an
//! `Arc<crate::boot::FabricRuntime>` — a concretely-typed,
//! Valkey-flavoured handle. `FabricRuntime` carries a
//! `ferriskey::Client`, a `ff_observability::Metrics`, the seeded
//! HMAC state, and the rest of the Valkey-specific startup
//! artefacts. A sibling Postgres runtime ([`PostgresFabricRuntime`])
//! cannot satisfy that type signature, so
//! `FabricServices::start(..., BackendKind::Postgres)` returned
//! `FabricError::Config` at boot — even though the actual work
//! those services do (mint ExecutionIds, dispatch
//! `ControlPlaneBackend` FCALL-shape methods, read post-commit
//! snapshots) is backend-agnostic.
//!
//! `FabricRuntimeHandle` is the narrow trait every service now
//! holds instead. Its surface is the union of every runtime-level
//! knob services actually read off `FabricRuntime` today (audit in
//! cairn-rs #602 PR-C4c):
//!
//! - `partition_config` — drives `id_map` ExecutionId minting
//! - `worker_instance_id` — stamped on every execution's
//!   `cairn.instance_id` tag for cross-instance isolation
//! - `lease_ttl_ms` — lifecycle FCALL arg on `issue_grant_and_claim`
//! - `signal_dedup_ttl_ms` — signal delivery dedup window
//! - `worker_capabilities` — advertised to FF at scheduler claim
//!   time via `Scheduler::claim_for_worker`
//! - `backend` — `Arc<dyn EngineBackend>` used by
//!   `suspend_by_triple` and future trait-routed methods
//!
//! Plus two Valkey-specific escape hatches described below.
//!
//! Both `FabricRuntime` (Valkey) and [`PostgresFabricRuntime`]
//! impl this trait. The aggregate holds an
//! `Arc<dyn FabricRuntimeHandle>` and services never see the
//! concrete runtime again.
//!
//! # Valkey escape hatches
//!
//! Two services still need Valkey-specific primitives that FF has
//! not yet surfaced through `EngineBackend`: `SignalBridge` does
//! `ff_deliver_signal` via a raw `ferriskey::Client::fcall`, and
//! `FabricSchedulerService` wraps `flowfabric::scheduler::Scheduler`
//! which is `ferriskey::Client`-constructed. The trait exposes:
//!
//! - [`FabricRuntimeHandle::valkey_client`] — `Option<&ferriskey::Client>`
//!   for constructors that need one.
//! - [`FabricRuntimeHandle::fcall`] — Valkey FCALL on Valkey,
//!   `FabricError::Engine(EngineError::Unavailable)` on Postgres.
//!
//! On the Postgres runtime both return `None` / `Unavailable`.
//! Callers that need them must gate their code paths behind the
//! Valkey-backend check or tolerate the `Unavailable` error
//! (the scheduler + signal-delivery surfaces are covered by
//! cairn-rs' own "worker code paths are Valkey-gated" contract —
//! full-app mode on PG does not spin a worker loop).
//!
//! [`PostgresFabricRuntime`]: crate::postgres_boot::PostgresFabricRuntime

use std::collections::BTreeSet;
use std::sync::Arc;

use async_trait::async_trait;
use flowfabric::core::engine_backend::EngineBackend;
use flowfabric::core::partition::PartitionConfig;
use flowfabric::core::types::WorkerInstanceId;

use crate::error::FabricError;

/// Backend-agnostic accessor surface for the cairn-fabric runtime.
///
/// Services hold `Arc<dyn FabricRuntimeHandle>` instead of
/// `Arc<FabricRuntime>` so the same constructor chain works for
/// both the Valkey and Postgres runtimes.
#[async_trait]
pub trait FabricRuntimeHandle: Send + Sync {
    /// Partition layout the runtime is configured against. Services
    /// thread this into `id_map::*` helpers to mint deterministic
    /// ExecutionIds and to derive `execution_partition` for
    /// `ExecKeyContext`.
    fn partition_config(&self) -> &PartitionConfig;

    /// The operator-chosen instance identifier for this cairn-app
    /// process. Stamped on every execution's `cairn.instance_id`
    /// tag so cross-instance scanner filters (FF#122) keep two
    /// cairn-apps sharing a backend blind to each other's frames.
    fn worker_instance_id(&self) -> &WorkerInstanceId;

    /// Configured lease TTL (ms). Threaded into
    /// `issue_grant_and_claim` + `renew_task_lease` so the FCALL
    /// agrees with the runtime's steady-state scan cadence.
    fn lease_ttl_ms(&self) -> u64;

    /// Configured signal-dedup TTL (ms). Threaded into
    /// `deliver_signal` + `deliver_approval_signal` idempotency
    /// fences. Independent of `lease_ttl_ms` so long approval
    /// windows can outlive lease renewal cycles without dropping
    /// the dedup slot.
    fn signal_dedup_ttl_ms(&self) -> u64;

    /// Capabilities this worker advertises to FF at claim time.
    /// Consumed by `FabricSchedulerService::claim_for_worker` —
    /// FF derives a deterministic sorted CSV from the set and
    /// matches against each execution's `required_capabilities`.
    /// An empty set means "no capabilities advertised".
    fn worker_capabilities(&self) -> &BTreeSet<String>;

    /// The trait-object `EngineBackend` handle. Used by
    /// `suspension::suspend_by_triple` + any other code path that
    /// hands off to an FF-trait-routed primitive. Valkey returns
    /// an `Arc<ValkeyBackend>` coerced to the trait object;
    /// Postgres returns an `Arc<PostgresBackend>`. Both satisfy
    /// the same trait surface.
    fn backend(&self) -> &Arc<dyn EngineBackend>;

    /// Valkey-specific escape hatch for services that construct
    /// Valkey-native helpers (the ff-scheduler `Scheduler` and the
    /// Valkey `ff_deliver_signal` FCALL path). Returns `Some` on
    /// the Valkey runtime and `None` on the Postgres runtime. The
    /// `Option` is the contract — callers that need this handle
    /// must either short-circuit on `None` or surface the
    /// unavailability to their caller.
    fn valkey_client(&self) -> Option<&ferriskey::Client>;

    /// Dispatch an FCALL against FF's registered Lua library.
    ///
    /// On the Valkey runtime this is a direct delegation to
    /// `FabricRuntime::fcall` (debug-mode arg verification +
    /// `fcall_timeout_ms` timeout + typed
    /// `FabricError::Valkey(_)` on driver errors).
    ///
    /// On the Postgres runtime this returns
    /// `FabricError::Engine(EngineError::Unavailable { op: "fcall …" })`
    /// without attempting any I/O — the PG backend has no Lua
    /// surface and the caller (today exclusively
    /// `SignalBridge::deliver_signal`) must be gated behind a
    /// Valkey-backend check. Cairn-app's worker loop is already
    /// gated at the app layer, so this path is unreachable in the
    /// PG full-aggregate boot scenario that motivated the
    /// refactor.
    async fn fcall(
        &self,
        function: &str,
        keys: &[String],
        args: &[String],
    ) -> Result<ferriskey::Value, FabricError>;
}
