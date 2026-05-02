//! PostgreSQL-backed runtime — sibling to [`crate::boot::FabricRuntime`].
//!
//! # Scope (PR-C4a)
//!
//! [`PostgresFabricRuntime::start`] constructs an `Arc<dyn EngineBackend>`
//! via [`ff_backend_postgres::PostgresBackend::connect`] and hands it
//! to [`crate::engine::PostgresControlPlane::new`]. The returned
//! runtime carries both the raw backend handle (exposed so callers
//! can read it in tests + in follow-up scanner wiring) and the
//! already-wrapped control-plane.
//!
//! Gated on `fabric-postgres` — default (Valkey-only) builds neither
//! compile nor link this module.
//!
//! # Scope bounds (what PR-C4a does NOT include)
//!
//! - **No migration trigger.** FF's migration set is applied out-of-
//!   band (matches the Valkey backend's "operator-provisioned FUNCTION
//!   LOAD" contract); the test harness applies `apply_migrations`
//!   manually. A boot-time check_schema_version call is a PR-C4b
//!   follow-up.
//! - **No scanner supervisor.** `PostgresBackend::with_scanners` is a
//!   follow-up; the control-plane reads + FCALL-style mutations work
//!   without the reconciler loop, but the `submitted → runnable` lane
//!   promotion + `lease_expiry` / `dependency_reconciler` stay
//!   pending.
//! - **No LeaseHistorySubscriber.** The Valkey `FabricServices` wires
//!   a `LeaseHistorySubscriber`; the PG equivalent
//!   (`subscribe_lease_history`) is stream-shaped and needs a
//!   separate wiring pass — deferred to PR-C4b.
//! - **No EventBridge handle.** `FabricServices::start_valkey`
//!   constructs `EventBridge::start(event_log)` + owns the join
//!   handle; the PG path currently constructs only the backend
//!   handle. PR-C4b lifts the event-bridge construction into a
//!   shared helper so both runtimes install it uniformly.
//!
//! # Why a sibling (vs extending `FabricRuntime`)
//!
//! `FabricRuntime` holds a `ferriskey::Client` handle and wires 8+
//! Valkey-specific subsystems (FCALL loader, version probe, instance-tag
//! backfill, lease-history subscriber). Forking the struct to hold
//! `Arc<dyn EngineBackend>` or a Valkey/Postgres enum would either leak
//! `ferriskey` types into the always-on interface or require shimming
//! every Postgres path with Valkey-specific methods. A sibling type is
//! cheaper and keeps each runtime's invariants local to its module.

use std::sync::Arc;

use ff_backend_postgres::PostgresBackend;
use flowfabric::core::engine_backend::EngineBackend;
use flowfabric::core::partition::PartitionConfig;
// `WorkerInstanceId` is only named inside the `FabricRuntimeHandle`
// impl below, which is itself gated on `fabric-valkey`. Guard the
// import at the same gate so a `fabric-postgres`-only build doesn't
// emit an unused-import warning.
#[cfg(feature = "fabric-valkey")]
use flowfabric::core::types::WorkerInstanceId;

use crate::config::FabricConfig;
use crate::engine::PostgresControlPlane;
use crate::error::FabricError;

/// PostgreSQL-backed Fabric runtime.
///
/// Holds the `Arc<dyn EngineBackend>` handle produced by
/// `PostgresBackend::connect` + the already-constructed
/// [`PostgresControlPlane`] that wraps it. Callers clone the
/// control-plane into both the `Arc<dyn Engine>` and
/// `Arc<dyn ControlPlaneBackend>` slots on `FabricServices`.
///
/// PR-C4c: grew `partition_config` and a shared `Arc<FabricConfig>`
/// so the runtime can impl
/// [`crate::runtime_handle::FabricRuntimeHandle`] symmetrically with
/// the Valkey [`crate::boot::FabricRuntime`]. Services (run / task /
/// session / quota / scheduler / signal_bridge) hold
/// `Arc<dyn FabricRuntimeHandle>` post-C4c and read every knob they
/// need through the trait — no service constructor takes a
/// concretely-typed runtime anymore.
pub struct PostgresFabricRuntime {
    /// Raw FF backend handle. `pub` so follow-up code (scanner
    /// supervisor wiring, lease-history subscriber, metrics reads)
    /// can reach it without breaking encapsulation.
    ///
    /// PR-C4c: typed `Arc<dyn EngineBackend>` (Send + Sync are
    /// implied by the trait's `Send + Sync` supertraits) to line
    /// up with [`crate::boot::FabricRuntime::backend`] and the
    /// shared [`crate::runtime_handle::FabricRuntimeHandle`] trait
    /// return type.
    pub backend: Arc<dyn EngineBackend>,
    /// Pre-constructed control-plane. Held so
    /// `FabricServices::start` can clone it into both trait-object
    /// slots without re-wrapping the backend.
    pub control_plane: Arc<PostgresControlPlane>,
    /// Partition layout the runtime is configured against. Carried
    /// so the runtime handle impl can expose it via
    /// [`crate::runtime_handle::FabricRuntimeHandle::partition_config`]
    /// — services mint ExecutionIds off this.
    pub partition_config: PartitionConfig,
    /// Shared `FabricConfig` handle. Carries
    /// `worker_instance_id`, `lease_ttl_ms`, `signal_dedup_ttl_ms`
    /// that the runtime handle impl reads from. Arc'd so the
    /// aggregate can clone cheaply.
    pub config: Arc<FabricConfig>,
}

impl PostgresFabricRuntime {
    /// Start the Postgres-backed runtime.
    ///
    /// Dials Postgres via `PostgresBackend::connect` using the
    /// `BackendConfig` carried on [`FabricConfig::backend`]. The call
    /// does NOT apply migrations — operators run those out-of-band.
    ///
    /// Returns an error if the `backend` arm is non-Postgres (the
    /// backend-kind cross-check in `FabricConfig::validate` catches
    /// this earlier; the check here is defense-in-depth).
    pub async fn start(config: FabricConfig) -> Result<Self, FabricError> {
        let backend = PostgresBackend::connect(config.backend.clone())
            .await
            .map_err(|e| FabricError::Engine(Box::new(e)))?;
        let control_plane = Arc::new(PostgresControlPlane::new(backend.clone()));
        // Mirror the Valkey runtime's partition-config handling
        // byte-for-byte: both runtimes install `PartitionConfig::
        // default()` (see `crate::boot::FabricRuntime::start` for
        // the Valkey symmetry). `PartitionConfig` is NOT carried
        // on `FabricConfig` today — it's an FF-owned knob that
        // operators tune via the FF partition count migration
        // path, not a cairn-fabric env var. If a future large-
        // scale deployment needs a non-default partition count
        // (e.g. raising `num_flow_partitions` past the RFC-011
        // 256 default), threading it through `FabricConfig` +
        // reading it here is the single integration point —
        // both runtimes would switch together so the
        // backend-agnostic contract holds.
        Ok(Self {
            backend,
            control_plane,
            partition_config: PartitionConfig::default(),
            config: Arc::new(config),
        })
    }
}

// PR-C4c: impl the backend-agnostic runtime handle trait so the PG
// runtime is drop-in compatible with every service constructor that
// used to take `Arc<FabricRuntime>`. Valkey-specific accessors
// (`valkey_client`, `fcall`) return `None` / `EngineError::Unavailable`
// — callers (scheduler + signal_bridge) are responsible for gating
// their code paths behind a Valkey-backend check. Cairn-app's worker
// loop is already gated at the app layer; full-app mode on PG does
// not spin a worker loop, so these unreachable-on-PG paths are
// covered by that contract.
//
// Gated on `fabric-valkey` because the trait + `ferriskey::{Client,
// Value}` types it mentions both live behind that feature.
// `fabric-postgres` + `fabric-valkey` is the only feature combination
// that links both this impl and its consumers; `fabric-postgres`
// alone (no Valkey) doesn't compile `services::*` / `aggregate::*`
// either, so nothing references this impl in that configuration.
#[cfg(feature = "fabric-valkey")]
#[async_trait::async_trait]
impl crate::runtime_handle::FabricRuntimeHandle for PostgresFabricRuntime {
    fn partition_config(&self) -> &PartitionConfig {
        &self.partition_config
    }

    fn worker_instance_id(&self) -> &WorkerInstanceId {
        &self.config.worker_instance_id
    }

    fn lease_ttl_ms(&self) -> u64 {
        self.config.lease_ttl_ms
    }

    fn signal_dedup_ttl_ms(&self) -> u64 {
        self.config.signal_dedup_ttl_ms
    }

    fn worker_capabilities(&self) -> &std::collections::BTreeSet<String> {
        &self.config.worker_capabilities
    }

    fn backend(&self) -> &Arc<dyn EngineBackend> {
        &self.backend
    }

    fn valkey_client(&self) -> Option<&ferriskey::Client> {
        // The PG runtime has no Valkey driver. Callers that need
        // one (FabricSchedulerService::new, SignalBridge's fcall
        // path) must gate on the `None` and surface the
        // unavailability to their own caller. The app-layer gate
        // that keeps worker code paths Valkey-only means this
        // `None` is never hit on the full-app-mode-on-PG path
        // that PR-C4c enables.
        None
    }

    async fn fcall(
        &self,
        _function: &str,
        _keys: &[String],
        _args: &[String],
    ) -> Result<ferriskey::Value, FabricError> {
        // PG has no Lua FCALL surface. Surface a typed
        // `EngineError::Unavailable` so callers can classify at
        // parity with every other PG-unavailable primitive (see
        // `PostgresControlPlane` bucket-C methods). Boxed because
        // `FabricError::Engine` wraps the error in a Box to keep
        // the enum small. Every parameter is `_`-prefixed because
        // the trait signature is satisfied purely by the error
        // return; no arg is inspected.
        Err(FabricError::Engine(Box::new(
            flowfabric::core::engine_error::EngineError::Unavailable {
                op: "fcall (Postgres backend has no Lua surface)",
            },
        )))
    }
}
