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
pub struct PostgresFabricRuntime {
    /// Raw FF backend handle. `pub` so follow-up code (scanner
    /// supervisor wiring, lease-history subscriber, metrics reads)
    /// can reach it without breaking encapsulation.
    pub backend: Arc<dyn EngineBackend + Send + Sync>,
    /// Pre-constructed control-plane. Held so
    /// `FabricServices::start` can clone it into both trait-object
    /// slots without re-wrapping the backend.
    pub control_plane: Arc<PostgresControlPlane>,
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
        Ok(Self {
            backend,
            control_plane,
        })
    }
}
