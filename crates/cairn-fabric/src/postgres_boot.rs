//! PostgreSQL-backed runtime stub — sibling to [`crate::boot::FabricRuntime`].
//!
//! # Scope (PR-C3)
//!
//! [`PostgresFabricRuntime::start`] is `unimplemented!("PR-C4")`. The
//! module exists so PR-C4's cutover is additive: it fills in the body,
//! wires `ff_backend_postgres::PostgresBackend::connect`, and routes
//! the resulting `Arc<dyn EngineBackend>` into
//! [`crate::engine::PostgresControlPlane::new`] without reshaping the
//! module tree.
//!
//! Gated on `fabric-postgres` — default (Valkey-only) builds neither
//! compile nor link this module.
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

use crate::config::FabricConfig;
use crate::error::FabricError;

/// PostgreSQL-backed Fabric runtime.
///
/// Today a unit struct because `PostgresFabricRuntime::start` panics
/// before constructing anything. PR-C4 grows this to carry the
/// `PostgresBackend` handle, the bridge, the background scanners, and
/// whichever lifecycle tasks FF 0.13's PG path requires.
#[derive(Debug)]
pub struct PostgresFabricRuntime;

impl PostgresFabricRuntime {
    /// Start the Postgres-backed runtime.
    ///
    /// **Stub body.** Panics with `unimplemented!("PR-C4: …")` so
    /// callers that reach this path under the stub get a loud,
    /// cross-referenced failure rather than a silent no-op.
    ///
    /// PR-C4 replaces the body with:
    /// 1. `ff_backend_postgres::PostgresBackend::connect(config.backend.clone()).await`
    /// 2. construct [`crate::engine::PostgresControlPlane::new(backend)`]
    /// 3. wire the bridge + background tasks equivalent to
    ///    [`crate::boot::FabricRuntime::start`].
    pub async fn start(_config: FabricConfig) -> Result<Self, FabricError> {
        unimplemented!(
            "PR-C4: PostgresFabricRuntime::start — stub only in PR-C3. \
             PR-C4 wires ff_backend_postgres::PostgresBackend::connect \
             and installs the bridge + background scanners."
        )
    }
}
