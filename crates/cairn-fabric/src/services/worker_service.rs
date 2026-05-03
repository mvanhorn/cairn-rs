//! Worker-registry service — thin shim over [`Engine`].
//!
//! FF 0.14 closed FF#473: `register_worker`, `heartbeat_worker`,
//! `mark_worker_dead`, `list_workers`, `list_expired_leases` all flow
//! through the `EngineBackend` trait on every in-tree backend (Valkey,
//! Postgres, SQLite). Cairn's [`Engine`] trait mirrors the surface so
//! this service stays backend-agnostic; both `ValkeyEngine` and
//! `PostgresControlPlane` delegate verbatim to the trait.
//!
//! **Lean-bridge silence (intentional).** None of this service's
//! methods emit `BridgeEvent`s — worker lifecycle is FF-owned
//! operational state with no corresponding cairn-store projection.
//! See `docs/design/bridge-event-audit.md` §2.5.
use std::collections::BTreeSet;
use std::sync::Arc;

use flowfabric::core::types::{LaneId, Namespace, WorkerId, WorkerInstanceId};

use crate::engine::control_plane_types::{
    WorkerRegistration as EngineWorkerRegistration, WorkerSummary,
};
use crate::engine::Engine;
use crate::error::FabricError;

/// Historical service-level name — kept as a re-export so importers of
/// `crate::services::worker_service::WorkerRegistration` keep working.
pub type WorkerRegistration = EngineWorkerRegistration;

/// Cairn's shutdown-reason literal for [`FabricWorkerService::mark_worker_dead`].
/// Kept terse — FF caps `MarkWorkerDeadArgs::reason` at 256 bytes.
pub const DEFAULT_MARK_DEAD_REASON: &str = "graceful_shutdown";

pub struct FabricWorkerService {
    engine: Arc<dyn Engine>,
    /// Per-service default TTL, derived from `FabricConfig::lease_ttl_ms * 3`
    /// at boot (preserves pre-FF-0.14 behaviour — the Valkey impl used the
    /// same `lease_ttl_ms * 3` window for its PEXPIRE safety net).
    liveness_ttl_ms: u64,
}

impl FabricWorkerService {
    pub fn new(engine: Arc<dyn Engine>, liveness_ttl_ms: u64) -> Self {
        Self {
            engine,
            liveness_ttl_ms,
        }
    }

    /// Exposed so tests + consumers that want to query the TTL cairn
    /// will stamp on each registration can read it back.
    pub fn liveness_ttl_ms(&self) -> u64 {
        self.liveness_ttl_ms
    }

    pub async fn register_worker(
        &self,
        worker_id: &WorkerId,
        instance_id: &WorkerInstanceId,
        namespace: &Namespace,
        lanes: &BTreeSet<LaneId>,
        capabilities: &BTreeSet<String>,
    ) -> Result<WorkerRegistration, FabricError> {
        self.engine
            .register_worker(
                worker_id,
                instance_id,
                namespace,
                lanes,
                capabilities,
                self.liveness_ttl_ms,
            )
            .await
    }

    pub async fn heartbeat_worker(
        &self,
        instance_id: &WorkerInstanceId,
        namespace: &Namespace,
    ) -> Result<(), FabricError> {
        self.engine.heartbeat_worker(instance_id, namespace).await
    }

    pub async fn mark_worker_dead(
        &self,
        instance_id: &WorkerInstanceId,
        namespace: &Namespace,
        reason: &str,
    ) -> Result<(), FabricError> {
        self.engine
            .mark_worker_dead(instance_id, namespace, reason)
            .await
    }

    pub async fn list_workers(
        &self,
        namespace: Option<&Namespace>,
    ) -> Result<Vec<WorkerSummary>, FabricError> {
        self.engine.list_workers(namespace).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flowfabric::core::types::{WorkerId, WorkerInstanceId};

    #[test]
    fn worker_registration_fields() {
        let reg = WorkerRegistration {
            worker_id: WorkerId::new("w1"),
            instance_id: WorkerInstanceId::new("inst1"),
            capabilities: vec!["gpu=true".into(), "model=large".into()],
            registered_at_ms: 1000,
        };
        assert_eq!(reg.worker_id.as_str(), "w1");
        assert_eq!(reg.instance_id.as_str(), "inst1");
        assert_eq!(reg.capabilities.len(), 2);
        assert_eq!(reg.registered_at_ms, 1000);
    }
}
