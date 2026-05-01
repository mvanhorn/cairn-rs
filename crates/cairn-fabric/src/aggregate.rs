use std::sync::Arc;

use cairn_store::event_log::EventLog;
use cairn_store::projections::FfLeaseHistoryCursorStore;
use tokio::task::JoinHandle;

use crate::boot::FabricRuntime;
use crate::config::{BackendKind, FabricConfig};
use crate::engine::{ControlPlaneBackend, Engine, ValkeyEngine};
use crate::error::FabricError;
use crate::event_bridge::EventBridge;
use crate::lease_history_subscriber::LeaseHistorySubscriber;
use crate::services::{
    FabricBudgetService, FabricQuotaService, FabricRotationService, FabricRunService,
    FabricSchedulerService, FabricSessionService, FabricTaskService, FabricWorkerService,
};
use crate::signal_bridge::SignalBridge;

pub struct FabricServices {
    pub runtime: Arc<FabricRuntime>,
    pub bridge: Arc<EventBridge>,
    /// Cairn-side read abstraction over FF state. Every service that
    /// needs to read an execution / flow / edge snapshot goes through
    /// this handle instead of reaching into Valkey directly. One impl
    /// today ([`ValkeyEngine`]); swappable when FF 0.3 ships the
    /// upstream `describe_*` primitives (FlowFabric#58).
    pub engine: Arc<dyn Engine>,
    /// FCALL-shaped control-plane backend. Exposed alongside
    /// [`Self::engine`] so tests + callers that build fresh service
    /// instances on the same runtime (e.g. simulating a process
    /// restart between FCALLs) can pass it into the constructor.
    pub control_plane: Arc<dyn ControlPlaneBackend>,
    pub runs: FabricRunService,
    pub tasks: FabricTaskService,
    pub sessions: FabricSessionService,
    pub scheduler: FabricSchedulerService,
    pub worker: FabricWorkerService,
    pub budgets: FabricBudgetService,
    pub quotas: FabricQuotaService,
    pub rotation: FabricRotationService,
    pub signals: SignalBridge,
    bridge_handle: JoinHandle<()>,
    lease_history: Option<LeaseHistorySubscriber>,
}

impl FabricServices {
    pub async fn start(
        config: FabricConfig,
        event_log: Arc<dyn EventLog + Send + Sync>,
    ) -> Result<Self, FabricError> {
        Self::start_inner(config, event_log, None).await
    }

    /// Variant that wires the lease-history subscriber against a
    /// cursor-store implementation. When `None`, the subscriber is
    /// skipped — useful for tests that don't want the background
    /// tail running against a scratch Valkey.
    pub async fn start_with_lease_history(
        config: FabricConfig,
        event_log: Arc<dyn EventLog + Send + Sync>,
        cursor_store: Arc<dyn FfLeaseHistoryCursorStore>,
    ) -> Result<Self, FabricError> {
        Self::start_inner(config, event_log, Some(cursor_store)).await
    }

    async fn start_inner(
        config: FabricConfig,
        event_log: Arc<dyn EventLog + Send + Sync>,
        cursor_store: Option<Arc<dyn FfLeaseHistoryCursorStore>>,
    ) -> Result<Self, FabricError> {
        // Defensive: re-run validation. `FabricConfig::from_env` already
        // calls it, but callers that build a config via struct-literal
        // (several tests do) may skip it. Surfacing
        // `FabricError::Config` here is strictly better than panicking
        // deeper in startup on a mismatched backend_kind / feature
        // combination. (Copilot review, PR #600.)
        config.validate()?;

        // PR-C3: runtime dispatch on the always-compiled `backend_kind`
        // selector. Each arm is **self-contained** — it owns the full
        // construction path for its backend and returns `Self` directly.
        // No fall-through: this is the contract PR-C4 relies on when it
        // replaces the `unimplemented!` in the Postgres arm with a real
        // `PostgresFabricRuntime::start` body. A restructuring back to
        // "match then shared valkey tail" would re-introduce the bug
        // the Gemini review on PR #600 flagged — the Postgres arm would
        // silently fall into Valkey init.
        //
        // `FabricConfig::validate` has already rejected the
        // "backend_kind requested but feature disabled" combination by
        // the time we reach this match, so the Postgres arm only fires
        // on a binary that genuinely linked the PG stack.
        match config.backend_kind {
            BackendKind::Valkey => Self::start_valkey(config, event_log, cursor_store).await,
            BackendKind::Postgres => {
                // PR-C4a + PR-C4b shipped the PG control-plane surface
                // (`PostgresFabricRuntime::start` +
                // `PostgresControlPlane`: 27 bucket-B trait methods
                // wired, 3 bucket-A methods as direct delegates, 5
                // bucket-C methods returning typed
                // `EngineError::Unavailable`). The full
                // `FabricServices` aggregate still takes
                // `Arc<FabricRuntime>` concretely on every service
                // constructor (`run_service.rs`, `task_service.rs`,
                // `session_service.rs`, `scheduler_service.rs`,
                // `quota_service.rs`, …). Lifting those to a
                // backend-agnostic runtime handle is PR-C4c's scope
                // (cairn-rs #602) — it touches ~30 service
                // constructors + the boot path in cairn-app. Today
                // the Postgres arm surfaces a loud, cross-referenced
                // failure so a misconfigured
                // `CAIRN_FABRIC_BACKEND=postgres` launch fails
                // informatively rather than silently falling into the
                // Valkey path.
                let _ = event_log;
                let _ = cursor_store;
                Err(FabricError::Config(
                    "CAIRN_FABRIC_BACKEND=postgres supports control-plane methods via \
                     PostgresControlPlane (PR-C4a + PR-C4b shipped 27 bucket-B + 3 bucket-A \
                     trait methods live-tested on a PG container; 5 bucket-C methods return \
                     typed EngineError::Unavailable — see docs/design/postgres-parity-gaps.md) \
                     but cannot boot the full cairn service aggregate yet — service \
                     constructors (run/task/session/scheduler/quota) still hold a concrete \
                     Arc<FabricRuntime> (Valkey runtime). Tracked at cairn-rs #602 (PR-C4c). \
                     Current options: (a) use CAIRN_FABRIC_BACKEND=valkey for full app mode, \
                     or (b) instantiate PostgresControlPlane directly for control-plane-only \
                     integrations (see docs/design/postgres-parity-gaps.md §'What works today \
                     on fabric-postgres')."
                        .into(),
                ))
            }
        }
    }

    /// Valkey-backend construction path. Owns the full startup sequence
    /// — runtime handshake, bridge wiring, engine construction,
    /// lease-history subscriber, service aggregate — and returns
    /// `Self`. Extracted out of `start_inner` so the backend-kind match
    /// arm can stay a one-liner and the Postgres arm in PR-C4 can
    /// similarly own its full startup sequence without any shared tail.
    async fn start_valkey(
        config: FabricConfig,
        event_log: Arc<dyn EventLog + Send + Sync>,
        cursor_store: Option<Arc<dyn FfLeaseHistoryCursorStore>>,
    ) -> Result<Self, FabricError> {
        let runtime = Arc::new(FabricRuntime::start(config).await?);
        let (bridge, bridge_handle) = EventBridge::start(event_log);
        let bridge = Arc::new(bridge);

        // One concrete [`ValkeyEngine`] impl backs both the [`Engine`]
        // read/tag trait AND the [`ControlPlaneBackend`] FCALL trait.
        // Hold it as a concrete Arc first, then cast to each trait
        // object — avoids spinning up two independent handles that
        // would each open their own partition-config clones.
        let valkey_engine = Arc::new(ValkeyEngine::new(runtime.clone()));
        let engine: Arc<dyn Engine> = valkey_engine.clone();
        let control_plane: Arc<dyn ControlPlaneBackend> = valkey_engine;

        let lease_history = cursor_store.map(|store| {
            LeaseHistorySubscriber::start(
                runtime.backend().clone(),
                engine.clone(),
                bridge.clone(),
                store,
                runtime.config.worker_instance_id.to_string(),
            )
        });

        let result = Self::build_services(
            runtime.clone(),
            bridge.clone(),
            engine,
            control_plane,
            bridge_handle,
            lease_history,
        );

        match result {
            Ok(services) => {
                tracing::info!("fabric services aggregate ready");
                Ok(services)
            }
            Err((e, handle)) => {
                bridge.stop();
                handle.abort();
                drop(bridge);
                Err(e)
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn build_services(
        runtime: Arc<FabricRuntime>,
        bridge: Arc<EventBridge>,
        engine: Arc<dyn Engine>,
        control_plane: Arc<dyn ControlPlaneBackend>,
        bridge_handle: JoinHandle<()>,
        lease_history: Option<LeaseHistorySubscriber>,
    ) -> Result<Self, (FabricError, JoinHandle<()>)> {
        let runs = FabricRunService::new(
            runtime.clone(),
            bridge.clone(),
            engine.clone(),
            control_plane.clone(),
        );
        let tasks = FabricTaskService::new(
            runtime.clone(),
            bridge.clone(),
            engine.clone(),
            control_plane.clone(),
        );
        let sessions = FabricSessionService::new(
            runtime.clone(),
            bridge.clone(),
            engine.clone(),
            control_plane.clone(),
        );
        let scheduler = FabricSchedulerService::new(&runtime);
        let worker = FabricWorkerService::new(engine.clone());
        let budgets = FabricBudgetService::new(control_plane.clone());
        let quotas = FabricQuotaService::new(control_plane.clone(), runtime.clone());
        let rotation = FabricRotationService::new(control_plane.clone());
        let signals = SignalBridge::new(&runtime, engine.clone());

        Ok(Self {
            runtime,
            bridge,
            engine,
            control_plane,
            runs,
            tasks,
            sessions,
            scheduler,
            worker,
            budgets,
            quotas,
            rotation,
            signals,
            bridge_handle,
            lease_history,
        })
    }

    pub async fn shutdown(self) {
        let Self {
            runtime,
            bridge,
            engine: _,
            control_plane: _,
            runs: _,
            tasks: _,
            sessions: _,
            scheduler: _,
            worker: _,
            budgets: _,
            quotas: _,
            rotation: _,
            signals: _,
            bridge_handle,
            lease_history,
        } = self;

        if let Some(lh) = lease_history {
            lh.shutdown().await;
        }

        bridge.stop();
        drop(bridge);
        if let Err(e) = bridge_handle.await {
            tracing::warn!(error = %e, "event bridge consumer task panicked");
        }

        match Arc::try_unwrap(runtime) {
            Ok(rt) => rt.shutdown().await,
            Err(arc) => {
                tracing::warn!(
                    refs = Arc::strong_count(&arc),
                    "fabric runtime has outstanding references, skipping engine shutdown"
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fabric_config_from_env_defaults() {
        use flowfabric::core::backend::BackendConnection;
        // Crate-shared lock so this test serialises against
        // `config::tests` (which also flips `CAIRN_FABRIC_URL`). Two
        // private Mutexes in sibling modules don't actually serialise —
        // that's what flaked this test on parallel cargo test runs.
        let _guard = crate::config::ENV_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        std::env::remove_var("CAIRN_FABRIC_URL");
        std::env::remove_var("CAIRN_FABRIC_LEASE_TTL_MS");
        std::env::remove_var("CAIRN_FABRIC_MAX_TASKS");
        std::env::remove_var("CAIRN_FABRIC_GRANT_TTL_MS");
        let config = FabricConfig::from_env().unwrap();
        match &config.backend.connection {
            BackendConnection::Valkey(vk) => {
                assert_eq!(vk.host, "localhost");
                assert_eq!(vk.port, 6379);
            }
            other => panic!("expected Valkey backend, got {other:?}"),
        }
    }
}
