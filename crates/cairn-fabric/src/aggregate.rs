use std::sync::Arc;

use cairn_store::event_log::EventLog;
use cairn_store::projections::FfLeaseHistoryCursorStore;
use tokio::task::JoinHandle;

use crate::boot::FabricRuntime;
use crate::config::{BackendKind, FabricConfig};
#[cfg(feature = "fabric-postgres")]
use crate::engine::PostgresControlPlane;
use crate::engine::{ControlPlaneBackend, Engine, ValkeyEngine};
use crate::error::FabricError;
use crate::event_bridge::EventBridge;
use crate::lease_history_subscriber::LeaseHistorySubscriber;
#[cfg(feature = "fabric-postgres")]
use crate::postgres_boot::PostgresFabricRuntime;
use crate::runtime_handle::FabricRuntimeHandle;
use crate::services::{
    FabricBudgetService, FabricQuotaService, FabricRotationService, FabricRunService,
    FabricSchedulerService, FabricSessionService, FabricTaskService, FabricWorkerService,
};
use crate::signal_bridge::SignalBridge;

pub struct FabricServices {
    /// Backend-agnostic view into the fabric runtime. Every service in
    /// the aggregate holds a clone of this same handle so the whole
    /// construction chain is drop-in compatible with both the Valkey
    /// [`FabricRuntime`] and the Postgres [`PostgresFabricRuntime`].
    /// Pre-PR-C4c this was a concrete `Arc<FabricRuntime>`; callers
    /// that need Valkey-specific accessors (`client`, `ff_metrics`)
    /// now route through [`Self::valkey_runtime`].
    pub runtime: Arc<dyn FabricRuntimeHandle>,
    /// Concrete Valkey runtime handle. Populated on
    /// [`BackendKind::Valkey`] boots only; `None` on Postgres boots.
    /// External consumers (cairn-app's `/metrics` handler, admin
    /// endpoints, backfill utility) that need `ferriskey::Client` or
    /// `ff_observability::Metrics` read through this slot and gate
    /// their code on the `Some(_)` branch.
    pub valkey_runtime: Option<Arc<FabricRuntime>>,
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
    pub signals: Arc<SignalBridge>,
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
                // PR-C4c (cairn-rs #602): the full service aggregate
                // now boots on Postgres. Every service constructor
                // takes `Arc<dyn FabricRuntimeHandle>` (PR-C4c)
                // instead of `Arc<FabricRuntime>`, and
                // `PostgresFabricRuntime` impls the same trait so
                // the PG runtime is drop-in. FF 0.14 closed the
                // final bucket-C gaps (worker registry via FF#473,
                // list_incoming_edges via FF#477) so every trait
                // method on `PostgresControlPlane` has a real body
                // now. The two remaining PG-vs-Valkey deltas
                // (`FabricSchedulerService::claim_for_worker`,
                // `SignalBridge::deliver_*_signal`) live at the
                // service layer and are app-layer-gated to Valkey;
                // see `docs/design/postgres-parity-gaps.md`.
                #[cfg(feature = "fabric-postgres")]
                {
                    Self::start_postgres(config, event_log, cursor_store).await
                }
                #[cfg(not(feature = "fabric-postgres"))]
                {
                    // `FabricConfig::validate` already rejects
                    // `backend_kind=postgres` when the
                    // `fabric-postgres` feature is disabled, so this
                    // arm is unreachable in practice. Kept for
                    // exhaustiveness — a future regression that
                    // weakens the cross-check would surface here
                    // instead of hitting an `unreachable!()` on a
                    // production binary.
                    let _ = event_log;
                    let _ = cursor_store;
                    Err(FabricError::Config(
                        "CAIRN_FABRIC_BACKEND=postgres selected but the fabric-postgres \
                         feature is not enabled on this binary. Rebuild cairn-app with \
                         --features cairn-fabric/fabric-postgres or switch to \
                         CAIRN_FABRIC_BACKEND=valkey."
                            .into(),
                    ))
                }
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

        // Coerce to the trait-object runtime handle once — every
        // service constructor + the aggregate's `runtime` field
        // share the same Arc.
        let runtime_handle: Arc<dyn FabricRuntimeHandle> = runtime.clone();

        let result = Self::build_services(
            runtime_handle,
            Some(runtime),
            bridge.clone(),
            engine,
            control_plane,
            bridge_handle,
            lease_history,
        );

        match result {
            Ok(services) => {
                tracing::info!("fabric services aggregate ready (valkey backend)");
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

    /// Postgres-backend construction path (PR-C4c).
    ///
    /// Symmetric with [`Self::start_valkey`]: dials Postgres via
    /// [`PostgresFabricRuntime::start`], stands up the event bridge,
    /// wraps the PG control-plane into the cairn-side trait objects,
    /// and hands the runtime handle to the shared
    /// [`Self::build_services`] path. The PG runtime satisfies
    /// [`FabricRuntimeHandle`] with the same 7 accessors the Valkey
    /// runtime exposes; the two `start_*` paths differ only in which
    /// concrete runtime + which `Engine`/`ControlPlaneBackend`
    /// implementation they install.
    ///
    /// # Deferred
    ///
    /// - **Lease-history subscriber.** PG's `subscribe_lease_history`
    ///   is stream-shaped and not yet wired. `cursor_store` is
    ///   accepted for signature parity but ignored on PG for now;
    ///   boot-time log surfaces the skip.
    /// - **Scanner supervisor.** FF's PG backend ships its own
    ///   per-partition reclaim scanners; cairn doesn't need the
    ///   cairn-side supervisor wiring the Valkey path uses. No-op.
    #[cfg(feature = "fabric-postgres")]
    async fn start_postgres(
        config: FabricConfig,
        event_log: Arc<dyn EventLog + Send + Sync>,
        cursor_store: Option<Arc<dyn FfLeaseHistoryCursorStore>>,
    ) -> Result<Self, FabricError> {
        let runtime = Arc::new(PostgresFabricRuntime::start(config).await?);
        let (bridge, bridge_handle) = EventBridge::start(event_log);
        let bridge = Arc::new(bridge);

        // One concrete [`PostgresControlPlane`] impl backs both
        // the [`Engine`] read/tag trait AND the
        // [`ControlPlaneBackend`] FCALL-shape trait. Same pattern
        // as `start_valkey` — one construction, two coercions.
        let pg_control_plane: Arc<PostgresControlPlane> = runtime.control_plane.clone();
        let engine: Arc<dyn Engine> = pg_control_plane.clone();
        let control_plane: Arc<dyn ControlPlaneBackend> = pg_control_plane;

        // Lease-history subscriber on PG requires
        // `EngineBackend::subscribe_lease_history` stream wiring
        // (tracked in `postgres_boot.rs` scope-bound doc). Skip
        // gracefully if the caller supplied a cursor store.
        if cursor_store.is_some() {
            tracing::warn!(
                "start_postgres: cursor_store supplied but LeaseHistorySubscriber is not \
                 wired on the PG backend yet (tracked in postgres_boot.rs scope bounds) — \
                 the subscriber is skipped; cursor writes will not happen on this boot"
            );
        }
        let lease_history = None;

        let runtime_handle: Arc<dyn FabricRuntimeHandle> = runtime.clone();

        let result = Self::build_services(
            runtime_handle,
            // No Valkey runtime on the PG boot path — external
            // consumers (cairn-app's `/metrics` ff_metrics render,
            // admin backfill) must gate on `valkey_runtime.is_some()`.
            None,
            bridge.clone(),
            engine,
            control_plane,
            bridge_handle,
            lease_history,
        );

        match result {
            Ok(services) => {
                tracing::info!("fabric services aggregate ready (postgres backend)");
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

    /// Build the service aggregate from a constructed runtime handle
    /// + bridge + trait-routed engine + control-plane.
    ///
    /// PR-C4c lifted this off the concrete `Arc<FabricRuntime>` —
    /// every field of the returned aggregate is backend-agnostic
    /// except `valkey_runtime`, which the caller populates with
    /// `Some(Arc<FabricRuntime>)` on the Valkey path and `None` on
    /// the Postgres path. Both backends flow through the same
    /// construction chain; the split lives entirely in the
    /// `start_*` helpers above.
    #[allow(clippy::too_many_arguments)]
    fn build_services(
        runtime: Arc<dyn FabricRuntimeHandle>,
        valkey_runtime: Option<Arc<FabricRuntime>>,
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
        // Preserve pre-FF-0.14 behaviour: the bespoke Valkey path stamped
        // `PEXPIRE` at `lease_ttl_ms * 3`. FF 0.14's
        // `RegisterWorkerArgs::liveness_ttl_ms` replaces that PEXPIRE —
        // keep the same 3× multiplier so dashboards + soak behaviour
        // carry over identically.
        let worker =
            FabricWorkerService::new(engine.clone(), runtime.lease_ttl_ms().saturating_mul(3));
        let budgets = FabricBudgetService::new(control_plane.clone());
        let quotas = FabricQuotaService::new(control_plane.clone(), runtime.clone());
        let rotation = FabricRotationService::new(control_plane.clone());
        let signals = Arc::new(SignalBridge::new(runtime.clone()));

        Ok(Self {
            runtime,
            valkey_runtime,
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
            valkey_runtime,
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

        // Drop the trait-object runtime handle so the inner
        // reference count drops before we try to unwrap the
        // concrete Valkey runtime (if any). Services all borrow
        // their runtime through this handle, so dropping it first
        // releases the 5 service-local clones.
        drop(runtime);

        // PR-C4c: only the Valkey path carries a concrete handle
        // with inherent shutdown semantics (FF engine scanners,
        // bridge queues, `ff-observability` metrics teardown). The
        // Postgres runtime's only live resource is the FF backend
        // Arc — it's released when the last reference drops.
        if let Some(valkey_runtime) = valkey_runtime {
            match Arc::try_unwrap(valkey_runtime) {
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
