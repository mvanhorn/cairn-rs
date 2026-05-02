//! PR-C4c live-integration proof-point: the full `FabricServices`
//! aggregate boots on Postgres.
//!
//! # What this binary proves
//!
//! Before PR-C4c, `FabricServices::start` on
//! `CAIRN_FABRIC_BACKEND=postgres` returned
//! [`FabricError::Config`] because every service constructor
//! (`run_service`, `task_service`, `session_service`,
//! `scheduler_service`, `quota_service`, `signal_bridge`) held a
//! concrete `Arc<cairn_fabric::boot::FabricRuntime>` — a
//! Valkey-typed struct. No PG runtime could satisfy those
//! signatures, so full-app-mode boot failed loud but useless.
//!
//! PR-C4c introduces [`cairn_fabric::FabricRuntimeHandle`] — a
//! trait whose surface is 6 runtime-level config accessors
//! (`partition_config`, `worker_instance_id`, `lease_ttl_ms`,
//! `signal_dedup_ttl_ms`, `worker_capabilities`, `backend`) plus
//! two Valkey-specific escape hatches (`valkey_client`, `fcall`)
//! that return `None` / `Unavailable` on Postgres. Every service
//! constructor now holds `Arc<dyn FabricRuntimeHandle>`; the
//! aggregate is genuinely backend-agnostic at the service-wiring
//! layer.
//!
//! This test spins a real Postgres container, builds a fabric
//! config with `backend_kind = Postgres`, and asserts
//! `FabricServices::start` returns `Ok(_)`. A regression that
//! re-couples any service constructor to Valkey trips this binary.
//!
//! # Harness
//!
//! - One Postgres container per test-binary invocation, shared via
//!   [`shared_pg`]. Migrations applied once at container boot.
//! - The test binary exercises a single boot + shutdown; isolation
//!   against sibling test binaries is provided by the per-invocation
//!   container (a fresh PG instance per cargo test run). The
//!   aggregate construction does not touch per-execution rows, so
//!   additional parallelism inside this binary is not needed today.
//!
//! # Gating
//!
//! Requires both `fabric-postgres` (for `PostgresFabricRuntime`)
//! and `test-harness` (for `testcontainers-modules`).
//!
//! Run with:
//!   cargo test -p cairn-fabric --features "fabric-postgres,test-harness" \
//!     --test postgres_full_aggregate_boot

#![cfg(all(feature = "fabric-postgres", feature = "test-harness"))]

use std::sync::Arc;

use cairn_fabric::{FabricConfig, FabricServices};
use cairn_store::event_log::EventLog;
use cairn_store::InMemoryStore;
use ff_backend_postgres::{apply_migrations, PgPool};
use flowfabric::core::backend::BackendConfig;
use flowfabric::core::types::{LaneId, Namespace, WorkerId, WorkerInstanceId};
use sqlx::postgres::PgPoolOptions;
use testcontainers::{runners::AsyncRunner, ContainerAsync, ImageExt};
use testcontainers_modules::postgres::Postgres as PostgresImage;
use tokio::sync::OnceCell;

struct SharedPg {
    _container: ContainerAsync<PostgresImage>,
    _pool: PgPool,
    url: String,
}

static SHARED: OnceCell<Arc<SharedPg>> = OnceCell::const_new();

async fn shared_pg() -> Arc<SharedPg> {
    SHARED
        .get_or_init(|| async {
            let container = PostgresImage::default()
                .with_db_name("cairn_test")
                .with_user("cairn")
                .with_password("cairn")
                .with_tag("16-alpine")
                .start()
                .await
                .expect("failed to start postgres container");

            let host = container
                .get_host()
                .await
                .expect("container host unavailable");
            let port = container
                .get_host_port_ipv4(5432)
                .await
                .expect("container port unavailable");
            let url = format!("postgres://cairn:cairn@{host}:{port}/cairn_test");

            let pool = PgPoolOptions::new()
                .max_connections(4)
                .connect(&url)
                .await
                .expect("failed to connect to postgres");

            apply_migrations(&pool)
                .await
                .expect("failed to apply FF migrations");

            Arc::new(SharedPg {
                _container: container,
                _pool: pool,
                url,
            })
        })
        .await
        .clone()
}

fn pg_fabric_config(pg_url: &str) -> FabricConfig {
    FabricConfig {
        backend: BackendConfig::postgres(pg_url.to_string()),
        lane_id: LaneId::new("cairn"),
        worker_id: WorkerId::new("test-worker"),
        worker_instance_id: WorkerInstanceId::new("test-worker-i1"),
        namespace: Namespace::new("cairn-test"),
        lease_ttl_ms: 30_000,
        grant_ttl_ms: 5_000,
        max_concurrent_tasks: 1,
        signal_dedup_ttl_ms: 86_400_000,
        fcall_timeout_ms: 5_000,
        worker_capabilities: std::collections::BTreeSet::new(),
        // The PG runtime does not seed HMAC secrets — it has no
        // Lua suspension path. Passing a secret here is harmless
        // (ignored by the PG boot path). Keep a deterministic
        // dev secret present so `FabricConfig::validate` doesn't
        // trip on a Valkey-only validation rule reaching the PG
        // arm.
        waitpoint_hmac_secret: Some(
            "00000000000000000000000000000000000000000000000000000000000000aa".into(),
        ),
        waitpoint_hmac_kid: Some("cairn-test-k1".into()),
        backend_kind: cairn_fabric::config::BackendKind::Postgres,
    }
}

/// Full-aggregate boot on Postgres. Regression guard for PR-C4c.
///
/// Before PR-C4c this failed with `FabricError::Config`. Post-refactor
/// `FabricServices::start` constructs the full service aggregate
/// against a `PostgresFabricRuntime` and returns `Ok(_)`.
#[tokio::test]
async fn pg_full_service_aggregate_boots() {
    let pg = shared_pg().await;
    let config = pg_fabric_config(&pg.url);

    let event_log: Arc<dyn EventLog + Send + Sync> = Arc::new(InMemoryStore::default());

    let services = FabricServices::start(config, event_log)
        .await
        .expect("FabricServices::start must succeed on Postgres backend");

    // Smoke-level assertion: the four primary business services
    // (`runs`, `tasks`, `sessions`, `quotas`) are reachable fields on
    // the aggregate — if any of them failed to construct under the
    // new `FabricRuntimeHandle` trait the aggregate's builder would
    // have errored out before we got here. Address-of each field to
    // prove the move-out worked.
    let _ = &services.runs;
    let _ = &services.tasks;
    let _ = &services.sessions;
    let _ = &services.quotas;
    let _ = &services.budgets;
    let _ = &services.rotation;

    services.shutdown().await;
}
