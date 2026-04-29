//! cairn-fabric — bridges cairn-rs to the FlowFabric Valkey-native execution engine.
//!
//! FlowFabric runs all execution state as atomic Valkey FCALLs (Lua functions),
//! giving cairn-rs durable, lease-based task execution with sub-millisecond
//! state transitions, automatic lease renewal, retry scheduling, suspension
//! with signal-driven resume, multi-dimensional budgets, and rate-limiting quotas.
//!
//! # Architecture
//!
//! ```text
//! User API request
//!   → cairn-app (HTTP/SSE)
//!     → cairn-fabric (this crate)
//!       → FlowFabric Valkey FCALL  (execution source of truth)
//!       → cairn-store EventLog     (audit trail + read model sync)
//! ```
//!
//! # Entry point
//!
//! [`FabricServices`] is the single aggregate that wires all Fabric-backed
//! services. Constructed at startup via [`FabricServices::start()`], it holds
//! the Valkey connection, background engine scanners, and every service impl.
//!
//! ```rust,ignore
//! use cairn_fabric::{FabricConfig, FabricServices};
//!
//! let config = FabricConfig::from_env()?;
//! let fabric = FabricServices::start(config, event_log).await?;
//!
//! // Use fabric.runs, fabric.tasks, fabric.budgets, fabric.quotas, etc.
//! // Worker loop — scheduler-routed claim, admission checks on every
//! // grant:
//! let worker = cairn_fabric::CairnWorker::connect(&worker_config, bridge.clone()).await?;
//! loop {
//!     let grant = match fabric.scheduler.claim_for_worker(
//!         &lane, &worker_id, &worker_instance_id, grant_ttl_ms,
//!     ).await? {
//!         Some(g) => g,
//!         None => { tokio::time::sleep(poll_interval).await; continue; }
//!     };
//!     let task = worker.claim_from_grant(lane.clone(), grant).await?;
//!     task.complete_with_result(None).await?;
//! }
//!
//! fabric.shutdown().await;
//! ```
//!
//! # Modules
//!
//! | Module | Purpose |
//! |--------|---------|
//! | [`aggregate`] | [`FabricServices`] — single wiring point for all services |
//! | [`services`] | RunService, TaskService, BudgetService, QuotaService impls via FCALL |
//! | [`worker_sdk`] | [`CairnWorker`] / [`CairnTask`] — claim-process-complete loop |
//! | [`boot`] | [`FabricRuntime`] — Valkey connection + engine scanner lifecycle |
//! | [`config`] | [`FabricConfig`] — env-var-driven configuration |
//! | [`id_map`] | Deterministic cairn ID → FlowFabric ID mapping (UUID v5) |
//! | [`state_map`] | FlowFabric PublicState ↔ cairn RunState/TaskState conversion |
//! | [`event_bridge`] | Async bridge: Fabric mutations → cairn-store RuntimeEvents |
//! | [`stream`] | Tool/LLM frame logging via FlowFabric output streams |
//! | [`suspension`] | Typed suspension builders for approval/subagent/tool-result waits |
//! | [`signal_bridge`] | cairn domain events → FlowFabric signal delivery |

// ── Always-compiled backend-agnostic surface ──────────────────────────
//
// These modules are pure types / trait definitions / configuration
// parsers and do not pull in any `ferriskey` or `ff_backend_valkey`
// symbols. They stay compiled under `--no-default-features` so the
// crate can serve as the single source of truth for backend-agnostic
// types (FabricError, FabricConfig, Engine + ControlPlaneBackend
// traits, control_plane_types, snapshots, id_map, state_map,
// suspension helpers, fcall ARGV builders, constants).
pub mod config;
pub mod constants;
pub mod engine;
pub mod error;
pub mod fcall;
pub mod helpers;
pub mod id_map;
pub mod state_map;
pub mod suspension;

// ── Valkey-backed runtime surface ────────────────────────────────────
//
// Everything below holds a `ferriskey::Client`, a
// `flowfabric::valkey::ValkeyBackend`, or depends on them transitively
// via `FabricRuntime`. Gated behind the `fabric-valkey` feature so a
// `--no-default-features` build is a proof-of-backend-agnosticism
// compile. PR-C adds the sibling `fabric-postgres` feature that wires
// the PostgreSQL backend against FF 0.12's trait-routed entry points.
#[cfg(feature = "fabric-valkey")]
pub mod aggregate;
#[cfg(feature = "fabric-valkey")]
pub mod boot;
#[cfg(feature = "fabric-valkey")]
pub mod event_bridge;
#[cfg(feature = "fabric-valkey")]
pub mod instance_tag_backfill;
#[cfg(feature = "fabric-valkey")]
pub mod lease_history_subscriber;
#[cfg(feature = "fabric-valkey")]
pub mod services;
#[cfg(feature = "fabric-valkey")]
pub mod signal_bridge;
#[cfg(feature = "fabric-valkey")]
pub mod stream;
#[cfg(feature = "fabric-valkey")]
pub mod version_check;
#[cfg(feature = "fabric-valkey")]
pub mod worker_sdk;

/// Valkey testcontainers harness for integration tests. Gated on the
/// `test-harness` cargo feature (which in turn implies `fabric-valkey`)
/// so production binaries don't link `testcontainers`.
#[cfg(feature = "test-harness")]
pub mod test_harness;
// `test_support` stays on the plain `#[cfg(test)]` gate it had pre-PR-B:
// unit-test modules inside always-on `src/fcall/*` and `src/services/*`
// import `crate::test_support::{test_eid, default_test_backend}`. Those
// helpers pull only backend-agnostic FF core types
// (`core::backend::BackendConfig`, `core::partition::PartitionConfig`,
// `core::types::*`) + `uuid` — no ferriskey — so the module compiles
// under `--no-default-features` and unit tests stay reachable there.
// (Copilot review, PR #583.)
#[cfg(test)]
pub(crate) mod test_support;

#[cfg(feature = "fabric-valkey")]
pub use aggregate::FabricServices;
#[cfg(feature = "fabric-valkey")]
pub use boot::FabricRuntime;
pub use config::FabricConfig;
pub use error::FabricError;
#[cfg(feature = "fabric-valkey")]
pub use worker_sdk::{CairnTask, CairnWorker};
