//! Provider-domain read models.
//!
//! Split into per-entity submodules in #441 (was a single 218-LOC
//! `provider.rs` holding 13 unrelated read-model traits). Each
//! submodule owns one table or one tightly-cohesive cluster of tables:
//!
//! * `connection` — `provider_connections`
//! * `binding` — `provider_bindings` + `provider_binding_cost_stats`
//! * `health` — `provider_health` + `provider_health_schedules`
//! * `route_policy` — `route_policies`
//! * `cost` — `session_costs`, `project_costs`, `workspace_costs`,
//!   `run_costs`, `run_cost_alerts` (share the same upsert tx)
//! * `model` — RFC 009 model capability registry
//! * `budget` — `provider_budgets`
//! * `pool` — RFC 009 connection pools
//!
//! Callers that did `use cairn_store::projections::*` or resolved a
//! specific trait by its original path (`…::ProviderBindingReadModel`)
//! continue to work unchanged — every public symbol is re-exported at
//! the historical path via `pub use …::*` below and the `projections`
//! module's own `pub use provider::*` glob.

pub mod binding;
pub mod budget;
pub mod connection;
pub mod cost;
pub mod health;
pub mod model;
pub mod pool;
pub mod route_policy;

pub use binding::{ProviderBindingCostStatsReadModel, ProviderBindingReadModel};
pub use budget::ProviderBudgetReadModel;
pub use connection::ProviderConnectionReadModel;
pub use cost::{
    ProjectCostReadModel, RunCostAlertReadModel, RunCostReadModel, SessionCostReadModel,
};
pub use health::{ProviderHealthReadModel, ProviderHealthScheduleReadModel};
pub use model::ProviderModelReadModel;
pub use pool::ProviderPoolReadModel;
pub use route_policy::RoutePolicyReadModel;
