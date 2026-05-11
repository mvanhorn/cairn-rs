//! Binary-only modules for `cairn-app`.
//!
//! These modules compile only into the `cairn-app` binary (they are
//! declared in `main.rs`, not `lib.rs`). Grouping them under this module
//! makes the lib-vs-bin boundary visible at the filesystem level
//! (closes #445). The `bin_` file-name prefix is retained so a reader
//! opening one of these files in isolation still sees the binary
//! scope at a glance.

pub mod bin_admin;
pub mod bin_events;
pub mod bin_export;
pub mod bin_frontend;
pub mod bin_handlers;
pub mod bin_health;
pub mod bin_providers;
pub mod bin_router;
#[cfg(target_os = "linux")]
pub mod bin_sandboxed_agent;
pub mod bin_seed;
pub mod bin_state;
pub mod bin_types;
pub mod bin_websocket;
