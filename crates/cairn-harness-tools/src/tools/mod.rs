//! Concrete `HarnessTool` implementations — one unit struct per upstream tool.

mod bash;
mod glob;
mod grep;
#[doc(hidden)]
pub mod lsp;
mod read;
mod webfetch;
mod write;

pub use bash::{HarnessBash, HarnessBashKill, HarnessBashOutput};
pub use glob::HarnessGlob;
pub use grep::HarnessGrep;
pub use lsp::HarnessLsp;
pub use read::HarnessRead;
pub use webfetch::HarnessWebFetch;
pub use write::{HarnessEdit, HarnessMultiEdit, HarnessWrite};

#[doc(hidden)]
pub use lsp::__clear_client_cache_for_tests;

/// Test-only per-entry lookup from the private `write` submodule. Gated
/// behind `#[doc(hidden)]` so it doesn't appear in rustdoc. Scoped to a
/// single `(ctx, project)` tuple so parallel-test execution doesn't
/// cross-pollute the process-global `LEDGERS` map.
#[doc(hidden)]
pub use write::__cache_contains_for_tests as __ledger_cache_contains_for_tests;

/// Internal cache eviction helpers — called from `crate::evict_run`.
/// Kept `pub(crate)` so downstream code cannot depend on them.
pub(crate) use lsp::evict_run_client;
pub(crate) use write::evict_run_ledger;
