//! Harness-tools adapter — bridges `@agent-sh/harness-*` Rust crates into
//! cairn's `cairn_tools::ToolHandler` surface.
//!
//! ## Layout
//!
//! ```text
//! HarnessTool        — shared associated-type trait (one impl per tool).
//! HarnessBuiltin<H>  — wrapper that implements cairn's `ToolHandler` for any
//!                      `HarnessTool`. Register as `Arc::new(HarnessBuiltin::<H>::new())`.
//! build_cairn_hook() — v1 permission hook: delegates to cairn's executor
//!                      pre-check (allow-all at the harness layer).
//! default_sensitive_patterns() — baseline deny list for permission policy.
//! From<harness_core::ToolError> for cairn_tools::ToolError — pass-through mapping.
//! ```
//!
//! ## Cache lifetime
//!
//! Two in-process caches back the write and LSP tools:
//!
//! * `tools::write::LEDGERS` — `harness-write` read-before-edit state,
//!   keyed by `(tenant, workspace, project, session_id, run_id)`.
//! * `tools::lsp::CLIENTS`   — spawned language-server clients, keyed by
//!   the same 5-tuple.
//!
//! Both are run-scoped so cross-tenant / cross-session / cross-run
//! coupling is impossible. The orchestrator calls [`evict_run`] on every
//! terminal `RunStateChanged`, bounding each map to the number of live
//! runs. Dropping the cached `Arc` causes `SpawnLspClient::close_session`
//! to run when the last reference is released, so language-server child
//! processes terminate promptly rather than lingering for the cairn-app
//! process lifetime (closes #606).

pub mod adapter;
pub mod error;
pub mod hook;
pub mod sensitive;
pub mod shell_policy;
#[doc(hidden)]
pub mod tools;

pub use adapter::{HarnessBuiltin, HarnessTool};
pub use hook::build_cairn_hook;
pub use sensitive::default_sensitive_patterns;
pub use shell_policy::{ShellPolicy, ShellVerdict};
pub use tools::{
    HarnessBash, HarnessBashKill, HarnessBashOutput, HarnessEdit, HarnessGlob, HarnessGrep,
    HarnessLsp, HarnessMultiEdit, HarnessRead, HarnessWebFetch, HarnessWrite,
};

#[doc(hidden)]
pub use tools::{__clear_client_cache_for_tests, __ledger_cache_contains_for_tests};

use cairn_domain::ProjectKey;
use cairn_tools::builtins::ToolContext;

/// Evict the harness-tools internal caches for the run described by
/// `(project, ctx.session_id, ctx.run_id)`.
///
/// The write-tool `LEDGERS` cache and the LSP `CLIENTS` cache both key
/// their entries by `(tenant, workspace, project, session, run)`. Without
/// an explicit eviction hook they would grow monotonically over the
/// cairn-app process lifetime. The orchestrator invokes this function on
/// every terminal `RunStateChanged` so the caches stay bounded to the
/// number of live runs.
///
/// Contract:
/// * Idempotent — repeated calls for the same run are no-ops after the
///   first.
/// * Scoped — only entries for `(tenant, workspace, project, session,
///   run)` are dropped; sibling runs are untouched.
/// * Never panics — missing identifiers are tolerated:
///   - The LSP cache stores nothing for contexts missing either
///     `session_id` or `run_id`, so eviction on those contexts is a
///     trivial no-op.
///   - The write-ledger cache keys on empty-string fallbacks when ids
///     are missing (see `tools/write.rs::ledger_key`); eviction targets
///     that fallback key and silently returns `None` from the map
///     `remove` if no entry is present.
///
/// Closes #606.
pub fn evict_run(ctx: &ToolContext, project: &ProjectKey) {
    tools::evict_run_ledger(ctx, project);
    tools::evict_run_client(ctx, project);
}
