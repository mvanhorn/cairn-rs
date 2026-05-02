//! Regression coverage for issue #606 — unbounded growth of the two
//! long-lived caches in `cairn-harness-tools`:
//!
//! * `LEDGERS` — per-run write-tool read/edit ledger (in `tools/write.rs`).
//! * `CLIENTS` — spawned LSP servers (in `tools/lsp.rs`).
//!
//! Both are now keyed by `(tenant, workspace, project, session, run)` and
//! evicted via the public `evict_run(&ToolContext, &ProjectKey)` hook.
//! These tests pin the eviction contract so the orchestrator call site
//! does not silently stop evicting if internals drift.

use std::sync::Arc;

use cairn_domain::ProjectKey;
use cairn_harness_tools::{
    __ledger_cache_contains_for_tests, evict_run, HarnessBuiltin, HarnessRead,
};
use cairn_tools::builtins::{ToolContext, ToolHandler};
use serde_json::json;
use tempfile::TempDir;

/// Every test uses a unique `ProjectKey` so the process-global
/// `LEDGERS` / `CLIENTS` caches don't cross-pollute under cargo's
/// parallel test execution. Callers pass a short discriminator.
fn unique_project(tag: &str) -> ProjectKey {
    ProjectKey::new(
        format!("tenant-{tag}"),
        format!("workspace-{tag}"),
        format!("project-{tag}"),
    )
}

fn ctx_with_run(dir: &TempDir, session: &str, run: &str) -> ToolContext {
    let mut c = ToolContext::default();
    c.working_dir = dir.path().to_path_buf();
    c.session_id = Some(session.to_owned());
    c.run_id = Some(run.to_owned());
    c
}

// ── LEDGERS cache (write-tool) ───────────────────────────────────────────────

#[tokio::test]
async fn ledger_cache_evicts_on_run_terminal() {
    let project = unique_project("ledger-evicts-on-terminal");
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("a.txt");
    std::fs::write(&path, "hello\n").unwrap();

    let read = HarnessBuiltin::<HarnessRead>::new();
    let ctx = ctx_with_run(&dir, "session-evict-1", "run-evict-1");

    read.execute_with_context(&project, json!({ "path": path.to_string_lossy() }), &ctx)
        .await
        .expect("read under the run should succeed");

    assert!(
        __ledger_cache_contains_for_tests(&ctx, &project),
        "read under the run should have populated the LEDGERS cache",
    );

    evict_run(&ctx, &project);

    assert!(
        !__ledger_cache_contains_for_tests(&ctx, &project),
        "evict_run must drop the cached ledger",
    );
}

#[tokio::test]
async fn evict_run_on_ledger_cache_is_idempotent() {
    let project = unique_project("ledger-idempotent");
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("b.txt");
    std::fs::write(&path, "world\n").unwrap();

    let read = HarnessBuiltin::<HarnessRead>::new();
    let ctx = ctx_with_run(&dir, "session-evict-idem", "run-evict-idem");
    read.execute_with_context(&project, json!({ "path": path.to_string_lossy() }), &ctx)
        .await
        .expect("read under the run should succeed");

    // Evict twice — second call must be a no-op, not a panic.
    evict_run(&ctx, &project);
    evict_run(&ctx, &project);

    assert!(!__ledger_cache_contains_for_tests(&ctx, &project));
}

#[tokio::test]
async fn evict_run_only_drops_the_scoped_run() {
    // Eviction for run A must not affect run B's cached ledger.
    let project = unique_project("ledger-scoped-per-run");
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("c.txt");
    std::fs::write(&path, "shared\n").unwrap();

    let read = HarnessBuiltin::<HarnessRead>::new();
    let ctx_a = ctx_with_run(&dir, "session-evict-scope", "run-A");
    let ctx_b = ctx_with_run(&dir, "session-evict-scope", "run-B");

    read.execute_with_context(&project, json!({ "path": path.to_string_lossy() }), &ctx_a)
        .await
        .expect("read in run A");
    read.execute_with_context(&project, json!({ "path": path.to_string_lossy() }), &ctx_b)
        .await
        .expect("read in run B");

    assert!(__ledger_cache_contains_for_tests(&ctx_a, &project));
    assert!(__ledger_cache_contains_for_tests(&ctx_b, &project));

    evict_run(&ctx_a, &project);

    assert!(
        !__ledger_cache_contains_for_tests(&ctx_a, &project),
        "run A should be evicted",
    );
    assert!(
        __ledger_cache_contains_for_tests(&ctx_b, &project),
        "run B must not be disturbed by run A's eviction",
    );
}

// ── CLIENTS cache (LSP) ──────────────────────────────────────────────────────

// Each LSP test uses a unique `(project, session, run)` tuple so sibling
// tests can't cross-pollute the process-global CLIENTS cache under
// cargo's default parallel execution. We deliberately DO NOT call
// `__clear_client_cache_for_tests()` here — draining the entire map would
// race with sibling LSP tests that have already populated their own
// entries. Unique-key scoping is the isolation mechanism.

#[tokio::test]
async fn lsp_cache_evicts_on_run_terminal() {
    use cairn_harness_tools::tools::lsp::client_for;

    let project = unique_project("lsp-evicts-on-terminal");
    let dir = TempDir::new().unwrap();
    let ctx = ctx_with_run(&dir, "session-lsp-evict", "run-lsp-evict");

    // Spawn + cache the client.
    let a = client_for(&ctx, &project);
    let b = client_for(&ctx, &project);
    assert!(
        Arc::ptr_eq(&a, &b),
        "same (session, run) must reuse the cached LSP client pre-eviction",
    );

    // Evict.
    evict_run(&ctx, &project);

    // Next client_for must produce a new Arc.
    let c = client_for(&ctx, &project);
    assert!(
        !Arc::ptr_eq(&a, &c),
        "after evict_run the LSP client cache must return a fresh Arc",
    );
}

#[tokio::test]
async fn evict_run_on_lsp_cache_is_idempotent() {
    use cairn_harness_tools::tools::lsp::client_for;

    let project = unique_project("lsp-idempotent");
    let dir = TempDir::new().unwrap();
    let ctx = ctx_with_run(&dir, "session-lsp-idem", "run-lsp-idem");
    let _ = client_for(&ctx, &project);

    // Double-evict must not panic.
    evict_run(&ctx, &project);
    evict_run(&ctx, &project);
}

#[tokio::test]
async fn lsp_cache_is_scoped_per_run_not_per_session() {
    use cairn_harness_tools::tools::lsp::client_for;

    let project = unique_project("lsp-run-scoped");
    let dir = TempDir::new().unwrap();
    let ctx_run1 = ctx_with_run(&dir, "shared-session", "run-1");
    let ctx_run2 = ctx_with_run(&dir, "shared-session", "run-2");

    let a = client_for(&ctx_run1, &project);
    let b = client_for(&ctx_run2, &project);
    assert!(
        !Arc::ptr_eq(&a, &b),
        "same session but different run_id must not share an LSP client — cache is run-scoped",
    );
}

// ── No-session / no-run contexts ─────────────────────────────────────────────
//
// Each of the "no-id" tests uses a unique `ProjectKey` so the empty-string
// fallback keys (`ledger_key` substitutes `""` for missing ids) don't
// collide with sibling tests that share the default `ToolContext`.

#[tokio::test]
async fn evict_run_without_session_id_is_noop() {
    // ToolContext::default() has no session_id/run_id. The LSP cache
    // skips insertion entirely; the write-ledger cache uses empty-string
    // fallbacks — either way, `evict_run` must be a safe no-op.
    let project = unique_project("evict-no-session");
    let ctx = ToolContext::default();
    evict_run(&ctx, &project); // must not panic

    // Sanity: no entry was created for this distinct project.
    assert!(!__ledger_cache_contains_for_tests(&ctx, &project));
}

#[tokio::test]
async fn evict_run_without_run_id_is_noop() {
    let project = unique_project("evict-no-run");
    let mut ctx = ToolContext::default();
    ctx.session_id = Some("session-only".to_owned());
    // run_id deliberately unset.
    evict_run(&ctx, &project); // must not panic
}

// Note: a cache-size monotonicity end-to-end test was intentionally omitted.
// `__clear_ledger_cache_for_tests` + `__ledger_cache_size_for_tests` race
// with sibling tests under cargo's default parallel execution (the LEDGERS
// map is process-global). The per-entry `__ledger_cache_contains_for_tests`
// helper above gives the same signal without the race.
