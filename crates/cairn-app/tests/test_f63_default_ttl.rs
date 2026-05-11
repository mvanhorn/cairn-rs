//! F63 regression: the default `CAIRN_FABRIC_LEASE_TTL_MS` must be
//! `180_000` (3 minutes), not the previous `30_000` (30 seconds).
//!
//! # Bug (M1-v2 dogfood, 2026-04-27)
//!
//! On the F62 binary, the 30 s default lease TTL routinely expired
//! between operator-paced `POST /v1/runs/:id/orchestrate` calls. Every
//! expiry tripped F62's `TerminalWriteDeadlock` path and lost the LLM's
//! productive work. A previously-tried 600 s workaround was actively
//! harmful (F43 triage: 20× zombie-run recovery delay,
//! `worker_leases` bloat) and has been reverted.
//!
//! The FF dual-door-deadlock root cause is tracked upstream at
//! <https://github.com/avifenesh/FlowFabric/issues/371>. Until FF ships
//! the fix, 180 s is the sweet spot: covers typical iteration
//! (LLM tail + human approval + tool exec) with headroom without
//! meaningfully degrading actual-crash recovery latency.
//!
//! # This test
//!
//! Build a `FabricConfig` via `from_env()` with no
//! `CAIRN_FABRIC_LEASE_TTL_MS` in the environment and assert the
//! default comes out at 180_000. This mirrors the assertion the
//! sibling unit test `default_config_from_env` makes inside
//! `crates/cairn-fabric/src/config.rs`, but from the consumer
//! (cairn-app) side — so if someone edits the default without also
//! grepping for F63, both tests fail and the owner is forced to
//! re-read the F63 rationale before they land.

use cairn_fabric::FabricConfig;

/// Mutex on the process env to keep this test safe in a
/// cargo-test-multi-threaded run. `from_env()` reads process-global
/// state, and other tests in this crate stage env vars too.
static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

#[test]
fn default_lease_ttl_ms_is_180_000() {
    let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());

    // Scrub the override so we measure the actual compiled-in default.
    // Also scrub the HMAC secret/kid so `from_env().validate()` doesn't
    // trip on a stray HMAC env leaking in from the parent shell / CI
    // environment / another test — the validator rejects non-hex or
    // wrong-length secrets and the kid/secret pairing rules, any of
    // which would fail this test for a reason unrelated to F63.
    let prior_ttl = std::env::var("CAIRN_FABRIC_LEASE_TTL_MS").ok();
    let prior_hmac_secret = std::env::var("CAIRN_FABRIC_WAITPOINT_HMAC_SECRET").ok();
    let prior_hmac_kid = std::env::var("CAIRN_FABRIC_WAITPOINT_HMAC_KID").ok();
    std::env::remove_var("CAIRN_FABRIC_LEASE_TTL_MS");
    std::env::remove_var("CAIRN_FABRIC_WAITPOINT_HMAC_SECRET");
    std::env::remove_var("CAIRN_FABRIC_WAITPOINT_HMAC_KID");

    let result = FabricConfig::from_env();

    // Restore whatever the parent harness had set before asserting, so
    // a panic on the assertion doesn't leak the scrubbed state into
    // sibling tests sharing this process.
    if let Some(v) = prior_ttl {
        std::env::set_var("CAIRN_FABRIC_LEASE_TTL_MS", v);
    }
    if let Some(v) = prior_hmac_secret {
        std::env::set_var("CAIRN_FABRIC_WAITPOINT_HMAC_SECRET", v);
    }
    if let Some(v) = prior_hmac_kid {
        std::env::set_var("CAIRN_FABRIC_WAITPOINT_HMAC_KID", v);
    }

    let config = result.expect("default config validates");
    assert_eq!(
        config.lease_ttl_ms, 180_000,
        "F63: default lease TTL must be 180_000 ms (3 min). \
         If you're intentionally changing it, update CHANGELOG.md, \
         docs/design/CAIRN-FABRIC-FINALIZED.md, and the field rustdoc \
         in crates/cairn-fabric/src/config.rs — and link the reason."
    );
}
