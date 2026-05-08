//! #743 Part B: opt-in boot-time waitpoint HMAC kid rotation via
//! `CAIRN_FABRIC_WAITPOINT_HMAC_BOOTSTRAP_KID_RESET`.
//!
//! Pre-fix Part A (#748): boot fails with the actionable error message
//! when Valkey carries a `current_kid` from a prior run that doesn't
//! match the env-supplied one. Operator workarounds: match the
//! persisted kid, FLUSHDB, or manual HDEL.
//!
//! Part B contract: when `waitpoint_hmac_bootstrap_kid_reset = true`
//! is set in `FabricConfig`, `FabricServices::start` rotates to the
//! supplied (kid, secret) BEFORE the seed step. Subsequent seed
//! observes `AlreadySeeded` and is a no-op.
//!
//! Test shape:
//!
//!   1. Boot with `SHARED_KID` (kid every other integration test
//!      uses) → succeeds, persisting `current_kid=SHARED_KID`.
//!   2. Boot with a unique KID_B + reset=false → fails with the Part A
//!      actionable error (regression guard for Part A).
//!   3. Boot with KID_B + reset=true → succeeds (rotation kicked in).
//!   4. Boot with KID_B + reset=false (rerun) → succeeds, proving the
//!      rotation persisted the new kid.
//!
//! ## Process isolation
//!
//! FF's waitpoint HMAC state is partition-scoped at the Valkey level
//! (`waitpoint_hmac_secrets:{p:N}` for each of 256 partitions),
//! NOT namespace-scoped. Within the parallel `tests/integration.rs`
//! suite, every test shares one Valkey container — and a test that
//! rotates `current_kid` mid-flight cannot run safely alongside
//! tests that seed under a different kid.
//!
//! This test lives in its OWN top-level `tests/<file>.rs` deliberately:
//! cargo runs each `tests/<file>` in a separate test binary process,
//! and `valkey_endpoint()`'s `OnceCell` is per-process — so this file
//! gets a dedicated Valkey container fully isolated from
//! `integration.rs`. No cross-test serialisation needed.
//!
//! Pre-fix the rotation step would fail (no in-process recovery
//! path); post-fix Steps 3 and 4 succeed. Step 2 also fails pre-Part A
//! (no actionable error message).

use std::collections::BTreeSet;
use std::sync::Arc;

use cairn_domain::tenancy::ProjectKey;
use cairn_fabric::test_harness::valkey_endpoint;
use cairn_fabric::{FabricConfig, FabricServices};
use cairn_store::InMemoryStore;

/// Shared kid + secret used by every other integration test in this
/// crate. Picking the same shared values means Step 1's seed observes
/// `AlreadySeeded` if any sibling test (running in this binary's
/// dedicated container — but in practice `valkey_endpoint()` reuses
/// across binaries when the testcontainer-reuse env is set) seeded
/// first.
const SHARED_KID: &str = "cairn-test-k1";
const SHARED_SECRET: &str = "00000000000000000000000000000000000000000000000000000000000000aa";
/// Unique-per-test kid for the rotation path. Stable so logs are
/// greppable.
const KID_B: &str = "test-743b-kid-b";
const SECRET_B: &str = "00000000000000000000000000000000000000000000000000000000000743bb";

fn build_config(
    host: &str,
    port: u16,
    namespace: &str,
    kid: &str,
    secret: &str,
    bootstrap_kid_reset: bool,
) -> FabricConfig {
    FabricConfig {
        backend: flowfabric::core::backend::BackendConfig::valkey(host, port),
        backend_kind: cairn_fabric::config::BackendKind::Valkey,
        lane_id: flowfabric::core::types::LaneId::new(format!("lane_{namespace}")),
        worker_id: flowfabric::core::types::WorkerId::new("worker-743b"),
        worker_instance_id: flowfabric::core::types::WorkerInstanceId::new(
            uuid::Uuid::new_v4().to_string(),
        ),
        namespace: flowfabric::core::types::Namespace::new(namespace),
        lease_ttl_ms: 30_000,
        grant_ttl_ms: 5_000,
        max_concurrent_tasks: 4,
        signal_dedup_ttl_ms: 86_400_000,
        fcall_timeout_ms: 5_000,
        worker_capabilities: BTreeSet::new(),
        waitpoint_hmac_secret: Some(secret.to_owned()),
        waitpoint_hmac_kid: Some(kid.to_owned()),
        waitpoint_hmac_bootstrap_kid_reset: bootstrap_kid_reset,
    }
}

async fn boot(config: FabricConfig) -> Result<FabricServices, cairn_fabric::FabricError> {
    let event_log = Arc::new(InMemoryStore::default());
    let event_log_shared: Arc<dyn cairn_store::event_log::EventLog + Send + Sync> = event_log;
    FabricServices::start(config, event_log_shared).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bootstrap_kid_reset_recovers_persisted_kid_mismatch() {
    // This binary owns its own Valkey container (separate process from
    // tests/integration.rs); no cross-test serialisation needed. See
    // module docstring.
    let (host, port) = valkey_endpoint().await;
    // Namespace is uuid-scoped, but note: FF's waitpoint HMAC state is
    // partition-scoped (NOT namespace-scoped) at the Valkey level —
    // see the module docstring. The namespace here just keeps any
    // OTHER cairn state this test creates (worker rows, lease entries)
    // from colliding with sibling tests on the shared Valkey
    // container.
    let namespace = format!("test_743b_{}", uuid::Uuid::new_v4().simple());
    let _project = ProjectKey::new("t", "w", "p");

    // ── Step 1: boot with the SHARED kid every other integration test
    //    uses. If a sibling test seeded first, this returns
    //    `AlreadySeeded`; if we got here first, this seeds. Either way
    //    the partition is now `current_kid=SHARED_KID`.
    let config_shared = build_config(&host, port, &namespace, SHARED_KID, SHARED_SECRET, false);
    let _services_shared = match boot(config_shared).await {
        Ok(s) => s,
        Err(e) => panic!(
            "boot with the shared test kid must succeed — sibling tests use the \
             same kid so Step 1 expects `AlreadySeeded` if they ran first, or \
             a fresh seed if not. Failure here means the shared-test-kid \
             contract is broken (someone else changed `cairn-test-k1` without \
             updating this test). err={e}"
        ),
    };

    // ── Step 2: re-boot with KID_B + reset=false MUST fail with the
    //    Part A actionable error mentioning BOTH kids AND the Part B
    //    opt-in. Locks in Part A's actionable-error contract so a
    //    regression in the seed-error reshaping can't slip past.
    let config_b_no_reset = build_config(&host, port, &namespace, KID_B, SECRET_B, false);
    let err = match boot(config_b_no_reset).await {
        Ok(_) => panic!(
            "boot with mismatched KID_B and reset=false MUST fail — Part A regression: \
             the seed step must reject persisted current_kid mismatches without the \
             opt-in rotation."
        ),
        Err(e) => e,
    };
    let err_msg = err.to_string();
    assert!(
        err_msg.contains(SHARED_KID) && err_msg.contains(KID_B),
        "Part A error message must mention both the persisted kid and the supplied \
         kid so operators know what to do. Got: {err_msg}",
    );
    assert!(
        err_msg.contains("CAIRN_FABRIC_WAITPOINT_HMAC_BOOTSTRAP_KID_RESET"),
        "Part A error message must point at the Part B opt-in. Got: {err_msg}",
    );

    // ── Step 3: re-boot with KID_B + reset=TRUE succeeds. The
    //    rotation flips the persisted current_kid from SHARED_KID to
    //    KID_B before the seed step runs.
    let config_b_with_reset = build_config(&host, port, &namespace, KID_B, SECRET_B, true);
    let _services_b = match boot(config_b_with_reset).await {
        Ok(s) => s,
        Err(e) => panic!(
            "#743 Part B regression: boot with reset=true MUST succeed even when the \
             persisted current_kid differs from the supplied kid — that's the \
             in-process recovery path Part B is meant to provide. err={e}"
        ),
    };

    // ── Step 4: re-boot WITHOUT reset, supplying KID_B again. This
    //    must succeed — proving the Part B rotation actually persisted
    //    the new kid in Valkey (rather than just matching at boot-
    //    time and immediately falling back). If the rotation didn't
    //    persist, this boot would fail the way Step 2 did.
    let config_b_recheck = build_config(&host, port, &namespace, KID_B, SECRET_B, false);
    let _services_b_recheck = match boot(config_b_recheck).await {
        Ok(s) => s,
        Err(e) => panic!(
            "#743 Part B durability: after rotation, the persisted current_kid must \
             match KID_B so a subsequent boot without reset succeeds. If this fails \
             the rotation didn't actually write back — check valkey_control_plane_impl \
             and the FF rotate FCALL. err={e}"
        ),
    };

    // No Step-5 cleanup needed: this test binary has its own dedicated
    // Valkey container (per-process testcontainers OnceCell), which is
    // torn down when the test process exits. The kid mutation we
    // performed is contained within this binary's lifetime.
}
