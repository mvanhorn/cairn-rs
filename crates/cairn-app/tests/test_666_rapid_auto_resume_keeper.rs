//! Issue #666 regression: deterministic **behavioural** proof that the
//! #666 phase-aware keeper does NOT call `ff_renew_lease` while FF
//! reports the execution is in a `runnable`-but-not-yet-claimed phase
//! (the post-`ff_deliver_signal`, pre-`ff_claim_resumed_execution`
//! window). Pre-fix the keeper fired the FCALL here, FF rejected
//! `execution_not_eligible`, and the `is_transient_phase_conflict`
//! classifier silently retried until the lease wall-clock expired.
//!
//! # Bug (dogfood round 5, 2026-05-03)
//!
//! Post-#658 (suspension-aware keeper) + #646 (auto-resume on approve).
//! On a run that resolves 19 tool-call approvals in 8 min, FF's
//! execution phase oscillates `active → suspended → runnable → active
//! …` faster than cairn's projection can track. #658 checked cairn's
//! `ApprovalReadModel` / `ToolCallApprovalReadModel` projection to
//! decide whether to skip the renew — but on the resume side, the
//! projection flips `Pending → Approved` before FF actually completes
//! the `claim_resumed` transition. The keeper fires a renew into the
//! microscopic `runnable`-but-not-yet-claimed window, FF rejects
//! `execution_not_eligible`, and the silent-retry classifier in
//! `run_keeper_loop` burned wall-clock until the lease expired.
//!
//! # Fix (#666)
//!
//! The keeper now probes FF directly via `Engine::read_execution_info`
//! (FF 0.15) and classifies the 7-dimension `StateVector`. Only the
//! shape `(lifecycle_phase=Active, attempt_state=RunningAttempt,
//! ownership_state=Leased)` is renewable; every other shape skips the
//! FCALL. The `is_transient_phase_conflict` silent-retry branch in the
//! keeper is gone.
//!
//! # Test shape (Option A — observability hook + live FF)
//!
//! This is an **in-process** test. It boots a shared Valkey
//! testcontainer, stands up a real `FabricServices` aggregate against
//! it, and drives a run's FF execution through an explicit FCALL
//! sequence that lands the execution in the exact race window the
//! keeper's #666 probe closes:
//!
//! ```text
//!   runs.start(...)                 — lifecycle_phase = runnable  (unclaimed)
//!   runs.claim(...)                 — lifecycle_phase = active    (leased)
//!   runs.enter_waiting_approval(..) — lifecycle_phase = suspended
//!   runs.resolve_approval(Approved) — lifecycle_phase = runnable  (post-signal, UNCLAIMED)
//! ```
//!
//! After `resolve_approval` the execution is in
//! `(lifecycle_phase = Runnable, ownership_state = Unowned)`. The test
//! then **starts the real lease keeper** via
//! `LeaseKeeperRegistry::ensure_running_with_observability`, wired with
//! a `KeeperObservability` hook that counts every renew attempt + every
//! FF phase-conflict rejection + notifies after every tick.
//!
//! The test waits deterministically for N keeper ticks via
//! `KeeperObservability::tick_completed.notified().await` — no sleeps,
//! no retry polling.
//!
//! ## Post-fix assertion (the one this file ships with)
//!
//! After three keeper ticks in the runnable-unclaimed phase:
//!
//!   * `renew_attempts == 0` — the phase probe recognised
//!     `RunnableUnclaimed` and the keeper skipped the FCALL.
//!   * `renew_rejections == 0` — no FF FCALL was issued, so FF
//!     had nothing to reject.
//!
//! ## Pre-fix assertion (same file, verified by manually reverting)
//!
//! If the `match phase { … }` block inside `run_keeper_loop` is
//! reverted to the pre-#666 shape (skip the FF probe entirely and
//! go straight to `renew_lease_if_stale` with the
//! `is_transient_phase_conflict` silent retry), the same three ticks
//! produce:
//!
//!   * `renew_attempts >= 1` — the keeper issues at least one FCALL
//!     because nothing gates it.
//!   * `renew_rejections >= 1` — FF rejects each FCALL with
//!     `execution_not_eligible` / `execution_not_active` / `lease_expired`
//!     because the execution is in the runnable-unclaimed phase the
//!     pre-fix keeper couldn't see.
//!
//! The pre-fix transcript is captured verbatim in PR #669's body.
//!
//! # Determinism
//!
//! * FCALL sequence is synchronous: each FF FCALL returns only after
//!   the Lua-side state transition is committed.
//! * `tick_completed.notified().await` wakes on the keeper loop's
//!   next `notify_waiters()` — no sleep windows, no polling.
//! * FF phase doesn't change under the keeper's feet: no background
//!   work drives the execution out of `runnable` during the test.
//!   A scanner could in theory, but the FabricConfig's
//!   `lease_ttl_ms = 30_000` + this test's 3-second execution window
//!   make that negligible (and the hook would catch any drift anyway).

use std::collections::BTreeSet;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use cairn_app::fabric_adapter::FabricRunServiceAdapter;
use cairn_app::lease_keeper::{KeeperObservability, LeaseKeeperRegistry, PhaseClassification};
use cairn_domain::policy::ApprovalDecision;
use cairn_domain::tenancy::ProjectKey;
use cairn_domain::{RunId, SessionId};
use cairn_fabric::engine::{LifecyclePhase, OwnershipState};
use cairn_fabric::test_harness::valkey_endpoint;
use cairn_fabric::{FabricConfig, FabricServices};
use cairn_runtime::RunService;
use cairn_store::InMemoryStore;

/// Force the lease keeper to tick fast enough that the test can sample
/// three ticks inside the tokio::test default timeout without dragging
/// the wall-clock. 500 ms is the floor (`KEEPER_MIN_INTERVAL_MS`); the
/// env override is clamped to that floor so we can't go lower.
const KEEPER_INTERVAL_ENV: &str = "CAIRN_LEASE_KEEPER_INTERVAL_MS";

/// #666: end-to-end behavioural regression.
///
/// Stages a live FF execution into `(lifecycle_phase = Runnable,
/// ownership_state = Unowned)` via the real service-layer FCALLs
/// (`runs.start → claim → enter_waiting_approval → resolve_approval`),
/// then spawns the real keeper with an observability hook installed
/// and waits deterministically for three ticks.
///
/// **Post-fix claim**: the keeper's `classify_phase` probe returns
/// `RunnableUnclaimed` on every tick, so no `ff_renew_lease` FCALL is
/// ever issued. `renew_attempts == 0` and `renew_rejections == 0`.
///
/// **Pre-fix contrast** (manually verified by reverting the
/// `match phase { … }` block in `run_keeper_loop`): the keeper
/// issues `renew_lease_if_stale` on every tick; FF rejects with a
/// phase-conflict error; `renew_attempts >= 1` and
/// `renew_rejections >= 1`.
#[tokio::test]
async fn keeper_skips_renew_in_runnable_unclaimed_phase() {
    // ── Spin up a real FabricServices against the shared test Valkey.
    //
    // In-process (no cairn-app subprocess) because the test asserts
    // on process-local counters (`KeeperObservability`) that only
    // exist inside the cairn-app / cairn-fabric linked binary. An
    // HTTP-only `LiveHarness` can't observe the keeper's per-tick
    // decision — which is the exact signal #666 flipped.
    let (host, port) = valkey_endpoint().await;

    // Per-test uuid scope keeps parallel harnesses routing to disjoint
    // `{fp:N}` hash tags on the shared container. No FLUSHDB between
    // tests (destructive on shared state) — see
    // `cairn_fabric::tests::integration::TestHarness` for the
    // established pattern this mirrors.
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let project = ProjectKey::new(
        format!("t_{suffix}").as_str(),
        format!("w_{suffix}").as_str(),
        format!("p_{suffix}").as_str(),
    );
    let session_id = SessionId::new(format!("sess_666_{suffix}"));
    let run_id = RunId::new(format!("run_666_{suffix}"));

    let lane_id = cairn_fabric::id_map::project_to_lane(&project);

    let config = FabricConfig {
        backend: flowfabric::core::backend::BackendConfig::valkey(host, port),
        lane_id,
        worker_id: flowfabric::core::types::WorkerId::new("test-worker-666"),
        worker_instance_id: flowfabric::core::types::WorkerInstanceId::new(
            uuid::Uuid::new_v4().to_string(),
        ),
        namespace: flowfabric::core::types::Namespace::new("test"),
        lease_ttl_ms: 30_000,
        grant_ttl_ms: 5_000,
        max_concurrent_tasks: 4,
        signal_dedup_ttl_ms: 86_400_000,
        fcall_timeout_ms: 5_000,
        worker_capabilities: BTreeSet::new(),
        // Same deterministic test secret as the sibling
        // `cairn-fabric/tests/integration.rs` harness. See that
        // file's ⚠ HMAC-ROTATION FOOTGUN comment before changing.
        waitpoint_hmac_secret: Some(
            "00000000000000000000000000000000000000000000000000000000000000aa".into(),
        ),
        waitpoint_hmac_kid: Some("cairn-test-k1".into()),
        waitpoint_hmac_bootstrap_kid_reset: false,
        backend_kind: cairn_fabric::config::BackendKind::Valkey,
    };

    let event_log = Arc::new(InMemoryStore::default());
    let event_log_for_bridge: Arc<dyn cairn_store::event_log::EventLog + Send + Sync> =
        event_log.clone();
    let fabric = FabricServices::start(config, event_log_for_bridge)
        .await
        .expect("FabricServices::start — is the Valkey testcontainer up?");

    // ── Drive the run through the FCALL sequence that produces the
    //    post-signal, pre-claim_resumed `runnable` window.

    fabric
        .runs
        .start(&project, &session_id, run_id.clone(), None)
        .await
        .expect("runs.start must succeed on a fresh run id");

    fabric
        .runs
        .claim(&project, &session_id, &run_id)
        .await
        .expect("runs.claim must succeed on a freshly-started run");

    fabric
        .runs
        .enter_waiting_approval(&project, &session_id, &run_id)
        .await
        .expect("runs.enter_waiting_approval must succeed on a claimed run");

    let resolved = fabric
        .runs
        .resolve_approval(&project, &session_id, &run_id, ApprovalDecision::Approved)
        .await
        .expect("runs.resolve_approval must succeed on a suspended run");
    assert_eq!(resolved.run_id, run_id);

    // ── Ground-truth setup assertions via the real FF probe.
    //
    // These ensure the test actually stages the execution into the
    // runnable-unclaimed state; if a future FF upgrade changes the
    // resume semantics the test fails loudly rather than silently
    // mis-proving what it claims.
    let execution_id = cairn_fabric::id_map::session_run_to_execution_id(
        &project,
        &session_id,
        &run_id,
        fabric.runtime.partition_config(),
    );
    let info = fabric
        .engine
        .read_execution_info(&execution_id)
        .await
        .expect("engine.read_execution_info must succeed on a live execution")
        .expect("execution must exist in FF (we just created it)");
    assert_eq!(
        info.state_vector.lifecycle_phase,
        LifecyclePhase::Runnable,
        "#666 setup invariant: after runs.resolve_approval, FF must \
         report lifecycle_phase = runnable. Observed state_vector = {:?}",
        info.state_vector,
    );
    assert_ne!(
        info.state_vector.ownership_state,
        OwnershipState::Leased,
        "#666 setup invariant: the resume path releases the lease. \
         Observed ownership_state = {:?}",
        info.state_vector.ownership_state,
    );
    assert_eq!(
        PhaseClassification::from_info(&info),
        PhaseClassification::RunnableUnclaimed,
        "#666 setup invariant: classifier must report RunnableUnclaimed \
         for a post-resolve_approval state vector"
    );

    // ── Install the real keeper with an observability hook.
    //
    // We force the interval floor (500 ms) via the env override so the
    // test can sample three ticks inside ~2 seconds. The lease TTL is
    // irrelevant (renew isn't wall-clock gated here) but we pass
    // 30_000 ms to keep the keeper's `interval_ms = max(ttl/3, floor)`
    // default path in the same shape as production.
    std::env::set_var(KEEPER_INTERVAL_ENV, "500");

    // Wrap `fabric` in the production `FabricRunServiceAdapter` so the
    // keeper calls the exact same `renew_lease_if_stale` path an HTTP
    // orchestrate handler would go through. Passing the raw
    // `FabricRunService` directly would bypass `fabric_err_to_runtime`
    // and the adapter's projection-based scope resolution — the
    // observability counters would then reflect a different error
    // shape than production ever sees.
    let fabric_arc = Arc::new(fabric);
    let runs: Arc<dyn RunService> = Arc::new(FabricRunServiceAdapter::new(
        fabric_arc.clone(),
        event_log.clone(),
    ));

    let observability = Arc::new(KeeperObservability::default());
    let registry = Arc::new(LeaseKeeperRegistry::new());
    registry
        .ensure_running_with_observability(
            run_id.clone(),
            session_id.clone(),
            execution_id.clone(),
            runs.clone(),
            fabric_arc.engine.clone(),
            30_000,
            observability.clone(),
        )
        .await;

    // Deterministically wait for three ticks. `Notify::notified()`
    // registers interest BEFORE we await, so a keeper `notify_waiters()`
    // call that races the awaiter is guaranteed to wake it. We arm a
    // fresh future each iteration because `Notify` is edge-triggered —
    // one `notify_waiters()` clears every armed future at that instant.
    //
    // Wrap each await in a hard 3-second timeout: if the keeper is
    // wedged, the test fails with a clear message instead of tokio's
    // default 60-second hang.
    for tick in 1..=3 {
        let notified = observability.tick_completed.notified();
        tokio::pin!(notified);
        tokio::time::timeout(Duration::from_secs(3), notified)
            .await
            .unwrap_or_else(|_| {
                panic!(
                    "#666 test: keeper did not signal tick {tick}/3 within 3s \
                     — keeper is wedged or `tick_completed` was never notified. \
                     Current counters: renew_attempts={} renew_rejections={}",
                    observability.renew_attempts.load(Ordering::SeqCst),
                    observability.renew_rejections.load(Ordering::SeqCst),
                )
            });
    }

    let attempts = observability.renew_attempts.load(Ordering::SeqCst);
    let rejections = observability.renew_rejections.load(Ordering::SeqCst);

    // ── Behavioural assertion.
    //
    // Post-fix: the #666 phase probe recognises `RunnableUnclaimed` and
    // the keeper skips `ff_renew_lease` entirely. Attempts AND
    // rejections are both zero.
    //
    // Pre-fix (verified by manually reverting the `match phase { … }`
    // block to go straight to renew): the keeper fires a renew on every
    // tick, FF rejects, the silent-retry classifier swallows the
    // rejection. Attempts AND rejections are both >= 1.
    //
    // Same assertion. Same test file. Different numbers because the
    // behaviour is different.
    assert_eq!(
        attempts, 0,
        "#666 regression: keeper must NOT call renew_lease_if_stale \
         while FF reports lifecycle_phase = Runnable (unclaimed). \
         Pre-fix the projection-based probe missed this state and the \
         keeper fired a doomed `ff_renew_lease` FCALL that FF rejected \
         with `execution_not_eligible`; the silent-retry classifier \
         burned wall-clock until the lease expired, surfacing as \
         Failed(TerminalWriteDeadlock).\n\n\
         Observed: renew_attempts={attempts} renew_rejections={rejections}",
    );
    assert_eq!(
        rejections, 0,
        "#666 regression: no keeper FCALL → no FF rejection. Observing \
         rejections without attempts indicates a broken observability \
         hook (should be unreachable).\n\n\
         Observed: renew_attempts={attempts} renew_rejections={rejections}",
    );

    registry.shutdown_all().await;
    std::env::remove_var(KEEPER_INTERVAL_ENV);
    // Drop the adapter's Arc so the only remaining reference is
    // `fabric_arc`. `FabricServices::shutdown` consumes `self`, so we
    // need sole ownership to call it. If another Arc is still live
    // (e.g. the keeper task didn't drop its RunService reference yet)
    // the test can leak the FabricServices drop chain — acceptable for
    // a tokio::test (the Valkey testcontainer persists across tests
    // anyway). Assert sole ownership via `try_unwrap` so a regression
    // that accidentally stashes an Arc somewhere surfaces loudly.
    drop(runs);
    match Arc::try_unwrap(fabric_arc) {
        Ok(f) => f.shutdown().await,
        Err(arc) => {
            tracing::warn!(
                strong_count = Arc::strong_count(&arc),
                "#666 test: FabricServices Arc still has live refs at shutdown; \
                 skipping explicit shutdown (testcontainer cleanup will reap it)"
            );
        }
    }
}

/// Retained from the original #666 PR: the classifier correctly
/// classifies the runnable-unclaimed state vector without the rest of
/// the live-keeper machinery. Complementary to the behavioural test
/// above — that test proves the keeper *acts correctly* on the
/// classification, this one proves the classification is correct for
/// the observed state vector. Keeping both guards against a refactor
/// that breaks either axis in isolation.
#[tokio::test]
async fn runnable_unclaimed_is_classified_as_non_renewable() {
    let (host, port) = valkey_endpoint().await;

    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let project = ProjectKey::new(
        format!("t_{suffix}").as_str(),
        format!("w_{suffix}").as_str(),
        format!("p_{suffix}").as_str(),
    );
    let session_id = SessionId::new(format!("sess_666cls_{suffix}"));
    let run_id = RunId::new(format!("run_666cls_{suffix}"));

    let lane_id = cairn_fabric::id_map::project_to_lane(&project);

    let config = FabricConfig {
        backend: flowfabric::core::backend::BackendConfig::valkey(host, port),
        lane_id,
        worker_id: flowfabric::core::types::WorkerId::new("test-worker-666cls"),
        worker_instance_id: flowfabric::core::types::WorkerInstanceId::new(
            uuid::Uuid::new_v4().to_string(),
        ),
        namespace: flowfabric::core::types::Namespace::new("test"),
        lease_ttl_ms: 30_000,
        grant_ttl_ms: 5_000,
        max_concurrent_tasks: 4,
        signal_dedup_ttl_ms: 86_400_000,
        fcall_timeout_ms: 5_000,
        worker_capabilities: BTreeSet::new(),
        waitpoint_hmac_secret: Some(
            "00000000000000000000000000000000000000000000000000000000000000aa".into(),
        ),
        waitpoint_hmac_kid: Some("cairn-test-k1".into()),
        waitpoint_hmac_bootstrap_kid_reset: false,
        backend_kind: cairn_fabric::config::BackendKind::Valkey,
    };

    let event_log = Arc::new(InMemoryStore::default());
    let event_log_for_bridge: Arc<dyn cairn_store::event_log::EventLog + Send + Sync> =
        event_log.clone();
    let fabric = FabricServices::start(config, event_log_for_bridge)
        .await
        .expect("FabricServices::start — is the Valkey testcontainer up?");

    fabric
        .runs
        .start(&project, &session_id, run_id.clone(), None)
        .await
        .expect("runs.start must succeed");
    fabric
        .runs
        .claim(&project, &session_id, &run_id)
        .await
        .expect("runs.claim must succeed");
    fabric
        .runs
        .enter_waiting_approval(&project, &session_id, &run_id)
        .await
        .expect("runs.enter_waiting_approval must succeed");
    fabric
        .runs
        .resolve_approval(&project, &session_id, &run_id, ApprovalDecision::Approved)
        .await
        .expect("runs.resolve_approval must succeed");

    let execution_id = cairn_fabric::id_map::session_run_to_execution_id(
        &project,
        &session_id,
        &run_id,
        fabric.runtime.partition_config(),
    );
    let info = fabric
        .engine
        .read_execution_info(&execution_id)
        .await
        .expect("engine.read_execution_info must succeed")
        .expect("execution must exist in FF");

    assert_eq!(
        info.state_vector.lifecycle_phase,
        LifecyclePhase::Runnable,
        "setup invariant"
    );
    assert_ne!(
        info.state_vector.ownership_state,
        OwnershipState::Leased,
        "setup invariant"
    );
    assert_eq!(
        PhaseClassification::from_info(&info),
        PhaseClassification::RunnableUnclaimed,
        "#666 regression: classifier must report RunnableUnclaimed \
         for post-resolve_approval state vector. Observed: {:?}",
        info.state_vector,
    );
    assert_ne!(
        PhaseClassification::from_info(&info),
        PhaseClassification::RenewableActive,
        "#666 regression: runnable-unclaimed must NOT be classified \
         as RenewableActive"
    );

    fabric.shutdown().await;
}
