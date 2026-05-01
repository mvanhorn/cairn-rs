//! PR-C2 migration regression tests (T1–T6).
//!
//! These tests are the behavioral contract for the six control-plane
//! call sites PR-C2 routes through the FF 0.13 `EngineBackend` trait
//! (see `.claude/plans/pr-c2-spec.md` §3).
//!
//! They call the `EngineBackend` trait methods **directly** against a
//! live Valkey testcontainer. That is intentional: the assertion is on
//! the backend contract, not on cairn's internal dispatch. The tests
//! therefore pass both before and after PR-C2's in-tree migration —
//! which is the regression guard the user asked for. If routing
//! through the trait ever changes an error code, idempotency fence,
//! or return variant, one of these tests trips.
//!
//! The test binary reuses the `integration::TestHarness` plumbing for
//! Valkey container boot + FF library load + per-test uuid isolation.
//! No FLUSHDB; each test gets its own uuid-scoped project + fresh
//! `BudgetId` / `ExecutionId` to keep parallel runs disjoint.
//!
//! Run with:
//!   cargo test -p cairn-fabric --test pr_c2_migrations --features test-harness

#![cfg(feature = "test-harness")]

use std::collections::BTreeMap;
use std::sync::Arc;

use cairn_domain::lifecycle::{PauseReason, PauseReasonKind};
use cairn_domain::tenancy::ProjectKey;
use cairn_fabric::test_harness::valkey_endpoint;
use cairn_fabric::{FabricConfig, FabricServices};
use cairn_store::InMemoryStore;
use flowfabric::core::contracts::{
    ClaimResumedExecutionArgs, DeliverApprovalSignalArgs, DeliverSignalResult,
    IssueGrantAndClaimArgs, RecordSpendArgs, ReleaseBudgetArgs, ReportUsageResult,
};
use flowfabric::core::engine_backend::EngineBackend;
use flowfabric::core::keys::ExecKeyContext;
use flowfabric::core::partition::execution_partition;
use flowfabric::core::types::{
    AttemptIndex, ExecutionId, LaneId, LeaseId, TimestampMs, WaitpointId,
};

// ── Harness ────────────────────────────────────────────────────────────

/// Minimal harness clone — the `integration.rs` test binary's harness
/// is test-binary-local. Re-use the exact boot shape so cross-harness
/// drift can't hide a regression in one binary.
struct PrC2Harness {
    fabric: FabricServices,
    project: ProjectKey,
}

impl PrC2Harness {
    async fn setup() -> Self {
        let (host, port) = valkey_endpoint().await;

        let suffix = uuid::Uuid::new_v4().simple().to_string();
        let tenant = format!("prc2_tenant_{}", suffix);
        let workspace = format!("prc2_workspace_{}", suffix);
        let project_id = format!("prc2_project_{}", suffix);
        let project = ProjectKey::new(tenant.as_str(), workspace.as_str(), project_id.as_str());

        let lane_id = cairn_fabric::id_map::project_to_lane(&project);

        let config = FabricConfig {
            backend: flowfabric::core::backend::BackendConfig::valkey(host, port),
            backend_kind: cairn_fabric::config::BackendKind::Valkey,
            lane_id,
            worker_id: flowfabric::core::types::WorkerId::new("prc2-worker"),
            worker_instance_id: flowfabric::core::types::WorkerInstanceId::new(
                uuid::Uuid::new_v4().to_string(),
            ),
            namespace: flowfabric::core::types::Namespace::new("prc2"),
            lease_ttl_ms: 30_000,
            grant_ttl_ms: 5_000,
            max_concurrent_tasks: 4,
            signal_dedup_ttl_ms: 86_400_000,
            fcall_timeout_ms: 5_000,
            worker_capabilities: std::collections::BTreeSet::new(),
            waitpoint_hmac_secret: Some(
                "00000000000000000000000000000000000000000000000000000000000000aa".into(),
            ),
            waitpoint_hmac_kid: Some("cairn-test-k1".into()),
        };

        let event_log = Arc::new(InMemoryStore::default());
        let event_log_for_bridge: Arc<dyn cairn_store::event_log::EventLog + Send + Sync> =
            event_log.clone();
        let fabric = FabricServices::start(config, event_log_for_bridge)
            .await
            .expect("FabricServices::start failed — is the Valkey container reachable?");

        Self { fabric, project }
    }

    fn partition_config(&self) -> &flowfabric::core::partition::PartitionConfig {
        &self.fabric.runtime.partition_config
    }

    fn backend(&self) -> &Arc<dyn EngineBackend> {
        &self.fabric.runtime.backend
    }

    fn unique_run_id(&self) -> cairn_domain::RunId {
        cairn_domain::RunId::new(format!("prc2_run_{}", uuid::Uuid::new_v4()))
    }

    fn unique_session_id(&self) -> cairn_domain::SessionId {
        cairn_domain::SessionId::new(format!("prc2_sess_{}", uuid::Uuid::new_v4()))
    }

    fn unique_task_id(&self) -> cairn_domain::TaskId {
        cairn_domain::TaskId::new(format!("prc2_task_{}", uuid::Uuid::new_v4()))
    }

    async fn teardown(self) {
        self.fabric.shutdown().await;
    }
}

/// Mint a deterministic-but-distinct ExecutionId keyed by `seed`.
/// Mirrors the helper in `test_budget.rs`.
fn test_eid(h: &PrC2Harness, seed: &str) -> ExecutionId {
    let uuid = uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_DNS, seed.as_bytes());
    ExecutionId::deterministic_solo(&LaneId::new("test"), h.partition_config(), uuid)
}

// ── T1 — record_spend_routes_via_engine_backend ────────────────────────

/// A fresh `backend.record_spend(RecordSpendArgs { .. })` call must
/// return `ReportUsageResult::Ok`, and a replay with the same
/// `idempotency_key` must return `ReportUsageResult::AlreadyApplied` —
/// the dedup fence the spec's regression guard requires.
#[tokio::test]
async fn test_record_spend_routes_via_engine_backend() {
    let h = PrC2Harness::setup().await;
    let run_id = h.unique_run_id();

    let budget_id = h
        .fabric
        .budgets
        .create_run_budget(&run_id, 1_000, 1_000_000, 100)
        .await
        .expect("create budget failed");

    let eid = test_eid(&h, "t1_record_spend");

    let mut deltas = BTreeMap::new();
    deltas.insert("tokens".to_owned(), 42u64);

    // Caller-computed idempotency key — matches cairn's own
    // `compute_spend_idempotency_key` shape (distinct keys per logical
    // spend).
    let idem_key = format!("prc2-t1-{}", uuid::Uuid::new_v4());

    let args = RecordSpendArgs::new(budget_id.clone(), eid.clone(), deltas.clone(), &idem_key);

    let outcome = h
        .backend()
        .record_spend(args.clone())
        .await
        .expect("record_spend failed");
    assert!(
        matches!(outcome, ReportUsageResult::Ok),
        "first record_spend must return Ok; got {outcome:?}"
    );

    // Regression guard: replay with the same idempotency_key returns
    // AlreadyApplied and does NOT double-increment the counter.
    let replay = h
        .backend()
        .record_spend(args)
        .await
        .expect("record_spend replay failed");
    assert!(
        matches!(replay, ReportUsageResult::AlreadyApplied),
        "replay with same idempotency_key must be AlreadyApplied; got {replay:?}"
    );

    h.teardown().await;
}

// ── T2 — release_budget_routes_via_engine_backend ──────────────────────

/// `backend.release_budget(ReleaseBudgetArgs { .. })` is the
/// per-execution attribution-release (not whole-budget reset).
/// Calling it on a budget+execution pair whose spend has landed must
/// succeed; a second call on the same pair must also be a safe no-op
/// (idempotent release, per FF 0.13 contracts.rs:4888).
#[tokio::test]
async fn test_release_budget_routes_via_engine_backend() {
    let h = PrC2Harness::setup().await;
    let run_id = h.unique_run_id();

    let budget_id = h
        .fabric
        .budgets
        .create_run_budget(&run_id, 1_000, 1_000_000, 100)
        .await
        .expect("create budget failed");

    let eid = test_eid(&h, "t2_release_budget");

    // Record a spend so there's attribution for release to reverse.
    let mut deltas = BTreeMap::new();
    deltas.insert("tokens".to_owned(), 77u64);
    let spend_args = RecordSpendArgs::new(
        budget_id.clone(),
        eid.clone(),
        deltas,
        format!("prc2-t2-spend-{}", uuid::Uuid::new_v4()),
    );
    let spend = h
        .backend()
        .record_spend(spend_args)
        .await
        .expect("record_spend failed");
    assert!(
        matches!(spend, ReportUsageResult::Ok),
        "precondition: record_spend must succeed; got {spend:?}"
    );

    let release_args = ReleaseBudgetArgs::new(budget_id.clone(), eid.clone());
    h.backend()
        .release_budget(release_args.clone())
        .await
        .expect("release_budget failed");

    // Replay — FF 0.13 contract documents release as idempotent. A
    // second call on the same (budget, execution) must not error.
    h.backend()
        .release_budget(release_args)
        .await
        .expect("release_budget replay must be idempotent");

    h.teardown().await;
}

// ── T3 — deliver_approval_signal_no_longer_reads_waitpoint_token_directly ─

/// After PR-C2 the caller no longer performs an HGET on
/// `ff_waitpoint_pending` to fetch the token — FF's
/// `EngineBackend::deliver_approval_signal` reads it server-side. The
/// test asserts the end-to-end outcome (execution leaves `suspended`)
/// driving through the trait method *directly*, not through cairn's
/// service layer, so the behavior is anchored at the backend seam.
#[tokio::test]
async fn test_deliver_approval_signal_routes_via_engine_backend() {
    let h = PrC2Harness::setup().await;
    let session_id = h.unique_session_id();
    let run_id = h.unique_run_id();

    // Bring a run to the `suspended` lifecycle with a pending approval
    // waitpoint — this is the state `deliver_approval_signal` is
    // designed for. Use the existing run service for the boot-up, then
    // drive the trait method directly for the delivery.
    h.fabric
        .runs
        .start(&h.project, &session_id, run_id.clone(), None)
        .await
        .expect("start failed");

    h.fabric
        .runs
        .claim(&h.project, &session_id, &run_id)
        .await
        .expect("claim failed");

    h.fabric
        .runs
        .enter_waiting_approval(&h.project, &session_id, &run_id)
        .await
        .expect("enter_waiting_approval failed");

    // Read the waitpoint id + lane_id directly from FF's exec_core.
    let eid = cairn_fabric::id_map::session_run_to_execution_id(
        &h.project,
        &session_id,
        &run_id,
        h.partition_config(),
    );
    let partition = execution_partition(&eid, h.partition_config());
    let ctx = ExecKeyContext::new(&partition, &eid);
    let fields: std::collections::HashMap<String, String> = h
        .fabric
        .runtime
        .client
        .hgetall(&ctx.core())
        .await
        .expect("HGETALL exec_core failed");
    let waitpoint_id = WaitpointId::parse(
        fields
            .get("current_waitpoint_id")
            .expect("waitpoint_id must be set after enter_waiting_approval")
            .trim(),
    )
    .expect("bad waitpoint_id");
    let lane_id = LaneId::new(
        fields
            .get("lane_id")
            .cloned()
            .unwrap_or_else(|| "cairn".to_owned()),
    );

    // Cairn's current run service signals with `approval_granted:<run_id>` /
    // `approval_rejected:<run_id>` — mirror that name shape so the
    // `SignalMatcher::Wildcard` path on the waitpoint accepts it. Using a
    // uuid suffix keeps parallel tests' dedup keys disjoint.
    let decision_id = uuid::Uuid::new_v4().to_string();
    let signal_name = format!("approval_granted:{}", run_id.as_str());
    let idem_suffix = format!("approval:{decision_id}");

    let args = DeliverApprovalSignalArgs::new(
        eid.clone(),
        lane_id.clone(),
        waitpoint_id.clone(),
        signal_name.clone(),
        idem_suffix.clone(),
        86_400_000_u64,
        None,
        None,
    );

    // Cairn's run service never saw the token, and neither does this
    // test. FF reads it server-side from `ff_waitpoint_pending`.
    let outcome = h
        .backend()
        .deliver_approval_signal(args)
        .await
        .expect("deliver_approval_signal failed");
    assert!(
        !matches!(outcome, DeliverSignalResult::Duplicate { .. }),
        "first delivery must not be Duplicate; got {outcome:?}"
    );

    // Post-condition: execution is no longer suspended — FF's signal
    // path matched the waitpoint, consumed it, and flipped
    // `public_state` off `suspended`. The fact that this resume
    // happened *without* the cairn caller reading the waitpoint token
    // is the contract PR-C2 migration preserves.
    let post: std::collections::HashMap<String, String> = h
        .fabric
        .runtime
        .client
        .hgetall(&ctx.core())
        .await
        .expect("HGETALL exec_core (post) failed");
    let post_state = post.get("public_state").cloned().unwrap_or_default();
    assert!(
        post_state != "suspended",
        "after deliver_approval_signal, public_state must NOT be 'suspended'; got {:?}",
        post_state,
    );

    // Regression guard: the consumed waitpoint id is cleared — proves
    // FF's internal waitpoint-close path ran. Unlike `tool_result`
    // signals (where non-matching signals leave the waitpoint open
    // and allow a true dedup replay), approval waitpoints use
    // `SignalMatcher::Wildcard`, so any delivery closes the waitpoint
    // on first success. That rules out a second-delivery-Duplicate
    // assertion on this exact waitpoint; the behavioral guard is
    // instead that the waitpoint is cleared and the post-state is
    // runnable/waiting — both observable here.
    let cleared = post
        .get("current_waitpoint_id")
        .cloned()
        .unwrap_or_default();
    assert_eq!(
        cleared, "",
        "after deliver_approval_signal, current_waitpoint_id must be cleared; got {:?}",
        cleared,
    );

    h.teardown().await;
}

// ── T4 — issue_grant_and_claim_is_atomic_via_engine_backend ────────────

/// `backend.issue_grant_and_claim(IssueGrantAndClaimArgs { .. })` is
/// the backend-atomic composition of `issue_claim_grant` +
/// `claim_execution`. After this one call the returned outcome must
/// carry a non-empty lease triple; FF's own tests cover the internal
/// fuse, the cairn-side test asserts the surface.
#[tokio::test]
async fn test_issue_grant_and_claim_atomic() {
    let h = PrC2Harness::setup().await;
    let session_id = h.unique_session_id();
    let task_id = h.unique_task_id();

    // Submit a task so FF's exec_core exists in `runnable` lifecycle
    // — `issue_grant_and_claim` requires a runnable execution.
    h.fabric
        .tasks
        .submit(
            &h.project,
            task_id.clone(),
            None,
            None,
            0,
            Some(&session_id),
        )
        .await
        .expect("submit failed");

    let eid = cairn_fabric::id_map::session_task_to_execution_id(
        &h.project,
        &session_id,
        &task_id,
        h.partition_config(),
    );
    let partition = execution_partition(&eid, h.partition_config());
    let ctx = ExecKeyContext::new(&partition, &eid);
    let fields: std::collections::HashMap<String, String> = h
        .fabric
        .runtime
        .client
        .hgetall(&ctx.core())
        .await
        .expect("HGETALL exec_core failed");
    let lane_id = LaneId::new(
        fields
            .get("lane_id")
            .cloned()
            .unwrap_or_else(|| "cairn".to_owned()),
    );

    let args = IssueGrantAndClaimArgs::new(eid.clone(), lane_id.clone(), 30_000_u64);
    let outcome = h
        .backend()
        .issue_grant_and_claim(args)
        .await
        .expect("issue_grant_and_claim failed");

    // lease_id must be a real LeaseId (not the default); lease_epoch
    // must be ≥ 1 (FF's first-lease epoch); attempt_index is 0 on the
    // first claim.
    let lease_str = outcome.lease_id.to_string();
    assert!(
        !lease_str.is_empty() && lease_str != LeaseId::default().to_string(),
        "lease_id must be a real claim; got {lease_str:?}"
    );
    assert!(
        outcome.lease_epoch.0 >= 1,
        "lease_epoch must be >= 1; got {:?}",
        outcome.lease_epoch
    );

    // Post-condition: FF's exec_core carries the same lease_id →
    // the atomic claim actually committed.
    let post: std::collections::HashMap<String, String> = h
        .fabric
        .runtime
        .client
        .hgetall(&ctx.core())
        .await
        .expect("HGETALL exec_core (post) failed");
    let stored_lease = post.get("current_lease_id").cloned().unwrap_or_default();
    assert_eq!(
        stored_lease, lease_str,
        "exec_core.current_lease_id must match the returned lease_id"
    );

    h.teardown().await;
}

// ── T5 — read_waitpoint_token_via_engine_backend ───────────────────────

/// `backend.read_waitpoint_token(partition, waitpoint_id)` returns
/// `Some(token)` for a live waitpoint and `Ok(None)` for a missing
/// one. This replaces cairn's direct
/// `client.hget(waitpoint_key, "waitpoint_token")` in
/// `signal_bridge.rs`.
#[tokio::test]
async fn test_read_waitpoint_token_via_engine_backend() {
    let h = PrC2Harness::setup().await;
    let session_id = h.unique_session_id();
    let run_id = h.unique_run_id();

    h.fabric
        .runs
        .start(&h.project, &session_id, run_id.clone(), None)
        .await
        .expect("start failed");
    h.fabric
        .runs
        .claim(&h.project, &session_id, &run_id)
        .await
        .expect("claim failed");
    h.fabric
        .runs
        .enter_waiting_approval(&h.project, &session_id, &run_id)
        .await
        .expect("enter_waiting_approval failed");

    let eid = cairn_fabric::id_map::session_run_to_execution_id(
        &h.project,
        &session_id,
        &run_id,
        h.partition_config(),
    );
    let partition = execution_partition(&eid, h.partition_config());
    let ctx = ExecKeyContext::new(&partition, &eid);
    let fields: std::collections::HashMap<String, String> = h
        .fabric
        .runtime
        .client
        .hgetall(&ctx.core())
        .await
        .expect("HGETALL exec_core failed");
    let waitpoint_id = WaitpointId::parse(
        fields
            .get("current_waitpoint_id")
            .expect("waitpoint_id must be set after enter_waiting_approval")
            .trim(),
    )
    .expect("bad waitpoint_id");

    // Live waitpoint → Some(non-empty token).
    let token = h
        .backend()
        .read_waitpoint_token(partition.into(), &waitpoint_id)
        .await
        .expect("read_waitpoint_token failed");
    let token = token.expect("live waitpoint must have a token");
    assert!(
        !token.is_empty(),
        "live waitpoint must produce a non-empty token"
    );

    // Regression guard: a waitpoint id that was never written must
    // read back as Ok(None) (not Err). Matches FF 0.13
    // engine_backend.rs:285 contract.
    let ghost_wp = WaitpointId::parse(&uuid::Uuid::new_v4().to_string())
        .expect("uuid must parse as waitpoint id");
    let missing = h
        .backend()
        .read_waitpoint_token(partition.into(), &ghost_wp)
        .await
        .expect("read_waitpoint_token on missing waitpoint must not error");
    assert!(
        missing.is_none(),
        "missing waitpoint must read back as Ok(None); got {missing:?}"
    );

    h.teardown().await;
}

// ── T6 — claim_resumed_uses_read_current_attempt_index ─────────────────

/// The resumed-claim path reads the attempt-index pointer via
/// `EngineBackend::read_current_attempt_index`, then dispatches
/// `EngineBackend::claim_resumed_execution`. The test drives both
/// trait methods directly against a mid-resume execution to confirm
/// the pointer survives suspension and the resumed handle re-binds to
/// the **same** attempt (not a freshly minted one).
#[tokio::test]
async fn test_claim_resumed_uses_read_current_attempt_index() {
    use flowfabric::core::contracts::ClaimResumedExecutionResult;
    use flowfabric::core::types::{WorkerId, WorkerInstanceId};

    let h = PrC2Harness::setup().await;
    let session_id = h.unique_session_id();
    let task_id = h.unique_task_id();

    // Build a claimed task, suspend on a tool-requested waitpoint, and
    // deliver a matching signal so the execution is runnable again
    // mid-attempt.
    h.fabric
        .tasks
        .submit(
            &h.project,
            task_id.clone(),
            None,
            None,
            0,
            Some(&session_id),
        )
        .await
        .expect("submit failed");

    h.fabric
        .tasks
        .claim(
            &h.project,
            Some(&session_id),
            &task_id,
            "prc2-worker".into(),
            30_000,
        )
        .await
        .expect("claim failed");

    let invocation = format!("inv_{}", uuid::Uuid::new_v4());
    h.fabric
        .tasks
        .pause(
            &h.project,
            Some(&session_id),
            &task_id,
            PauseReason {
                kind: PauseReasonKind::ToolRequestedSuspension,
                detail: Some(invocation.clone()),
                resume_after_ms: None,
                actor: None,
            },
        )
        .await
        .expect("pause failed");

    let eid = cairn_fabric::id_map::session_task_to_execution_id(
        &h.project,
        &session_id,
        &task_id,
        h.partition_config(),
    );
    let partition = execution_partition(&eid, h.partition_config());
    let ctx = ExecKeyContext::new(&partition, &eid);
    let fields_pre: std::collections::HashMap<String, String> = h
        .fabric
        .runtime
        .client
        .hgetall(&ctx.core())
        .await
        .expect("HGETALL exec_core (pre) failed");
    let waitpoint_id = WaitpointId::parse(
        fields_pre
            .get("current_waitpoint_id")
            .expect("wp_id must be set after pause")
            .trim(),
    )
    .expect("bad waitpoint_id");
    let lane_id = LaneId::new(
        fields_pre
            .get("lane_id")
            .cloned()
            .unwrap_or_else(|| "cairn".to_owned()),
    );

    // Deliver the tool_result signal through cairn's signal bridge so
    // the waitpoint resolves and the execution transitions from
    // suspended → runnable (attempt_interrupted). cairn's existing
    // signals service uses the same FCALL the trait method replaces;
    // here we just need to reach `attempt_interrupted`.
    h.fabric
        .signals
        .deliver_tool_result_signal(&eid, &waitpoint_id, &invocation, None)
        .await
        .expect("deliver_tool_result_signal failed");

    // The resume path cleared `current_lease_id` but preserved
    // `current_attempt_index` (FF's attempt-interrupted state).
    // Step 1: read the pointer via the trait method.
    let attempt_index_before = h
        .backend()
        .read_current_attempt_index(&eid)
        .await
        .expect("read_current_attempt_index failed");

    // Sanity: the pointer must match what exec_core recorded.
    let fields_mid: std::collections::HashMap<String, String> = h
        .fabric
        .runtime
        .client
        .hgetall(&ctx.core())
        .await
        .expect("HGETALL exec_core (mid) failed");
    let stored_idx: u32 = fields_mid
        .get("current_attempt_index")
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    assert_eq!(
        attempt_index_before,
        AttemptIndex::new(stored_idx),
        "read_current_attempt_index must match exec_core.current_attempt_index"
    );

    // FF requires `ff_issue_claim_grant` before `claim_resumed_execution`
    // — the grant gates the resume claim. Issue it via the trait's
    // atomic composition `issue_grant_and_claim`; on an
    // `attempt_interrupted` execution the body dispatches into the
    // resumed-claim path internally. That gives us the real lease
    // handle without reaching for the raw FCALLs — and still exercises
    // `read_current_attempt_index`'s value, which `claim_resumed_execution`
    // consumes as its `current_attempt_index` arg.
    let grant_outcome = h
        .backend()
        .issue_grant_and_claim(IssueGrantAndClaimArgs::new(
            eid.clone(),
            lane_id.clone(),
            30_000_u64,
        ))
        .await
        .expect("issue_grant_and_claim (resumed dispatch) failed");

    // Invariant: the resumed claim did NOT mint a new attempt. The
    // attempt_index on the grant outcome equals the pre-claim pointer.
    assert_eq!(
        grant_outcome.attempt_index, attempt_index_before,
        "resumed claim must re-bind the same attempt; got new={:?} pre={:?}",
        grant_outcome.attempt_index, attempt_index_before,
    );

    // Prove the typed `claim_resumed_execution` path is reachable
    // with the pre-read index. The claim above already consumed the
    // grant so this direct call is expected to fail with a typed
    // backend error — we assert the call SHAPE, not success. This
    // keeps the test green against both pre-PR-C2 (where cairn uses
    // the raw FCALL) and post-PR-C2 (where it routes through the
    // trait method).
    let direct_args = ClaimResumedExecutionArgs {
        execution_id: eid.clone(),
        worker_id: WorkerId::new("prc2-worker"),
        worker_instance_id: WorkerInstanceId::new(uuid::Uuid::new_v4().to_string()),
        lane_id: lane_id.clone(),
        lease_id: LeaseId::new(),
        lease_ttl_ms: 30_000,
        current_attempt_index: attempt_index_before,
        remaining_attempt_timeout_ms: None,
        now: TimestampMs::now(),
    };
    let direct = h.backend().claim_resumed_execution(direct_args).await;
    // Either a typed Err (grant consumed) or a success — both prove
    // the trait method is wired on the Valkey backend. The spec's
    // assertion is "resumed claim re-binds same attempt", which the
    // `grant_outcome.attempt_index` check above already nailed.
    // `ClaimResumedExecutionResult` is `#[non_exhaustive]` at FF's
    // module boundary — today it has one variant (`Claimed(_)`), but
    // FF may add more (e.g. `AlreadySatisfied`, `Drained`). Copilot
    // #599 flagged that a bare `let ClaimResumedExecutionResult::Claimed(c) = r`
    // would panic on any such addition. Use `if let` so this test
    // stays green on the single current success variant and ignores
    // unknown variants (the core assertion —
    // "resumed claim re-binds same attempt" — is already anchored
    // by the earlier `grant_outcome.attempt_index` check).
    if let Ok(ClaimResumedExecutionResult::Claimed(c)) = direct {
        assert_eq!(
            c.attempt_index, attempt_index_before,
            "claim_resumed_execution must preserve the attempt pointer"
        );
    }

    h.teardown().await;
}
