//! Integration tests for the cairn-side `ControlPlaneBackend::issue_reclaim_grant`
//! / `reclaim_execution` overrides (#710 PR-2).
//!
//! Scope: prove the cairn translation layer between
//! `cairn_fabric::engine::control_plane_types::*` and
//! `flowfabric::core::contracts::*` round-trips correctly across all
//! four `ReclaimExecutionOutcome` variants the recovery loop will hit
//! in production. FF's own `rfc024_reclaim.rs` test suite (in
//! `ff-backend-valkey/tests/`) already proves the underlying Lua
//! works — these tests prove cairn's typed surface forwards the
//! arguments and translates the variants without dropping fields or
//! mismatching enums.
//!
//! Setup follows FF's own pattern (`rfc024_reclaim.rs`): drive the raw
//! FCALL trio (`ff_create_execution` →
//! `ff_issue_claim_grant` → `ff_claim_execution` →
//! `ff_mark_lease_expired_if_due`) via the cairn FabricRuntime's
//! ferriskey client, then exercise the cairn trait method.
//!
//! Why the raw-FCALL setup vs cairn services: cairn's run/task
//! services don't expose a "force lease expiry" path — they're
//! production paths, not test fixtures. Driving the raw FCALLs in
//! the test mirrors what FF does for its own backend tests and
//! keeps the surface-under-test scoped to the trait override
//! rather than dragging the whole service layer in.

use cairn_fabric::engine::{
    IssueReclaimGrantInput, IssueReclaimGrantOutcome, ReclaimExecutionInput,
    ReclaimExecutionOutcome,
};
use ferriskey::Value;
use flowfabric::core::keys::{ExecKeyContext, IndexKeys};
use flowfabric::core::partition::execution_partition;
use flowfabric::core::types::{
    AttemptId, AttemptIndex, ExecutionId, FlowId, LaneId, LeaseId, WorkerId, WorkerInstanceId,
};

use crate::TestHarness;

const LANE: &str = "default";
const WORKER: &str = "cairn-control-plane";
const NS: &str = "cairn-reclaim-test";

fn fresh_eid(h: &TestHarness) -> ExecutionId {
    ExecutionId::for_flow(&FlowId::new(), h.partition_config())
}

fn worker_inst_for(eid: &ExecutionId) -> WorkerInstanceId {
    // Bind the synthetic worker instance to the test execution so
    // parallel tests on the shared Valkey don't collide on
    // `worker_leases` / `lease_current` keys.
    WorkerInstanceId::new(format!("cairn-cp-{eid}"))
}

async fn fcall_create_execution(h: &TestHarness, eid: &ExecutionId) {
    let partition = execution_partition(eid, h.partition_config());
    let ctx = ExecKeyContext::new(&partition, eid);
    let idx = IndexKeys::new(&partition);
    let lane_id = LaneId::new(LANE);

    let keys: Vec<String> = vec![
        ctx.core(),
        ctx.payload(),
        ctx.policy(),
        ctx.tags(),
        idx.lane_eligible(&lane_id),
        ctx.noop(),
        idx.execution_deadline(),
        idx.all_executions(),
    ];
    let args: Vec<String> = vec![
        eid.to_string(),
        NS.to_owned(),
        LANE.to_owned(),
        "standalone".to_owned(),
        "0".to_owned(),
        "cairn-pr2-test".to_owned(),
        "{}".to_owned(),
        String::new(),
        String::new(),
        String::new(),
        "{}".to_owned(),
        String::new(),
        "0".to_owned(),
    ];
    let key_refs: Vec<&str> = keys.iter().map(String::as_str).collect();
    let arg_refs: Vec<&str> = args.iter().map(String::as_str).collect();
    let _: Value = h
        .valkey_runtime()
        .client
        .fcall("ff_create_execution", &key_refs, &arg_refs)
        .await
        .expect("FCALL ff_create_execution");
}

async fn fcall_issue_claim_grant(h: &TestHarness, eid: &ExecutionId, worker_inst: &str) {
    let partition = execution_partition(eid, h.partition_config());
    let ctx = ExecKeyContext::new(&partition, eid);
    let idx = IndexKeys::new(&partition);
    let lane_id = LaneId::new(LANE);
    let keys: Vec<String> = vec![ctx.core(), ctx.claim_grant(), idx.lane_eligible(&lane_id)];
    let args: Vec<String> = vec![
        eid.to_string(),
        WORKER.to_owned(),
        worker_inst.to_owned(),
        LANE.to_owned(),
        String::new(),
        "5000".to_owned(),
        String::new(),
        String::new(),
        String::new(),
    ];
    let key_refs: Vec<&str> = keys.iter().map(String::as_str).collect();
    let arg_refs: Vec<&str> = args.iter().map(String::as_str).collect();
    let _: Value = h
        .valkey_runtime()
        .client
        .fcall("ff_issue_claim_grant", &key_refs, &arg_refs)
        .await
        .expect("FCALL ff_issue_claim_grant");
}

async fn fcall_claim_execution(
    h: &TestHarness,
    eid: &ExecutionId,
    lease_ttl_ms: u64,
    worker_inst: &str,
) {
    let partition = execution_partition(eid, h.partition_config());
    let ctx = ExecKeyContext::new(&partition, eid);
    let idx = IndexKeys::new(&partition);
    let lane_id = LaneId::new(LANE);
    let wid = WorkerInstanceId::new(worker_inst);
    let att_idx = AttemptIndex::new(0);

    let keys: Vec<String> = vec![
        ctx.core(),
        ctx.claim_grant(),
        idx.lane_eligible(&lane_id),
        idx.lease_expiry(),
        idx.worker_leases(&wid),
        ctx.attempt_hash(att_idx),
        ctx.attempt_usage(att_idx),
        ctx.attempt_policy(att_idx),
        ctx.attempts(),
        ctx.lease_current(),
        ctx.lease_history(),
        idx.lane_active(&lane_id),
        idx.attempt_timeout(),
        idx.execution_deadline(),
    ];
    let lease_id = uuid::Uuid::new_v4().to_string();
    let attempt_id = uuid::Uuid::new_v4().to_string();
    let args: Vec<String> = vec![
        eid.to_string(),
        WORKER.to_owned(),
        worker_inst.to_owned(),
        LANE.to_owned(),
        String::new(),
        lease_id,
        lease_ttl_ms.to_string(),
        (lease_ttl_ms * 2 / 3).to_string(),
        attempt_id,
        "{}".to_owned(),
        String::new(),
        String::new(),
    ];
    let key_refs: Vec<&str> = keys.iter().map(String::as_str).collect();
    let arg_refs: Vec<&str> = args.iter().map(String::as_str).collect();
    let _: Value = h
        .valkey_runtime()
        .client
        .fcall("ff_claim_execution", &key_refs, &arg_refs)
        .await
        .expect("FCALL ff_claim_execution");
}

async fn fcall_mark_lease_expired(h: &TestHarness, eid: &ExecutionId) {
    let partition = execution_partition(eid, h.partition_config());
    let ctx = ExecKeyContext::new(&partition, eid);
    let idx = IndexKeys::new(&partition);
    let keys: Vec<String> = vec![
        ctx.core(),
        ctx.lease_current(),
        idx.lease_expiry(),
        ctx.lease_history(),
    ];
    let args: Vec<String> = vec![eid.to_string()];
    let key_refs: Vec<&str> = keys.iter().map(String::as_str).collect();
    let arg_refs: Vec<&str> = args.iter().map(String::as_str).collect();
    let _: Value = h
        .valkey_runtime()
        .client
        .fcall("ff_mark_lease_expired_if_due", &key_refs, &arg_refs)
        .await
        .expect("FCALL ff_mark_lease_expired_if_due");
}

/// Drive an execution to `lease_expired_reclaimable`. Returns the
/// `ExecutionId` and the bound `WorkerInstanceId` so the caller can
/// reuse them on the cairn-trait call.
async fn setup_lease_expired_reclaimable(h: &TestHarness) -> (ExecutionId, WorkerInstanceId) {
    let eid = fresh_eid(h);
    let worker_inst = worker_inst_for(&eid);

    fcall_create_execution(h, &eid).await;
    fcall_issue_claim_grant(h, &eid, worker_inst.as_str()).await;
    // Short lease TTL (100 ms) — sleep past expiry, then ask FF to
    // mark it expired. No wall-clock racing on the test side: the
    // mark FCALL is what flips the state, not the sleep itself.
    fcall_claim_execution(h, &eid, 100, worker_inst.as_str()).await;
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    fcall_mark_lease_expired(h, &eid).await;
    (eid, worker_inst)
}

fn issue_input(eid: ExecutionId, worker_inst: WorkerInstanceId) -> IssueReclaimGrantInput {
    IssueReclaimGrantInput {
        execution_id: eid,
        lane_id: LaneId::new(LANE),
        worker_id: WorkerId::new(WORKER),
        worker_instance_id: worker_inst,
        grant_ttl_ms: 5_000,
        capability_hash: None,
    }
}

fn reclaim_input(
    eid: ExecutionId,
    worker_inst: WorkerInstanceId,
    grant_handle_carrier: &IssueReclaimGrantOutcome,
) -> ReclaimExecutionInput {
    let IssueReclaimGrantOutcome::Granted(grant) = grant_handle_carrier else {
        panic!("reclaim_input requires a Granted outcome to thread the handle");
    };
    ReclaimExecutionInput {
        grant: grant.clone(),
        execution_id: eid,
        lane_id: LaneId::new(LANE),
        worker_id: WorkerId::new(WORKER),
        worker_instance_id: worker_inst.clone(),
        old_worker_instance_id: worker_inst,
        attempt_id: AttemptId::new(),
        current_attempt_index: AttemptIndex::new(0),
        lease_id: LeaseId::new(),
        lease_ttl_ms: 30_000,
        attempt_policy_json: String::new(),
        max_reclaim_count: None,
        capability_hash: None,
    }
}

/// Maps to FF's `IssueReclaimGrantOutcome::Granted` →
/// `ReclaimExecutionOutcome::Claimed(ReclaimedHandle)`. Proves the
/// cairn impl forwards the synthetic worker identity, threads the
/// grant handle into `reclaim_execution`, and unwraps `Claimed` into
/// cairn's typed mirror.
#[tokio::test]
async fn cairn_trait_grant_then_reclaim_mints_fresh_attempt() {
    let h = TestHarness::setup().await;
    let (eid, worker_inst) = setup_lease_expired_reclaimable(&h).await;

    let granted = h
        .fabric
        .control_plane
        .issue_reclaim_grant(issue_input(eid.clone(), worker_inst.clone()))
        .await
        .expect("issue_reclaim_grant must succeed on lease_expired_reclaimable execution");

    assert!(
        matches!(granted, IssueReclaimGrantOutcome::Granted(_)),
        "expected Granted, got {granted:?}",
    );

    let claimed = h
        .fabric
        .control_plane
        .reclaim_execution(reclaim_input(eid.clone(), worker_inst, &granted))
        .await
        .expect("reclaim_execution must succeed on a granted handle");

    assert!(
        matches!(claimed, ReclaimExecutionOutcome::Claimed(_)),
        "expected Claimed(ReclaimedHandle), got {claimed:?}",
    );

    h.teardown().await;
}

/// Maps to FF's `ReclaimExecutionOutcome::GrantNotFound`.
///
/// PR-2's mirror added this fourth variant precisely because the
/// recovery loop will hit it in real outages — grant TTLs are
/// short by design (1-5 s) and any pause between issue and reclaim
/// risks expiry. This test proves cairn's translation layer
/// surfaces `GrantNotFound` cleanly rather than coercing into
/// another variant.
///
/// Setup: issue a real grant with a 1ms TTL, sleep 100ms, then
/// call `reclaim_execution`. The Valkey grant key is `EXPIRE`-d
/// out by then, so FF's Lua sees an absent grant and returns
/// `ScriptError::InvalidClaimGrant` → cairn's translation maps
/// to `GrantNotFound`.
#[tokio::test]
async fn cairn_trait_reclaim_with_expired_grant_returns_grant_not_found() {
    let h = TestHarness::setup().await;
    let (eid, worker_inst) = setup_lease_expired_reclaimable(&h).await;

    // Issue a grant with a 1ms TTL — guaranteed to be EXPIRE'd by
    // the time we call reclaim 100ms later.
    let granted = h
        .fabric
        .control_plane
        .issue_reclaim_grant(IssueReclaimGrantInput {
            execution_id: eid.clone(),
            lane_id: LaneId::new(LANE),
            worker_id: WorkerId::new(WORKER),
            worker_instance_id: worker_inst.clone(),
            grant_ttl_ms: 1,
            capability_hash: None,
        })
        .await
        .expect("issue_reclaim_grant on lease_expired_reclaimable must succeed");

    assert!(
        matches!(granted, IssueReclaimGrantOutcome::Granted(_)),
        "expected initial Granted before TTL elapse, got {granted:?}",
    );

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let outcome = h
        .fabric
        .control_plane
        .reclaim_execution(reclaim_input(eid, worker_inst, &granted))
        .await
        .expect("reclaim_execution must surface GrantNotFound, not error");

    assert!(
        matches!(outcome, ReclaimExecutionOutcome::GrantNotFound),
        "expected GrantNotFound for a TTL-elapsed grant, got {outcome:?}",
    );

    h.teardown().await;
}

/// Maps to FF's `IssueReclaimGrantOutcome::NotReclaimable`.
///
/// A fresh execution with no expired lease is NOT in
/// `lease_expired_reclaimable`. Cairn's recovery loop must treat
/// this as "deadlock cleared, retry the original FCALL" — this
/// test proves the variant lands in the cairn mirror cleanly.
#[tokio::test]
async fn cairn_trait_issue_grant_on_fresh_execution_returns_not_reclaimable() {
    let h = TestHarness::setup().await;
    let eid = fresh_eid(&h);
    let worker_inst = worker_inst_for(&eid);

    // Just create — no claim → no lease → state is `runnable` /
    // `eligible_now`, not `lease_expired_reclaimable`.
    fcall_create_execution(&h, &eid).await;

    let outcome = h
        .fabric
        .control_plane
        .issue_reclaim_grant(issue_input(eid, worker_inst))
        .await
        .expect("issue_reclaim_grant must surface NotReclaimable, not error");

    match outcome {
        IssueReclaimGrantOutcome::NotReclaimable { detail } => {
            assert!(
                !detail.is_empty(),
                "NotReclaimable must carry a non-empty detail for diagnostics; \
                 got empty string",
            );
        }
        other => panic!("expected NotReclaimable, got {other:?}"),
    }

    h.teardown().await;
}

/// Cross-validation: `reclaim_execution` MUST reject a grant whose
/// `execution_id` does not match the input's `execution_id`.
///
/// PR-2 review feedback (Gemini HIGH on the original commit): the
/// grant handle was carried into the impl but never inspected. The
/// type system already prevents callers from synthesising a grant
/// (the `inner` field is `pub(crate)`), but does not stop a caller
/// from threading a stale grant from a previous execution into a
/// new `reclaim_execution` call. This test pins the defense-in-depth
/// check that turns the otherwise-passive grant parameter into a
/// load-bearing safety net.
#[tokio::test]
async fn cairn_trait_reclaim_rejects_grant_execution_id_mismatch() {
    let h = TestHarness::setup().await;

    // Two distinct lease_expired_reclaimable executions on the same
    // harness — both are valid candidates for reclaim, but each has
    // its own grant.
    let (eid_a, worker_inst_a) = setup_lease_expired_reclaimable(&h).await;
    let (eid_b, worker_inst_b) = setup_lease_expired_reclaimable(&h).await;

    let granted_a = h
        .fabric
        .control_plane
        .issue_reclaim_grant(issue_input(eid_a.clone(), worker_inst_a.clone()))
        .await
        .expect("issue_reclaim_grant on execution A must succeed");
    assert!(
        matches!(granted_a, IssueReclaimGrantOutcome::Granted(_)),
        "expected Granted on execution A, got {granted_a:?}",
    );

    // Build a reclaim input that thrads execution A's grant but
    // points at execution B. Cairn must reject pre-FCALL — without
    // this guard, FF's Lua would lookup the grant via partition(B)'s
    // claim_grant key (a different key than the one execution A's
    // grant landed in) and return GrantNotFound, which is the wrong
    // error. The mismatch is a CALLER bug, not an FF state issue.
    let mismatched = ReclaimExecutionInput {
        grant: match &granted_a {
            IssueReclaimGrantOutcome::Granted(handle) => handle.clone(),
            _ => unreachable!("checked above"),
        },
        // ↓ wrong execution_id (B instead of A)
        execution_id: eid_b.clone(),
        lane_id: LaneId::new(LANE),
        worker_id: WorkerId::new(WORKER),
        worker_instance_id: worker_inst_b.clone(),
        old_worker_instance_id: worker_inst_b,
        attempt_id: AttemptId::new(),
        current_attempt_index: AttemptIndex::new(0),
        lease_id: LeaseId::new(),
        lease_ttl_ms: 30_000,
        attempt_policy_json: String::new(),
        max_reclaim_count: None,
        capability_hash: None,
    };

    let err = h
        .fabric
        .control_plane
        .reclaim_execution(mismatched)
        .await
        .expect_err(
            "reclaim_execution must reject a grant whose execution_id does not \
             match input.execution_id",
        );

    let msg = format!("{err}");
    assert!(
        msg.contains("does not match"),
        "expected validation error mentioning 'does not match'; got: {msg}",
    );

    h.teardown().await;
}
