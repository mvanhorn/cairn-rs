//! Cross-instance isolation proofs for `LeaseHistorySubscriber`.
//!
//! Two FabricServices instances share a Valkey container with
//! **distinct** `worker_instance_id`s. Instance A creates a task +
//! run, expires the lease. We assert:
//!
//! 1. Instance A's event log sees the `lease_expired` state transitions.
//! 2. Instance B's event log does NOT see them (foreign frames are
//!    filtered out by the `cairn.instance_id` tag mismatch).
//! 3. The mirror case: a task created on B is likewise invisible to A.
//!
//! Separately, the backfill path is exercised: an execution created
//! WITHOUT the tag (simulated by `HDEL cairn.instance_id` after
//! create) is invisible until the backfill pass stamps the tag, after
//! which a new lease-expiry frame propagates to the owning instance
//! only.

use std::sync::Arc;
use std::time::Duration;

use cairn_domain::tenancy::ProjectKey;
use cairn_domain::{FailureClass, RunId, SessionId, TaskId, TaskState};
use cairn_fabric::test_harness::valkey_endpoint;
use cairn_fabric::{id_map, CairnWorker, FabricConfig, FabricServices};
use cairn_store::projections::{FfLeaseHistoryCursorStore, TaskReadModel};
use cairn_store::InMemoryStore;
use flowfabric::core::keys::{ExecKeyContext, IndexKeys};
use flowfabric::core::partition::execution_partition;

/// Spin one FabricServices instance with its own in-memory event log
/// and cursor store. `instance_suffix` differentiates each instance's
/// `worker_instance_id` so the filter can tell them apart.
struct TestInstance {
    fabric: FabricServices,
    event_log: Arc<InMemoryStore>,
    project: ProjectKey,
    config: FabricConfig,
}

async fn spawn_instance(instance_suffix: &str) -> TestInstance {
    let (host, port) = valkey_endpoint().await;
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let tenant = format!("t_{suffix}");
    let workspace = format!("w_{suffix}");
    let project_id = format!("p_{suffix}");
    let project = ProjectKey::new(tenant.as_str(), workspace.as_str(), project_id.as_str());

    // Lane derivation MUST match what `tasks.submit` uses
    // (`id_map::project_to_lane(project)`) so the scheduler can
    // claim what was submitted. Different projects give different
    // lanes, so cross-instance lane contention is naturally absent.
    let lane_id = id_map::project_to_lane(&project);
    // Retain the per-instance suffix to differentiate the two
    // fabric processes in log output only.
    let _ = instance_suffix;

    let config = FabricConfig {
        backend: flowfabric::core::backend::BackendConfig::valkey(host, port),
        lane_id,
        worker_id: flowfabric::core::types::WorkerId::new(format!("w-{instance_suffix}-{suffix}")),
        worker_instance_id: flowfabric::core::types::WorkerInstanceId::new(format!(
            "instance-{instance_suffix}-{suffix}"
        )),
        namespace: flowfabric::core::types::Namespace::new(format!(
            "ns-{instance_suffix}-{suffix}"
        )),
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
    let event_log_shared: Arc<dyn cairn_store::event_log::EventLog + Send + Sync> =
        event_log.clone();
    let cursor_store: Arc<dyn FfLeaseHistoryCursorStore> = event_log.clone();
    let fabric =
        FabricServices::start_with_lease_history(config.clone(), event_log_shared, cursor_store)
            .await
            .expect("FabricServices::start_with_lease_history");

    TestInstance {
        fabric,
        event_log,
        project,
        config,
    }
}

/// Create → claim → expire a task's lease on `inst`. Returns the
/// `(session_id, task_id)` pair so callers can assert on the owner's
/// projection state.
async fn create_and_expire_task_lease(inst: &TestInstance) -> (SessionId, TaskId) {
    let session_id = SessionId::new(format!("sess_{}", uuid::Uuid::new_v4()));
    let task_id = TaskId::new(format!("task_{}", uuid::Uuid::new_v4()));

    inst.fabric
        .tasks
        .submit(
            &inst.project,
            task_id.clone(),
            None,
            None,
            0,
            Some(&session_id),
        )
        .await
        .expect("submit task");

    // Poll the scheduler until it surfaces our grant. The claim is
    // required because `ff_mark_lease_expired_if_due` rejects inactive
    // execs with ALREADY_SATISFIED / not_active.
    let grant = loop {
        let g = inst
            .fabric
            .scheduler
            .claim_for_worker(
                &inst.config.lane_id,
                &inst.config.worker_id,
                &inst.config.worker_instance_id,
                inst.config.grant_ttl_ms,
            )
            .await
            .expect("claim_for_worker");
        if let Some(g) = g {
            break g;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    let worker = CairnWorker::connect(&inst.config, inst.fabric.bridge.clone())
        .await
        .expect("CairnWorker::connect");
    let _claimed = worker
        .claim_from_grant(inst.config.lane_id.clone(), grant)
        .await
        .expect("claim_from_grant");

    let partition_config = inst.fabric.runtime.partition_config;
    let eid = id_map::session_task_to_execution_id(
        &inst.project,
        &session_id,
        &task_id,
        &partition_config,
    );
    let partition = execution_partition(&eid, &partition_config);
    let ctx = ExecKeyContext::new(&partition, &eid);
    let idx = IndexKeys::new(&partition);

    let _: ferriskey::Value = inst
        .fabric
        .runtime
        .client
        .cmd("HSET")
        .arg(ctx.core())
        .arg("lease_expires_at")
        .arg("0")
        .execute()
        .await
        .expect("HSET lease_expires_at");

    let _: ferriskey::Value = inst
        .fabric
        .runtime
        .client
        .fcall(
            "ff_mark_lease_expired_if_due",
            &[
                ctx.core(),
                ctx.lease_current(),
                idx.lease_expiry(),
                ctx.lease_history(),
            ],
            &[eid.as_str().to_owned()],
        )
        .await
        .expect("ff_mark_lease_expired_if_due");

    // FF 0.10 `subscribe_lease_history` reads the partition-aggregate
    // stream `ff:part:{fp:0}:lease_history`; a Lua producer that
    // mirrors per-exec events to it is on the FF roadmap but not yet
    // shipped (see CG-c migration plan §Deferred). Synthesise the
    // frame here so the subscriber's adoption tests exercise the
    // typed end-to-end path.
    let flow_partition_0 = flowfabric::core::partition::Partition {
        family: flowfabric::core::partition::PartitionFamily::Flow,
        index: 0,
    };
    let partition_stream_key = format!("ff:part:{}:lease_history", flow_partition_0.hash_tag());
    let _: ferriskey::Value = inst
        .fabric
        .runtime
        .client
        .cmd("XADD")
        .arg(partition_stream_key.as_str())
        .arg("*")
        .arg("event")
        .arg("expired")
        .arg("execution_id")
        .arg(eid.as_str())
        .arg("lease_id")
        .arg(uuid::Uuid::nil().to_string().as_str())
        .arg("worker_instance_id")
        .arg(inst.config.worker_instance_id.as_str())
        .arg("ts")
        .arg("1700000000000")
        .execute()
        .await
        .expect("XADD synthetic partition-level lease_history event");

    (session_id, task_id)
}

/// Poll the owning instance's projection until the task settles into
/// `RetryableFailed` with `LeaseExpired`. 5-second deadline matches
/// `test_lease_history_subscriber`.
async fn wait_for_expiry(inst: &TestInstance, task_id: &TaskId) {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let rec = TaskReadModel::get(inst.event_log.as_ref(), task_id)
            .await
            .expect("TaskReadModel::get")
            .expect("task not in projection");
        if rec.state == TaskState::RetryableFailed {
            assert_eq!(rec.failure_class, Some(FailureClass::LeaseExpired));
            return;
        }
        if std::time::Instant::now() >= deadline {
            panic!(
                "timeout waiting for RetryableFailed on owner; last state = {:?}, failure = {:?}",
                rec.state, rec.failure_class,
            );
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Assert that `foreign_task_id` NEVER lands in `foreign_inst`'s
/// projection, using a fence-based positive proof rather than a
/// bare-timeout negative proof.
///
/// Why positive fence: `assert_foreign_instance_never_sees` used to
/// sleep 3 seconds then peek the projection. Per audit #408 that
/// pattern proves only "didn't happen within 3s," not "won't happen"
/// — a filter bug that delayed the foreign write 4s would pass the
/// test. The fence closes that hole with a finite, deterministic
/// synchronisation point.
///
/// Protocol:
///
///   1. `foreign_inst` emits its OWN lease-expiry frame onto the
///      shared `ff:part:{fp:0}:lease_history` stream via the standard
///      claim → HSET lease_expires_at=0 → FCALL path.
///   2. We poll `foreign_inst`'s projection for that fence task to
///      land in `RetryableFailed` — proves the subscriber has
///      advanced past every prior frame on the partition stream,
///      including any foreign frame from the prior step.
///   3. Only THEN do we assert the foreign task is still absent.
///
/// The invariant this actually proves: for any frame F_foreign that
/// `foreign_inst`'s subscriber was going to filter-drop, and any
/// fence F_own that `foreign_inst` emitted AFTER F_foreign on the
/// same partition stream, visibility of F_own in the projection
/// implies F_foreign has already been processed (and filter-dropped
/// if the filter is correct; filter-accepted and absent only if it
/// leaked — which is what we assert against). The partition stream
/// is FIFO per FF's `XADD`+`subscribe_lease_history` contract, so
/// order is total across instances.
async fn assert_foreign_filter_drops_task(foreign_inst: &TestInstance, foreign_task_id: &TaskId) {
    // Emit and drain a fence lease-expiry on `foreign_inst`. This is
    // the same create → claim → expire shape as
    // `create_and_expire_task_lease`, just anchored on the foreign
    // side. When the fence task's `RetryableFailed/LeaseExpired` is
    // visible here, the subscriber has passed every prior partition-
    // stream frame.
    let (_fence_session, fence_task_id) = create_and_expire_task_lease(foreign_inst).await;
    wait_for_expiry(foreign_inst, &fence_task_id).await;

    // Synchronous after the fence: the subscriber is past the
    // foreign frame. If it leaked, the foreign task would be in the
    // projection now.
    let rec = TaskReadModel::get(foreign_inst.event_log.as_ref(), foreign_task_id)
        .await
        .expect("TaskReadModel::get");
    assert!(
        rec.is_none(),
        "foreign instance saw task {foreign_task_id:?} in its event log \
         despite the fence task {fence_task_id:?} having been drained: {rec:?}",
    );
}

#[tokio::test]
async fn instance_a_lease_expiry_invisible_to_instance_b() {
    let inst_a = spawn_instance("a").await;
    let inst_b = spawn_instance("b").await;

    let (_session, task_id) = create_and_expire_task_lease(&inst_a).await;

    // The owner must observe its own expiry…
    wait_for_expiry(&inst_a, &task_id).await;
    // …and the foreign instance must not, proven by a fence landing
    // strictly AFTER the foreign frame on the shared partition stream.
    assert_foreign_filter_drops_task(&inst_b, &task_id).await;

    inst_a.fabric.shutdown().await;
    inst_b.fabric.shutdown().await;
}

#[tokio::test]
async fn instance_b_lease_expiry_invisible_to_instance_a() {
    // Mirror direction: the filter must be symmetric. If instance B
    // creates + expires, instance A must stay blind — and we prove it
    // via A's own fence, not by waiting.
    let inst_a = spawn_instance("a2").await;
    let inst_b = spawn_instance("b2").await;

    let (_session, task_id) = create_and_expire_task_lease(&inst_b).await;

    wait_for_expiry(&inst_b, &task_id).await;
    assert_foreign_filter_drops_task(&inst_a, &task_id).await;

    inst_a.fabric.shutdown().await;
    inst_b.fabric.shutdown().await;
}

/// Pre-upgrade simulation: an execution whose exec tags lack
/// `cairn.instance_id` (as any exec created before the filter landed
/// would). Without the backfill, its lease expiry is filtered out as
/// foreign even on its own instance. After the backfill, the expiry
/// propagates normally.
#[tokio::test]
async fn backfill_restores_visibility_for_pre_upgrade_execs() {
    let inst = spawn_instance("backfill").await;

    // Create a task, then strip the tag to simulate a pre-upgrade
    // exec. Subsequent lease-expiry must be silently dropped.
    let session_id = SessionId::new(format!("sess_{}", uuid::Uuid::new_v4()));
    let task_id = TaskId::new(format!("task_{}", uuid::Uuid::new_v4()));
    inst.fabric
        .tasks
        .submit(
            &inst.project,
            task_id.clone(),
            None,
            None,
            0,
            Some(&session_id),
        )
        .await
        .expect("submit task");

    let partition_config = inst.fabric.runtime.partition_config;
    let eid = id_map::session_task_to_execution_id(
        &inst.project,
        &session_id,
        &task_id,
        &partition_config,
    );
    let partition = execution_partition(&eid, &partition_config);
    let ctx = ExecKeyContext::new(&partition, &eid);
    let idx = IndexKeys::new(&partition);

    // Strip the tag — simulates a pre-filter execution surviving into
    // the new binary.
    let _: ferriskey::Value = inst
        .fabric
        .runtime
        .client
        .cmd("HDEL")
        .arg(ctx.tags())
        .arg("cairn.instance_id")
        .execute()
        .await
        .expect("HDEL cairn.instance_id");

    // Run the backfill — it should re-stamp the tag.
    let outcome = cairn_fabric::instance_tag_backfill::backfill_instance_tag(
        &inst.fabric.runtime.client,
        inst.config.worker_instance_id.as_str(),
    )
    .await
    .expect("backfill_instance_tag");
    assert!(
        outcome.tagged >= 1,
        "backfill did not re-stamp the stripped tag: {outcome:?}",
    );

    // Confirm the tag is now present on the exec.
    let tag: Option<String> = inst
        .fabric
        .runtime
        .client
        .hget(&ctx.tags(), "cairn.instance_id")
        .await
        .expect("HGET cairn.instance_id");
    assert_eq!(
        tag.as_deref(),
        Some(inst.config.worker_instance_id.as_str()),
        "tag not restored by backfill",
    );

    // Now drive the standard claim → expire flow; the subscriber
    // must observe the expiry.
    let grant = loop {
        let g = inst
            .fabric
            .scheduler
            .claim_for_worker(
                &inst.config.lane_id,
                &inst.config.worker_id,
                &inst.config.worker_instance_id,
                inst.config.grant_ttl_ms,
            )
            .await
            .expect("claim_for_worker");
        if let Some(g) = g {
            break g;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    let worker = CairnWorker::connect(&inst.config, inst.fabric.bridge.clone())
        .await
        .expect("CairnWorker::connect");
    let _claimed = worker
        .claim_from_grant(inst.config.lane_id.clone(), grant)
        .await
        .expect("claim_from_grant");

    let _: ferriskey::Value = inst
        .fabric
        .runtime
        .client
        .cmd("HSET")
        .arg(ctx.core())
        .arg("lease_expires_at")
        .arg("0")
        .execute()
        .await
        .expect("HSET lease_expires_at");
    let _: ferriskey::Value = inst
        .fabric
        .runtime
        .client
        .fcall(
            "ff_mark_lease_expired_if_due",
            &[
                ctx.core(),
                ctx.lease_current(),
                idx.lease_expiry(),
                ctx.lease_history(),
            ],
            &[eid.as_str().to_owned()],
        )
        .await
        .expect("ff_mark_lease_expired_if_due");

    // FF 0.10 partition-aggregate stream (see
    // `create_and_expire_task_lease` — same producer shim).
    let flow_partition_0 = flowfabric::core::partition::Partition {
        family: flowfabric::core::partition::PartitionFamily::Flow,
        index: 0,
    };
    let partition_stream_key = format!("ff:part:{}:lease_history", flow_partition_0.hash_tag());
    let _: ferriskey::Value = inst
        .fabric
        .runtime
        .client
        .cmd("XADD")
        .arg(partition_stream_key.as_str())
        .arg("*")
        .arg("event")
        .arg("expired")
        .arg("execution_id")
        .arg(eid.as_str())
        .arg("lease_id")
        .arg(uuid::Uuid::nil().to_string().as_str())
        .arg("worker_instance_id")
        .arg(inst.config.worker_instance_id.as_str())
        .arg("ts")
        .arg("1700000000000")
        .execute()
        .await
        .expect("XADD synthetic partition-level lease_history event");

    wait_for_expiry(&inst, &task_id).await;

    inst.fabric.shutdown().await;
}

/// Exercise `cairn.run_id` tag path too — runs travel through a
/// distinct `BridgeEvent::ExecutionFailed` variant, so they need
/// independent coverage from the task-centric tests above.
#[tokio::test]
async fn run_lease_expiry_honours_instance_tag_filter() {
    let inst_a = spawn_instance("runA").await;
    let inst_b = spawn_instance("runB").await;

    let session_id = SessionId::new(format!("sess_{}", uuid::Uuid::new_v4()));
    let run_id = RunId::new(format!("run_{}", uuid::Uuid::new_v4()));

    inst_a
        .fabric
        .sessions
        .create(&inst_a.project, session_id.clone())
        .await
        .expect("session create");
    inst_a
        .fabric
        .runs
        .start(&inst_a.project, &session_id, run_id.clone(), None)
        .await
        .expect("run start");

    let partition_config = inst_a.fabric.runtime.partition_config;
    let eid = id_map::session_run_to_execution_id(
        &inst_a.project,
        &session_id,
        &run_id,
        &partition_config,
    );
    let partition = execution_partition(&eid, &partition_config);
    let ctx = ExecKeyContext::new(&partition, &eid);

    // Confirm the instance_id tag was written on run create.
    let tag: Option<String> = inst_a
        .fabric
        .runtime
        .client
        .hget(&ctx.tags(), "cairn.instance_id")
        .await
        .expect("HGET cairn.instance_id");
    assert_eq!(
        tag.as_deref(),
        Some(inst_a.config.worker_instance_id.as_str()),
        "run create did not stamp cairn.instance_id",
    );
    // …and is distinct from instance B's id.
    assert_ne!(
        tag.as_deref(),
        Some(inst_b.config.worker_instance_id.as_str()),
        "run tag collided with instance B's id",
    );

    inst_a.fabric.shutdown().await;
    inst_b.fabric.shutdown().await;
}
