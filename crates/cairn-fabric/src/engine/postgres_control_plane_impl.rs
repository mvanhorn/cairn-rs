//! PostgreSQL-backed implementations of the cairn-side [`Engine`] +
//! [`ControlPlaneBackend`] traits.
//!
//! # Scope (FF 0.14)
//!
//! FF 0.14 closed the two upstream asks that held bucket-C open:
//!
//! - **FF#473** (worker-registry parity): 5 new `EngineBackend`
//!   trait methods (`register_worker`, `heartbeat_worker`,
//!   `mark_worker_dead`, `list_expired_leases`, `list_workers`),
//!   every in-tree backend (Valkey, Postgres, SQLite) ships a body.
//! - **FF#477** (`list_incoming_edges`): surfaced on `EngineBackend`
//!   with a Postgres-native body.
//!
//! Every cairn trait method here now routes through the FF 0.14
//! trait; bucket-C is closed. The file-local `mod conversions` owns
//! the per-method mappings between cairn's mirror types (the
//! structs under [`crate::engine::control_plane_types`]) and FF
//! 0.14's contracts types.
//!
//! # Why one struct, two impls
//!
//! Mirrors [`ValkeyEngine`](super::valkey_impl::ValkeyEngine): one
//! concrete type carries both the [`Engine`] (read + tag writes) and
//! [`ControlPlaneBackend`] (FCALL-shaped mutations) trait impls.
//! Callers hold a single `Arc<PostgresControlPlane>` and cast to
//! either trait object. If the struct ever grows unwieldy we'll
//! split, but at the current method count keeping them co-located
//! avoids a cross-file jump on every body while still letting the
//! two traits live in their own `impl` blocks.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use async_trait::async_trait;
use flowfabric::core::contracts::{
    ExecutionInfo, HeartbeatWorkerArgs, HeartbeatWorkerOutcome, ListExpiredLeasesArgs,
    ListWorkersArgs, MarkWorkerDeadArgs, RegisterWorkerArgs, RegisterWorkerOutcome,
};
use flowfabric::core::engine_backend::EngineBackend;
use flowfabric::core::types::{
    BudgetId, EdgeId, ExecutionId, FlowId, LaneId, Namespace, QuotaPolicyId, TimestampMs, WorkerId,
    WorkerInstanceId,
};

use crate::error::FabricError;

use super::control_plane::ControlPlaneBackend;
use super::control_plane_types::{
    AddExecutionToFlowInput, ApplyDependencyToChildInput, BudgetSpendOutcome, BudgetStatusSnapshot,
    CancelFlowInput, CancelRunInput, ClaimGrantOutcome, CompleteRunInput, CreateFlowInput,
    CreateRunExecutionInput, DeliverApprovalSignalInput, EligibilityResult, ExecutionCreated,
    ExpiredLease, FailExecutionOutcome, FailRunInput, FlowCancelOutcome, IssueGrantAndClaimInput,
    QuotaAdmission, RenewLeaseInput, ResumeRunInput, RotationOutcome, StageDependencyEdgeInput,
    StageDependencyOutcome, SubmitTaskInput, WorkerRegistration, WorkerSummary,
};
use super::snapshots::{EdgeSnapshot, ExecutionSnapshot, FlowSnapshot};
use super::Engine;

/// PostgreSQL-backed implementation of [`Engine`] + [`ControlPlaneBackend`].
///
/// Holds an `Arc<dyn EngineBackend>` produced by
/// [`ff_backend_postgres::PostgresBackend::connect`] and wired in
/// [`crate::postgres_boot::PostgresFabricRuntime::start`]. Every
/// in-scope (bucket A + B) method delegates to the FF trait; bucket
/// C methods still stub pending PR-C4b.
pub struct PostgresControlPlane {
    /// FF 0.13 typed backend handle. All trait-method bodies read
    /// this field. `pub` so a caller that holds a pre-built
    /// `Arc<dyn EngineBackend>` (e.g. tests that boot a backend from
    /// a shared pool) can construct the control-plane directly.
    pub backend: Arc<dyn EngineBackend + Send + Sync>,
}

impl PostgresControlPlane {
    /// Construct a control-plane from an already-built
    /// `Arc<dyn EngineBackend>`.
    /// [`crate::postgres_boot::PostgresFabricRuntime::start`] calls
    /// [`ff_backend_postgres::PostgresBackend::connect`] and passes
    /// the result here; tests may construct a backend directly from a
    /// shared sqlx pool via
    /// [`ff_backend_postgres::PostgresBackend::from_pool`].
    pub fn new(backend: Arc<dyn EngineBackend + Send + Sync>) -> Self {
        Self { backend }
    }
}

// ── Engine trait impl ─────────────────────────────────────────────────

#[async_trait]
impl Engine for PostgresControlPlane {
    async fn describe_execution(
        &self,
        id: &ExecutionId,
    ) -> Result<Option<ExecutionSnapshot>, FabricError> {
        // Bucket B — delegate + snapshot conversion. FF returns its
        // own `ExecutionSnapshot` shape (carries `flow_id`, typed
        // `PublicState`, richer `LeaseSummary`). We reshape into
        // cairn's snapshot so services never see the FF-specific
        // enum or the extra fields cairn doesn't consume yet.
        let got = self
            .backend
            .describe_execution(id)
            .await
            .map_err(|e| FabricError::Engine(Box::new(e)))?;
        Ok(got.map(conversions::ff_execution_snapshot_to_cairn))
    }

    async fn describe_flow(&self, id: &FlowId) -> Result<Option<FlowSnapshot>, FabricError> {
        // Bucket B — delegate + snapshot conversion. FF's flow
        // snapshot carries cancellation metadata + edge-group view
        // cairn doesn't surface; drop them on the conversion.
        let got = self
            .backend
            .describe_flow(id)
            .await
            .map_err(|e| FabricError::Engine(Box::new(e)))?;
        Ok(got.map(conversions::ff_flow_snapshot_to_cairn))
    }

    async fn describe_edge(
        &self,
        flow_id: &FlowId,
        edge_id: &EdgeId,
    ) -> Result<Option<EdgeSnapshot>, FabricError> {
        // Bucket B — delegate + snapshot conversion. FF's edge
        // snapshot carries `edge_state` / `satisfaction_condition` /
        // `created_by` fields; cairn reshapes `edge_state` into the
        // `EdgeState` enum and drops the other two.
        let got = self
            .backend
            .describe_edge(flow_id, edge_id)
            .await
            .map_err(|e| FabricError::Engine(Box::new(e)))?;
        Ok(got.map(conversions::ff_edge_snapshot_to_cairn))
    }

    async fn list_incoming_edges(
        &self,
        execution_id: &ExecutionId,
    ) -> Result<Vec<EdgeSnapshot>, FabricError> {
        // FF 0.14 closed FF#477 with two trait primitives rather than
        // a composite SDK-level method: `resolve_execution_flow_id`
        // (eid → flow_id pivot) + `list_edges(flow_id, direction)`.
        // ff-sdk's `list_incoming_edges` composes these two. On PG
        // both primitives have native bodies, so we replicate the
        // composition in-line.
        let flow_id = match self
            .backend
            .resolve_execution_flow_id(execution_id)
            .await
            .map_err(|e| FabricError::Engine(Box::new(e)))?
        {
            Some(fid) => fid,
            // Standalone execution (no flow) — no dependency edges
            // possible by construction. Matches the Valkey path's
            // empty-SMEMBERS result.
            None => return Ok(Vec::new()),
        };
        let edges = self
            .backend
            .list_edges(
                &flow_id,
                flowfabric::core::contracts::EdgeDirection::Incoming {
                    to_node: execution_id.clone(),
                },
            )
            .await
            .map_err(|e| FabricError::Engine(Box::new(e)))?;
        Ok(edges
            .into_iter()
            .map(conversions::ff_edge_snapshot_to_cairn)
            .collect())
    }

    async fn get_execution_tag(
        &self,
        id: &ExecutionId,
        key: &str,
    ) -> Result<Option<String>, FabricError> {
        // Bucket A — direct delegate. cairn's trait and FF's trait
        // share `(id, key) -> Option<String>`; no conversion.
        self.backend
            .get_execution_tag(id, key)
            .await
            .map_err(|e| FabricError::Engine(Box::new(e)))
    }

    async fn get_execution_lane_id(&self, id: &ExecutionId) -> Result<Option<LaneId>, FabricError> {
        // Bucket B — reuses `describe_execution` because FF's PG
        // backend doesn't expose a targeted `get_lane_id` primitive.
        // `lane_id` is stamped at create-time and never rewritten,
        // so the extra fields pulled by `describe_execution` are
        // benign overhead (Valkey is already a two-HGETALL surface;
        // PG's describe is a single row fetch either way).
        let got = self
            .backend
            .describe_execution(id)
            .await
            .map_err(|e| FabricError::Engine(Box::new(e)))?;
        Ok(got.map(|snap| snap.lane_id))
    }

    async fn set_execution_tag(
        &self,
        id: &ExecutionId,
        key: &str,
        value: &str,
    ) -> Result<(), FabricError> {
        // Bucket A — direct delegate. FF's backend owns the
        // `^[a-z][a-z0-9_]*\.` namespace validation via
        // `ff_core::engine_backend::validate_tag_key`, so cairn
        // doesn't re-check here — a malformed key surfaces as
        // `EngineError::Validation` and maps into
        // `FabricError::Engine`.
        self.backend
            .set_execution_tag(id, key, value)
            .await
            .map_err(|e| FabricError::Engine(Box::new(e)))
    }

    async fn set_flow_tag(&self, id: &FlowId, key: &str, value: &str) -> Result<(), FabricError> {
        // Bucket A — direct delegate. See `set_execution_tag` above
        // for the namespace-validation note.
        self.backend
            .set_flow_tag(id, key, value)
            .await
            .map_err(|e| FabricError::Engine(Box::new(e)))
    }

    async fn set_flow_tags(
        &self,
        id: &FlowId,
        tags: &BTreeMap<String, String>,
    ) -> Result<(), FabricError> {
        // Bucket B — per-key loop around FF's singular
        // `set_flow_tag` (the backend trait has no bulk primitive
        // today). Atomicity tradeoff: Valkey's impl is one variadic
        // HSET (all-or-nothing); PG's per-key loop is all-but-last
        // writes committing independently. Cairn's only bulk-use
        // site is session creation, which writes two keys
        // (`cairn.project` + `cairn.session_id`) in sequence and
        // tolerates the split because bridge-event emission is
        // keyed on the final write succeeding.
        if tags.is_empty() {
            return Ok(());
        }
        for (k, v) in tags {
            self.backend
                .set_flow_tag(id, k, v)
                .await
                .map_err(|e| FabricError::Engine(Box::new(e)))?;
        }
        Ok(())
    }

    async fn register_worker(
        &self,
        worker_id: &WorkerId,
        instance_id: &WorkerInstanceId,
        namespace: &Namespace,
        lanes: &BTreeSet<LaneId>,
        capabilities: &BTreeSet<String>,
        liveness_ttl_ms: u64,
    ) -> Result<WorkerRegistration, FabricError> {
        // FF 0.14 closed FF#473 — `register_worker` now ships on
        // `EngineBackend` with bodies for every in-tree backend.
        // PG uses `INSERT … ON CONFLICT DO UPDATE RETURNING (xmax=0)`
        // + a 30-s `ttl_sweep` scanner (no native PEXPIRE).
        let now = TimestampMs::now();
        let args = RegisterWorkerArgs::new(
            worker_id.clone(),
            instance_id.clone(),
            lanes.clone(),
            capabilities.clone(),
            liveness_ttl_ms,
            namespace.clone(),
            now,
        );
        let outcome = self
            .backend
            .register_worker(args)
            .await
            .map_err(|e| FabricError::Engine(Box::new(e)))?;
        // `RegisterWorkerOutcome` is `#[non_exhaustive]`; fail loud on a
        // future FF variant so the cairn mapping is audited rather than
        // silently dropped.
        match outcome {
            RegisterWorkerOutcome::Registered | RegisterWorkerOutcome::Refreshed => {
                Ok(WorkerRegistration {
                    worker_id: worker_id.clone(),
                    instance_id: instance_id.clone(),
                    capabilities: capabilities.iter().cloned().collect(),
                    registered_at_ms: now.0.max(0) as u64,
                })
            }
            other => Err(FabricError::Internal(format!(
                "unhandled RegisterWorkerOutcome variant (post-FF 0.14 addition): {other:?}"
            ))),
        }
    }

    async fn heartbeat_worker(
        &self,
        instance_id: &WorkerInstanceId,
        namespace: &Namespace,
    ) -> Result<(), FabricError> {
        let args =
            HeartbeatWorkerArgs::new(instance_id.clone(), namespace.clone(), TimestampMs::now());
        let outcome = self
            .backend
            .heartbeat_worker(args)
            .await
            .map_err(|e| FabricError::Engine(Box::new(e)))?;
        match outcome {
            HeartbeatWorkerOutcome::Refreshed { .. } => Ok(()),
            HeartbeatWorkerOutcome::NotRegistered => Err(FabricError::Validation {
                reason: format!(
                    "worker instance {instance_id} liveness key absent — re-register required"
                ),
            }),
            other => Err(FabricError::Internal(format!(
                "unhandled HeartbeatWorkerOutcome variant (post-FF 0.14 addition): {other:?}"
            ))),
        }
    }

    async fn mark_worker_dead(
        &self,
        instance_id: &WorkerInstanceId,
        namespace: &Namespace,
        reason: &str,
    ) -> Result<(), FabricError> {
        let args = MarkWorkerDeadArgs::new(
            instance_id.clone(),
            namespace.clone(),
            reason.to_owned(),
            TimestampMs::now(),
        );
        self.backend
            .mark_worker_dead(args)
            .await
            .map(|_| ())
            .map_err(|e| FabricError::Engine(Box::new(e)))
    }

    async fn list_workers(
        &self,
        namespace: Option<&Namespace>,
    ) -> Result<Vec<WorkerSummary>, FabricError> {
        let mut args = ListWorkersArgs::new();
        args.namespace = namespace.cloned();
        let result = self
            .backend
            .list_workers(args)
            .await
            .map_err(|e| FabricError::Engine(Box::new(e)))?;
        Ok(result
            .entries
            .into_iter()
            .map(|w| WorkerSummary {
                worker_id: w.worker_id,
                instance_id: w.worker_instance_id,
                namespace: w.namespace,
                lanes: w.lanes,
                capabilities: w.capabilities,
                last_heartbeat_ms: w.last_heartbeat_ms.0,
                liveness_ttl_ms: w.liveness_ttl_ms,
                registered_at_ms: w.registered_at_ms.0,
            })
            .collect())
    }

    async fn list_expired_leases(
        &self,
        now_ms: u64,
        limit: usize,
    ) -> Result<Vec<ExpiredLease>, FabricError> {
        // FF 0.14 closed FF#473 — `list_expired_leases` now ships on
        // `EngineBackend` for every in-tree backend. PG queries the
        // `ff_execution_lease_expiry` index ordered by
        // `(expires_at_ms ASC, execution_id ASC)`.
        let mut args = ListExpiredLeasesArgs::new(TimestampMs::from_millis(now_ms as i64));
        args.limit = Some(
            limit.min(flowfabric::core::contracts::LIST_EXPIRED_LEASES_MAX_LIMIT as usize) as u32,
        );
        let result = self
            .backend
            .list_expired_leases(args)
            .await
            .map_err(|e| FabricError::Engine(Box::new(e)))?;
        Ok(result
            .entries
            .into_iter()
            .map(|e| ExpiredLease {
                execution_id: e.execution_id,
                expires_at_ms: e.expires_at_ms.0.max(0) as u64,
            })
            .collect())
    }

    async fn read_execution_info(
        &self,
        id: &ExecutionId,
    ) -> Result<Option<ExecutionInfo>, FabricError> {
        // FF 0.15 ships `read_execution_info` on `EngineBackend` with
        // a concrete Postgres body (see `exec_core::read_execution_info_impl`).
        // We forward verbatim — the read lives server-side so cairn
        // never touches FF's `ff_exec_core` table layout (issue #666).
        self.backend
            .read_execution_info(id)
            .await
            .map_err(|e| FabricError::Engine(Box::new(e)))
    }
}

// ── ControlPlaneBackend trait impl ────────────────────────────────────

#[async_trait]
impl ControlPlaneBackend for PostgresControlPlane {
    // ── Budget ────────────────────────────────────────────────────────

    async fn create_budget(
        &self,
        scope_type: &str,
        scope_id: &str,
        dimensions: &[&str],
        hard_limits: &[u64],
        soft_limits: &[u64],
        reset_interval_ms: u64,
        enforcement_mode: &str,
    ) -> Result<BudgetId, FabricError> {
        // Bucket B — mirrors the Valkey impl's input-validation +
        // lane-parity check, then delegates to FF's typed
        // `create_budget`. Idempotent on budget-id via FF's
        // `AlreadySatisfied` variant (cairn mints a fresh id each
        // call, so that branch is effectively unreachable — but
        // accept it to keep the trait future-proof for caller-
        // supplied ids).
        if dimensions.len() != hard_limits.len() || dimensions.len() != soft_limits.len() {
            return Err(FabricError::Validation {
                reason: "dimensions, hard_limits, soft_limits must have equal length".to_owned(),
            });
        }
        let budget_id = BudgetId::new();
        let args = flowfabric::core::contracts::CreateBudgetArgs {
            budget_id: budget_id.clone(),
            scope_type: scope_type.to_owned(),
            scope_id: scope_id.to_owned(),
            enforcement_mode: enforcement_mode.to_owned(),
            on_hard_limit: "block".to_owned(),
            on_soft_limit: "log".to_owned(),
            reset_interval_ms,
            dimensions: dimensions.iter().map(|s| (*s).to_owned()).collect(),
            hard_limits: hard_limits.to_vec(),
            soft_limits: soft_limits.to_vec(),
            now: TimestampMs::now(),
        };
        let outcome = self
            .backend
            .create_budget(args)
            .await
            .map_err(|e| FabricError::Engine(Box::new(e)))?;
        // Both success variants return the same id we supplied.
        match outcome {
            flowfabric::core::contracts::CreateBudgetResult::Created { budget_id }
            | flowfabric::core::contracts::CreateBudgetResult::AlreadySatisfied { budget_id } => {
                Ok(budget_id)
            }
        }
    }

    async fn record_spend(
        &self,
        budget_id: &BudgetId,
        execution_id: &ExecutionId,
        dimension_deltas: &[(&str, u64)],
        idempotency_key: &str,
    ) -> Result<BudgetSpendOutcome, FabricError> {
        // Bucket B — mirrors PR-C2's Valkey impl shape: reject empty
        // + duplicate dims caller-side, then delegate through FF's
        // typed `record_spend` (cairn #454 Phase 3a).
        if dimension_deltas.is_empty() {
            return Err(FabricError::Validation {
                reason: "record_spend: at least one dimension_delta is required".to_owned(),
            });
        }
        let mut deltas: BTreeMap<String, u64> = BTreeMap::new();
        for (dim, delta) in dimension_deltas {
            if deltas.insert((*dim).to_owned(), *delta).is_some() {
                return Err(FabricError::Validation {
                    reason: format!(
                        "record_spend: duplicate dimension `{dim}` in dimension_deltas — \
                         caller must dedup upstream (additive semantics not implied)"
                    ),
                });
            }
        }
        let args = flowfabric::core::contracts::RecordSpendArgs::new(
            budget_id.clone(),
            execution_id.clone(),
            deltas,
            idempotency_key,
        );
        let outcome = self
            .backend
            .record_spend(args)
            .await
            .map_err(|e| FabricError::Engine(Box::new(e)))?;
        Ok(conversions::map_report_usage_result(outcome))
    }

    async fn release_budget(
        &self,
        budget_id: &BudgetId,
        execution_id: &ExecutionId,
    ) -> Result<(), FabricError> {
        // Bucket B — direct arg-struct delegate. cairn #454 Phase 3b
        // clarified this is a per-execution attribution release (not
        // a whole-budget flush).
        let args = flowfabric::core::contracts::ReleaseBudgetArgs::new(
            budget_id.clone(),
            execution_id.clone(),
        );
        self.backend
            .release_budget(args)
            .await
            .map_err(|e| FabricError::Engine(Box::new(e)))?;
        Ok(())
    }

    async fn get_budget_status(
        &self,
        budget_id: &BudgetId,
    ) -> Result<Option<BudgetStatusSnapshot>, FabricError> {
        // Bucket B — delegate + BudgetStatus conversion. FF's trait
        // returns `Result<BudgetStatus, EngineError>` (no `Option`);
        // a missing budget surfaces as `EngineError::NotFound`.
        // Cairn's mirror uses `Option<_>`, so map NotFound → None.
        use flowfabric::core::engine_error::EngineError;
        match self.backend.get_budget_status(budget_id).await {
            Ok(status) => Ok(Some(conversions::ff_budget_status_to_cairn(status))),
            Err(EngineError::NotFound { .. }) => Ok(None),
            Err(e) => Err(FabricError::Engine(Box::new(e))),
        }
    }

    // ── Quota ─────────────────────────────────────────────────────────

    async fn create_quota_policy(
        &self,
        _scope_type: &str,
        _scope_id: &str,
        window_seconds: u64,
        max_requests_per_window: u64,
        max_concurrent: u64,
    ) -> Result<QuotaPolicyId, FabricError> {
        // Bucket B — FF's `CreateQuotaPolicyArgs` doesn't carry
        // `scope_type` / `scope_id`; those live on the cairn service
        // layer (which pre-reads them off an alternative path when
        // it needs to surface policy scope). Documented in
        // `docs/design/ff-migration/pr-c4a-classification.md` as a
        // PG-vs-Valkey parity gap; operator dashboards that want
        // scope metadata on PG route through the service projection
        // rather than trait reads.
        let qid = QuotaPolicyId::new();
        let args = flowfabric::core::contracts::CreateQuotaPolicyArgs {
            quota_policy_id: qid.clone(),
            window_seconds,
            max_requests_per_window,
            max_concurrent,
            now: TimestampMs::now(),
        };
        let outcome = self
            .backend
            .create_quota_policy(args)
            .await
            .map_err(|e| FabricError::Engine(Box::new(e)))?;
        match outcome {
            flowfabric::core::contracts::CreateQuotaPolicyResult::Created { quota_policy_id }
            | flowfabric::core::contracts::CreateQuotaPolicyResult::AlreadySatisfied {
                quota_policy_id,
            } => Ok(quota_policy_id),
        }
    }

    async fn check_admission(
        &self,
        quota_policy_id: &QuotaPolicyId,
        execution_id: &ExecutionId,
        window_seconds: u64,
        rate_limit: u64,
        concurrency_cap: u64,
    ) -> Result<QuotaAdmission, FabricError> {
        // Bucket B — delegate to FF's typed `check_admission`.
        // Dimension defaults to `"default"` (matches Valkey impl).
        let args = flowfabric::core::contracts::CheckAdmissionArgs {
            execution_id: execution_id.clone(),
            now: TimestampMs::now(),
            window_seconds,
            rate_limit,
            concurrency_cap,
            jitter_ms: None,
        };
        let outcome = self
            .backend
            .check_admission(quota_policy_id, "default", args)
            .await
            .map_err(|e| FabricError::Engine(Box::new(e)))?;
        Ok(conversions::map_admission_result(outcome))
    }

    // ── Rotation ──────────────────────────────────────────────────────

    async fn rotate_waitpoint_hmac(
        &self,
        new_kid: &str,
        new_secret_hex: &str,
        grace_ms: u64,
    ) -> RotationOutcome {
        // Bucket B — delegate to FF's cluster-wide
        // `rotate_waitpoint_hmac_secret_all`. PG writes one global
        // row and returns a single-entry vec (partition=0); Valkey
        // fans out per partition. Either way we fold the entries
        // into cairn's `{rotated, noop, failed}` counter triple.
        let args = flowfabric::core::contracts::RotateWaitpointHmacSecretAllArgs::new(
            new_kid.to_owned(),
            new_secret_hex.to_owned(),
            grace_ms,
        );
        match self.backend.rotate_waitpoint_hmac_secret_all(args).await {
            Ok(result) => conversions::map_rotation_result(result, new_kid),
            Err(e) => {
                // Whole-call failure — cairn's mirror has no "all
                // partitions failed" variant; we surface it as a
                // single failed entry with the "transport_error"
                // classification hint so the HTTP layer's
                // rotation-failure dashboard still renders.
                tracing::warn!(error = %e, "postgres rotate_waitpoint_hmac_secret_all failed");
                RotationOutcome {
                    rotated: 0,
                    noop: 0,
                    failed: vec![super::control_plane_types::RotationFailure {
                        partition_index: 0,
                        code: None,
                        detail: "transport_error".to_owned(),
                    }],
                    new_kid: new_kid.to_owned(),
                }
            }
        }
    }

    // ── Run lifecycle ─────────────────────────────────────────────────

    async fn create_run_execution(
        &self,
        input: CreateRunExecutionInput,
    ) -> Result<ExecutionCreated, FabricError> {
        // Bucket B — delegate to FF's typed `create_execution`.
        // Builds the full `CreateExecutionArgs` from cairn's mirror:
        // - `execution_kind = "run"` (matches the cairn constant
        //   `EXECUTION_KIND_RUN`).
        // - `input_payload` empty; runs don't carry an ingress body.
        // - `priority = 0`; runs don't use the operator priority knob.
        // - `tags` forwarded verbatim.
        // - `policy` parsed from JSON (`Some(_)` on non-empty,
        //   `None` on empty); FF applies its backend default retry
        //   shape when `None`.
        let args = conversions::build_create_execution_args(
            &input.execution_id,
            input.namespace.clone(),
            input.lane_id.clone(),
            crate::constants::EXECUTION_KIND_RUN,
            0,
            &input.tags,
            &input.policy_json,
        )?;
        let outcome = self
            .backend
            .create_execution(args)
            .await
            .map_err(|e| FabricError::Engine(Box::new(e)))?;
        Ok(match outcome {
            flowfabric::core::contracts::CreateExecutionResult::Created { .. } => {
                ExecutionCreated {
                    newly_created: true,
                }
            }
            flowfabric::core::contracts::CreateExecutionResult::Duplicate { .. } => {
                ExecutionCreated {
                    newly_created: false,
                }
            }
        })
    }

    async fn complete_run_execution(&self, input: CompleteRunInput) -> Result<(), FabricError> {
        // Bucket B — delegate to FF's typed `complete_execution`.
        // Builds an optional `LeaseFence` from cairn's lease context
        // — an all-empty fence triple signals the operator-override
        // path (fence=None + source=OperatorOverride), which FF's
        // Lua checks before stale-lease validation.
        let fence = conversions::lease_fence_from_context(&input.lease)?;
        let source = conversions::cancel_source_from_str(&input.lease.source);
        let args = flowfabric::core::contracts::CompleteExecutionArgs {
            execution_id: input.execution_id.clone(),
            fence,
            attempt_index: input.lease.attempt_index,
            result_payload: None,
            result_encoding: None,
            source,
            now: TimestampMs::now(),
        };
        self.backend
            .complete_execution(args)
            .await
            .map_err(|e| FabricError::Engine(Box::new(e)))?;
        Ok(())
    }

    async fn fail_run_execution(
        &self,
        input: FailRunInput,
    ) -> Result<FailExecutionOutcome, FabricError> {
        // Bucket B — delegate to FF's typed `fail_execution`. Maps
        // `FailExecutionResult::{RetryScheduled, TerminalFailed}`
        // onto cairn's mirror variants (the `delay_until` +
        // `next_attempt_index` on `RetryScheduled` aren't surfaced
        // through cairn's trait today — services only need to
        // branch on retry-vs-terminal for bridge-event emission).
        let fence = conversions::lease_fence_from_context(&input.lease)?;
        let source = conversions::cancel_source_from_str(&input.lease.source);
        let args = flowfabric::core::contracts::FailExecutionArgs {
            execution_id: input.execution_id.clone(),
            fence,
            attempt_index: input.lease.attempt_index,
            failure_reason: input.reason.clone(),
            failure_category: input.category.clone(),
            retry_policy_json: input.retry_policy_json.clone(),
            next_attempt_policy_json: String::new(),
            source,
        };
        let outcome = self
            .backend
            .fail_execution(args)
            .await
            .map_err(|e| FabricError::Engine(Box::new(e)))?;
        Ok(match outcome {
            flowfabric::core::contracts::FailExecutionResult::RetryScheduled { .. } => {
                FailExecutionOutcome::RetryScheduled
            }
            flowfabric::core::contracts::FailExecutionResult::TerminalFailed => {
                FailExecutionOutcome::TerminalFailed
            }
        })
    }

    async fn cancel_run_execution(&self, input: CancelRunInput) -> Result<(), FabricError> {
        // Bucket B — delegate to FF's typed `cancel_execution`.
        // FF's `CancelExecutionArgs` splits the fence into three
        // optional fields + a separate `source`. The fence triple
        // and source are built from cairn's `ExecutionLeaseContext`
        // via the same all-empty-or-all-set invariant the complete
        // path enforces.
        let fence = conversions::lease_fence_from_context(&input.lease)?;
        let source = conversions::cancel_source_from_str(&input.lease.source);
        let (lease_id, lease_epoch, attempt_id) = match &fence {
            Some(f) => (
                Some(f.lease_id.clone()),
                Some(f.lease_epoch),
                Some(f.attempt_id.clone()),
            ),
            None => (None, None, None),
        };
        let args = flowfabric::core::contracts::CancelExecutionArgs {
            execution_id: input.execution_id.clone(),
            reason: crate::constants::CANCEL_SOURCE_OVERRIDE.to_owned(),
            source,
            lease_id,
            lease_epoch,
            attempt_id,
            now: TimestampMs::now(),
        };
        self.backend
            .cancel_execution(args)
            .await
            .map_err(|e| FabricError::Engine(Box::new(e)))?;
        Ok(())
    }

    async fn resume_run_execution(&self, input: ResumeRunInput) -> Result<(), FabricError> {
        // Bucket B — delegate to FF's typed `resume_execution`.
        // FF's args only need `execution_id` + `trigger_type` +
        // optional resume delay — the lane / waitpoint fields that
        // cairn's mirror carries are pre-read server-side by FF's
        // impl, matching the Valkey path's `build_resume_execution`
        // FCALL.
        let args = flowfabric::core::contracts::ResumeExecutionArgs {
            execution_id: input.execution_id.clone(),
            trigger_type: if input.resume_source.is_empty() {
                "signal".to_owned()
            } else {
                input.resume_source.clone()
            },
            resume_delay_ms: 0,
        };
        self.backend
            .resume_execution(args)
            .await
            .map_err(|e| FabricError::Engine(Box::new(e)))?;
        Ok(())
    }

    async fn deliver_approval_signal(
        &self,
        input: DeliverApprovalSignalInput,
    ) -> Result<(), FabricError> {
        // Bucket B — delegate to FF's typed
        // `deliver_approval_signal` (cairn #454 Phase 3). FF reads
        // the HMAC waitpoint token server-side; operator API never
        // sees the token bytes.
        let args = flowfabric::core::contracts::DeliverApprovalSignalArgs::new(
            input.execution_id,
            input.lane_id,
            input.waitpoint_id,
            input.signal_name,
            input.idempotency_suffix,
            input.signal_dedup_ttl_ms,
            Some(input.maxlen),
            Some(input.max_signals_per_execution),
        );
        self.backend
            .deliver_approval_signal(args)
            .await
            .map_err(|e| FabricError::Engine(Box::new(e)))?;
        Ok(())
    }

    // ── Session lifecycle ─────────────────────────────────────────────

    async fn create_flow(&self, input: CreateFlowInput) -> Result<(), FabricError> {
        // Bucket B — delegate to FF's typed `create_flow`.
        // Idempotent via FF's `AlreadySatisfied` variant; cairn
        // treats both variants as success (service layer decides
        // whether to emit the bridge event).
        let args = flowfabric::core::contracts::CreateFlowArgs {
            flow_id: input.flow_id,
            flow_kind: input.flow_kind,
            namespace: input.namespace,
            now: TimestampMs::now(),
        };
        self.backend
            .create_flow(args)
            .await
            .map_err(|e| FabricError::Engine(Box::new(e)))?;
        Ok(())
    }

    async fn cancel_flow(&self, input: CancelFlowInput) -> Result<FlowCancelOutcome, FabricError> {
        // Bucket B — delegate to FF's typed `cancel_flow_header`.
        // The full `cancel_flow` trait method carries a wait-vs-async
        // dispatch policy machinery cairn doesn't use here; the
        // `_header` variant returns only the atomic flow-state flip
        // + membership view, which is all cairn's archive path
        // needs. Matches the `FlowCancelOutcome::{Cancelled,
        // AlreadyTerminal}` mirror exactly.
        let args = flowfabric::core::contracts::CancelFlowArgs {
            flow_id: input.flow_id,
            reason: input.reason,
            cancellation_policy: input.cancel_mode,
            now: TimestampMs::now(),
        };
        let outcome = self
            .backend
            .cancel_flow_header(args)
            .await
            .map_err(|e| FabricError::Engine(Box::new(e)))?;
        Ok(match outcome {
            flowfabric::core::contracts::CancelFlowHeader::Cancelled { .. } => {
                FlowCancelOutcome::Cancelled
            }
            flowfabric::core::contracts::CancelFlowHeader::AlreadyTerminal { .. } => {
                FlowCancelOutcome::AlreadyTerminal
            }
            // `CancelFlowHeader` is `#[non_exhaustive]` — a future FF
            // variant must fail loud so we audit the mapping rather
            // than silently dropping the outcome.
            other => {
                return Err(FabricError::Internal(format!(
                    "unhandled CancelFlowHeader variant (post-FF 0.13 addition): {other:?}"
                )));
            }
        })
    }

    // ── Claim ─────────────────────────────────────────────────────────

    async fn issue_grant_and_claim(
        &self,
        input: IssueGrantAndClaimInput,
    ) -> Result<ClaimGrantOutcome, FabricError> {
        // Bucket B — mirror of PR-C2's Valkey shape. FF's PG backend
        // fuses `issue_claim_grant` + `claim_execution` in one sqlx
        // transaction (cairn #454 Phase 4c), so a mid-op crash
        // cannot leak a dangling grant. Return struct fields are
        // identical between the FF `ClaimGrantOutcome` and cairn's
        // mirror — one-line shape map.
        let args = flowfabric::core::contracts::IssueGrantAndClaimArgs::new(
            input.execution_id,
            input.lane_id,
            input.lease_duration_ms,
        );
        let outcome = self
            .backend
            .issue_grant_and_claim(args)
            .await
            .map_err(|e| FabricError::Engine(Box::new(e)))?;
        Ok(ClaimGrantOutcome {
            lease_id: outcome.lease_id,
            lease_epoch: outcome.lease_epoch,
            attempt_index: outcome.attempt_index,
        })
    }

    // ── Task lifecycle ────────────────────────────────────────────────

    async fn submit_task_execution(
        &self,
        input: SubmitTaskInput,
    ) -> Result<ExecutionCreated, FabricError> {
        // Bucket B — mirrors `create_run_execution` but routes
        // `EXECUTION_KIND_TASK` + the caller-supplied priority
        // through FF's `create_execution`. When `policy_json` is
        // empty the cairn historical default (`max_retries=2`,
        // exponential backoff) is applied caller-side so both Valkey
        // and PG see the same concrete policy.
        let policy_json = if input.policy_json.is_empty() {
            serde_json::json!({
                "max_retries": 2,
                "backoff": {
                    "type": "exponential",
                    "initial_delay_ms": 1000,
                    "max_delay_ms": 30000,
                    "multiplier": 2
                }
            })
            .to_string()
        } else {
            input.policy_json.clone()
        };
        let args = conversions::build_create_execution_args(
            &input.execution_id,
            input.namespace.clone(),
            input.lane_id.clone(),
            crate::constants::EXECUTION_KIND_TASK,
            input.priority as i32,
            &input.tags,
            &policy_json,
        )?;
        let outcome = self
            .backend
            .create_execution(args)
            .await
            .map_err(|e| FabricError::Engine(Box::new(e)))?;
        Ok(match outcome {
            flowfabric::core::contracts::CreateExecutionResult::Created { .. } => {
                ExecutionCreated {
                    newly_created: true,
                }
            }
            flowfabric::core::contracts::CreateExecutionResult::Duplicate { .. } => {
                ExecutionCreated {
                    newly_created: false,
                }
            }
        })
    }

    async fn add_execution_to_flow(
        &self,
        input: AddExecutionToFlowInput,
    ) -> Result<(), FabricError> {
        // Bucket B — two-step: idempotent `create_flow` (FF replies
        // `AlreadySatisfied` on duplicate, which the backend maps to
        // success), then `add_execution_to_flow`. Mirrors Valkey
        // impl's two-FCALL pattern.
        let create = flowfabric::core::contracts::CreateFlowArgs {
            flow_id: input.flow_id.clone(),
            flow_kind: input.flow_kind.clone(),
            namespace: input.namespace.clone(),
            now: TimestampMs::now(),
        };
        self.backend
            .create_flow(create)
            .await
            .map_err(|e| FabricError::Engine(Box::new(e)))?;
        let add = flowfabric::core::contracts::AddExecutionToFlowArgs {
            flow_id: input.flow_id,
            execution_id: input.execution_id,
            now: TimestampMs::now(),
        };
        self.backend
            .add_execution_to_flow(add)
            .await
            .map_err(|e| FabricError::Engine(Box::new(e)))?;
        Ok(())
    }

    async fn stage_dependency_edge(
        &self,
        input: StageDependencyEdgeInput,
    ) -> Result<StageDependencyOutcome, FabricError> {
        // Bucket B — delegate to FF's typed `stage_dependency_edge`.
        // FF's result enum only surfaces the `Staged` success
        // variant; typed reject paths (stale graph revision, cycle,
        // self-referencing, already-exists, …) come back as
        // `EngineError` variants. Map those into cairn's mirror
        // outcome enum so services can branch on the same set of
        // cases they branch on today against Valkey.
        let args = flowfabric::core::contracts::StageDependencyEdgeArgs {
            flow_id: input.flow_id.clone(),
            edge_id: input.edge_id.clone(),
            upstream_execution_id: input.upstream_execution_id,
            downstream_execution_id: input.downstream_execution_id,
            dependency_kind: input.dependency_kind,
            data_passing_ref: if input.data_passing_ref.is_empty() {
                None
            } else {
                Some(input.data_passing_ref)
            },
            expected_graph_revision: input.expected_graph_revision,
            now: TimestampMs::now(),
        };
        match self.backend.stage_dependency_edge(args).await {
            Ok(flowfabric::core::contracts::StageDependencyEdgeResult::Staged {
                new_graph_revision,
                ..
            }) => Ok(StageDependencyOutcome::Staged { new_graph_revision }),
            Err(e) => Ok(conversions::stage_dependency_err_to_outcome(e)?),
        }
    }

    async fn apply_dependency_to_child(
        &self,
        input: ApplyDependencyToChildInput,
    ) -> Result<(), FabricError> {
        // Bucket B — delegate to FF's typed
        // `apply_dependency_to_child`. Both success variants
        // (`Applied` + `AlreadyApplied`) map to `Ok(())`; cairn's
        // service path is idempotent, so the distinction doesn't
        // reach the bridge layer.
        let args = flowfabric::core::contracts::ApplyDependencyToChildArgs {
            flow_id: input.flow_id,
            edge_id: input.edge_id,
            downstream_execution_id: input.downstream_execution_id,
            upstream_execution_id: input.upstream_execution_id,
            graph_revision: input.graph_revision,
            dependency_kind: input.dependency_kind,
            data_passing_ref: if input.data_passing_ref.is_empty() {
                None
            } else {
                Some(input.data_passing_ref)
            },
            now: TimestampMs::now(),
        };
        self.backend
            .apply_dependency_to_child(args)
            .await
            .map_err(|e| FabricError::Engine(Box::new(e)))?;
        Ok(())
    }

    async fn evaluate_flow_eligibility(
        &self,
        execution_id: &ExecutionId,
    ) -> Result<EligibilityResult, FabricError> {
        // Bucket B — delegate to FF's typed
        // `evaluate_flow_eligibility`. FF returns a single-variant
        // enum `Status { status: String }`; parse the string on the
        // same axis cairn's Valkey impl does (`eligible` /
        // `blocked_by_dependencies` / other).
        let args = flowfabric::core::contracts::EvaluateFlowEligibilityArgs {
            execution_id: execution_id.clone(),
        };
        let outcome = self
            .backend
            .evaluate_flow_eligibility(args)
            .await
            .map_err(|e| FabricError::Engine(Box::new(e)))?;
        let flowfabric::core::contracts::EvaluateFlowEligibilityResult::Status { status } = outcome;
        Ok(match status.as_str() {
            "eligible" => EligibilityResult::Eligible,
            "blocked_by_dependencies" => EligibilityResult::BlockedByDependencies,
            other => EligibilityResult::Other(other.to_owned()),
        })
    }

    async fn renew_task_lease(&self, input: RenewLeaseInput) -> Result<(), FabricError> {
        // Bucket B — delegate to FF's typed `renew_lease`. FF's
        // renew has no operator-override path (matches Valkey); the
        // fence triple must be fully populated. `Some`-wrap the
        // conversion and surface the `fence_required` Lua error as
        // `FabricError::Engine` if the caller handed in an empty
        // triple.
        let fence = conversions::lease_fence_from_context(&input.lease)?;
        let args = flowfabric::core::contracts::RenewLeaseArgs {
            execution_id: input.execution_id.clone(),
            attempt_index: input.lease.attempt_index,
            fence,
            lease_ttl_ms: input.lease_extension_ms,
            // `DEFAULT_LEASE_HISTORY_GRACE_MS` is a compile-time
            // `&str` constant; an unparseable value would be a
            // cairn-source defect, not a runtime input. `.expect()`
            // fails loud on the first affected build rather than
            // silently falling back to a hard-coded `60_000` that
            // would mask the regression.
            lease_history_grace_ms: crate::constants::DEFAULT_LEASE_HISTORY_GRACE_MS
                .parse()
                .expect(
                    "DEFAULT_LEASE_HISTORY_GRACE_MS constant must parse as u64 (source defect)",
                ),
        };
        self.backend
            .renew_lease(args)
            .await
            .map_err(|e| FabricError::Engine(Box::new(e)))?;
        Ok(())
    }
}

// ─── Conversions (file-local) ──────────────────────────────────────────
//
// Per Q3 in the PR-C4a scope: conversion helpers stay file-local under
// `mod conversions` rather than escaping to a module `super::conversions`
// — they only serve the PG impl and don't need to be re-used by Valkey.

mod conversions {
    use flowfabric::core::contracts::{
        BudgetStatus as FfBudgetStatus, CheckAdmissionResult, CreateExecutionArgs,
        EdgeSnapshot as FfEdgeSnapshot, ExecutionSnapshot as FfExecutionSnapshot,
        FlowSnapshot as FfFlowSnapshot, ReportUsageResult, RotateWaitpointHmacSecretAllEntry,
        RotateWaitpointHmacSecretAllResult, RotateWaitpointHmacSecretOutcome,
    };
    use flowfabric::core::engine_error::EngineError;
    use flowfabric::core::partition::execution_partition;
    use flowfabric::core::types::{
        CancelSource, ExecutionId, LaneId, LeaseFence, Namespace, TimestampMs,
    };

    use crate::engine::control_plane_types::{
        BudgetSpendOutcome, BudgetStatusSnapshot, ExecutionLeaseContext, QuotaAdmission,
        RotationFailure, RotationOutcome, StageDependencyOutcome,
    };
    use crate::engine::snapshots::{
        AttemptSummary, EdgeSnapshot as CairnEdgeSnapshot, EdgeState,
        ExecutionSnapshot as CairnExecutionSnapshot, FlowSnapshot as CairnFlowSnapshot,
    };
    use crate::error::FabricError;

    // ── Simple enum mappers ─────────────────────────────────────────

    pub(super) fn map_report_usage_result(r: ReportUsageResult) -> BudgetSpendOutcome {
        match r {
            ReportUsageResult::Ok => BudgetSpendOutcome::Ok,
            ReportUsageResult::AlreadyApplied => BudgetSpendOutcome::AlreadyApplied,
            ReportUsageResult::SoftBreach {
                dimension,
                current_usage,
                soft_limit,
            } => BudgetSpendOutcome::SoftBreach {
                dimension,
                current_usage,
                soft_limit,
            },
            ReportUsageResult::HardBreach {
                dimension,
                current_usage,
                hard_limit,
            } => BudgetSpendOutcome::HardBreach {
                dimension,
                current_usage,
                hard_limit,
            },
            // `ReportUsageResult` is `#[non_exhaustive]` — a new FF
            // variant (e.g. RFC-015 quota-scoped breach) must fail
            // loud so we audit the mapping rather than silently
            // dropping the outcome.
            other => {
                panic!("unhandled ReportUsageResult variant (post-FF 0.13 addition): {other:?}")
            }
        }
    }

    pub(super) fn map_admission_result(r: CheckAdmissionResult) -> QuotaAdmission {
        match r {
            CheckAdmissionResult::Admitted => QuotaAdmission::Admitted,
            CheckAdmissionResult::AlreadyAdmitted => QuotaAdmission::AlreadyAdmitted,
            CheckAdmissionResult::RateExceeded { retry_after_ms } => {
                QuotaAdmission::RateExceeded { retry_after_ms }
            }
            CheckAdmissionResult::ConcurrencyExceeded => QuotaAdmission::ConcurrencyExceeded,
        }
    }

    // ── Budget status ───────────────────────────────────────────────

    pub(super) fn ff_budget_status_to_cairn(s: FfBudgetStatus) -> BudgetStatusSnapshot {
        BudgetStatusSnapshot {
            budget_id: s.budget_id,
            scope_type: s.scope_type,
            scope_id: s.scope_id,
            enforcement_mode: s.enforcement_mode,
            usage: s.usage,
            hard_limits: s.hard_limits,
            soft_limits: s.soft_limits,
            breach_count: s.breach_count,
            soft_breach_count: s.soft_breach_count,
        }
    }

    // ── Rotation fan-out ────────────────────────────────────────────

    pub(super) fn map_rotation_result(
        result: RotateWaitpointHmacSecretAllResult,
        new_kid: &str,
    ) -> RotationOutcome {
        let mut rotated = 0u16;
        let mut noop = 0u16;
        let mut failed: Vec<RotationFailure> = Vec::new();
        for entry in result.entries {
            let RotateWaitpointHmacSecretAllEntry {
                partition, result, ..
            } = entry;
            match result {
                Ok(RotateWaitpointHmacSecretOutcome::Rotated { .. }) => rotated += 1,
                Ok(RotateWaitpointHmacSecretOutcome::Noop { .. }) => noop += 1,
                Err(e) => {
                    tracing::debug!(partition, fabric_err = %e, "postgres rotation entry failed");
                    failed.push(RotationFailure {
                        partition_index: partition,
                        code: None,
                        detail: "lua_rejected".to_owned(),
                    });
                }
            }
        }
        RotationOutcome {
            rotated,
            noop,
            failed,
            new_kid: new_kid.to_owned(),
        }
    }

    // ── Snapshot conversions ───────────────────────────────────────

    pub(super) fn ff_execution_snapshot_to_cairn(
        ff: FfExecutionSnapshot,
    ) -> CairnExecutionSnapshot {
        // FF's `public_state` is a typed `PublicState` enum; cairn
        // stores the raw wire string so forward-compatible additions
        // don't get swallowed. `PublicState::to_wire_str()` returns
        // FF's canonical snake_case wire form.
        let public_state = public_state_wire_str(&ff.public_state);
        let current_attempt = ff.current_attempt.map(|a| AttemptSummary {
            id: a.attempt_id,
            index: a.attempt_index,
        });
        let current_lease_epoch = ff.current_lease.as_ref().map(|l| l.lease_epoch);
        CairnExecutionSnapshot {
            execution_id: ff.execution_id,
            lane_id: ff.lane_id,
            namespace: ff.namespace,
            public_state,
            blocking_reason: ff.blocking_reason,
            blocking_detail: ff.blocking_detail,
            current_attempt,
            current_lease: ff.current_lease,
            current_waitpoint: ff.current_waitpoint,
            created_at: ff.created_at,
            last_mutation_at: ff.last_mutation_at,
            total_attempt_count: ff.total_attempt_count,
            current_lease_epoch,
            tags: ff.tags,
        }
    }

    pub(super) fn ff_flow_snapshot_to_cairn(ff: FfFlowSnapshot) -> CairnFlowSnapshot {
        CairnFlowSnapshot {
            flow_id: ff.flow_id,
            kind: ff.flow_kind,
            namespace: ff.namespace,
            node_count: ff.node_count,
            edge_count: ff.edge_count,
            graph_revision: ff.graph_revision,
            public_flow_state: ff.public_flow_state,
            created_at: ff.created_at,
            last_mutation_at: ff.last_mutation_at,
            tags: ff.tags,
        }
    }

    pub(super) fn ff_edge_snapshot_to_cairn(ff: FfEdgeSnapshot) -> CairnEdgeSnapshot {
        // FF's `edge_state` is an opaque string (staging-time
        // literal, typically `"pending"`). Cairn's `EdgeState` enum
        // distinguishes `Unsatisfied` / `Satisfied` / `Impossible` /
        // `Unknown` — map `pending` → `Unsatisfied` to match the
        // Valkey parser and treat unknown values as `Unknown` for
        // forward-compat.
        let state = match ff.edge_state.as_str() {
            "unsatisfied" | "pending" => EdgeState::Unsatisfied,
            "satisfied" => EdgeState::Satisfied,
            "impossible" => EdgeState::Impossible,
            _ => EdgeState::Unknown,
        };
        CairnEdgeSnapshot {
            edge_id: ff.edge_id,
            flow_id: ff.flow_id,
            upstream_execution_id: ff.upstream_execution_id,
            downstream_execution_id: ff.downstream_execution_id,
            kind: ff.dependency_kind,
            data_passing_ref: ff.data_passing_ref,
            state,
            created_at: ff.created_at,
        }
    }

    // ── Execution-args builder (shared by run + task paths) ────────

    pub(super) fn build_create_execution_args(
        execution_id: &ExecutionId,
        namespace: Namespace,
        lane_id: LaneId,
        execution_kind: &str,
        priority: i32,
        tags: &std::collections::HashMap<String, String>,
        policy_json: &str,
    ) -> Result<CreateExecutionArgs, FabricError> {
        // FF's Postgres backend wants a pre-computed `partition_id`
        // on the args so the per-row partition slot is known before
        // the INSERT fires (matches Valkey's hash-tag derivation).
        let partition = execution_partition(
            execution_id,
            &flowfabric::core::partition::PartitionConfig::default(),
        );
        // A non-empty `policy_json` that cairn builds from a domain
        // object *must* parse — silently falling back to `None` on
        // parse failure would drop the caller-supplied retry shape
        // without indication. Propagate the parse error as a typed
        // validation failure so the service layer surfaces a real
        // error at the boundary rather than running with the
        // backend default.
        let policy = if policy_json.is_empty() {
            None
        } else {
            Some(
                serde_json::from_str::<flowfabric::core::policy::ExecutionPolicy>(policy_json)
                    .map_err(|e| FabricError::Validation {
                        reason: format!("policy_json failed to parse as ExecutionPolicy: {e}"),
                    })?,
            )
        };
        Ok(CreateExecutionArgs {
            execution_id: execution_id.clone(),
            namespace,
            lane_id,
            execution_kind: execution_kind.to_owned(),
            input_payload: Vec::new(),
            payload_encoding: None,
            priority,
            creator_identity: "cairn".to_owned(),
            idempotency_key: None,
            tags: tags.clone(),
            policy,
            delay_until: None,
            execution_deadline_at: None,
            partition_id: partition.index,
            now: TimestampMs::now(),
        })
    }

    // ── Lease fence + cancel source ────────────────────────────────

    /// Build an optional [`LeaseFence`] from cairn's lease context.
    /// The all-empty triple signals operator-override (FF accepts
    /// `fence=None` only when `source == OperatorOverride`); a
    /// fully-populated triple produces `Some(fence)`. Any partial
    /// triple is rejected — matches FF Lua's `partial_fence_triple`
    /// guard.
    pub(super) fn lease_fence_from_context(
        ctx: &ExecutionLeaseContext,
    ) -> Result<Option<LeaseFence>, FabricError> {
        let all_empty =
            ctx.lease_id.is_empty() && ctx.lease_epoch.is_empty() && ctx.attempt_id.is_empty();
        let all_set =
            !ctx.lease_id.is_empty() && !ctx.lease_epoch.is_empty() && !ctx.attempt_id.is_empty();
        if all_empty {
            return Ok(None);
        }
        if !all_set {
            return Err(FabricError::Validation {
                reason: "lease fence triple must be all-empty (operator override) or all-set"
                    .to_owned(),
            });
        }
        let lease_id = flowfabric::core::types::LeaseId::parse(&ctx.lease_id).map_err(|e| {
            FabricError::Internal(format!("parse lease_id {:?}: {e}", ctx.lease_id))
        })?;
        let lease_epoch = flowfabric::core::types::LeaseEpoch::new(
            ctx.lease_epoch
                .parse()
                .map_err(|e| FabricError::Internal(format!("parse lease_epoch: {e}")))?,
        );
        let attempt_id =
            flowfabric::core::types::AttemptId::parse(&ctx.attempt_id).map_err(|e| {
                FabricError::Internal(format!("parse attempt_id {:?}: {e}", ctx.attempt_id))
            })?;
        Ok(Some(LeaseFence {
            lease_id,
            lease_epoch,
            attempt_id,
        }))
    }

    pub(super) fn cancel_source_from_str(s: &str) -> CancelSource {
        // Empty source = "lease-holder" happy path; matches cairn's
        // Valkey services which set `source = ""` when the fence
        // triple is fully populated and `source = "operator_override"`
        // when the triple is empty.
        if s.is_empty() {
            CancelSource::LeaseHolder
        } else {
            use std::str::FromStr;
            CancelSource::from_str(s).unwrap_or(CancelSource::OperatorOverride)
        }
    }

    // ── stage_dependency_edge error → outcome mapping ─────────────

    /// FF's `stage_dependency_edge` returns typed reject reasons as
    /// `EngineError` variants. cairn's trait has a matching outcome
    /// enum with `StaleGraphRevision` / `Cycle` / `SelfReferencing`
    /// / `AlreadyExists` / `FlowNotFound` / `FlowAlreadyTerminal` /
    /// `ExecutionNotInFlow`. Map the known codes; propagate any
    /// other variant as a genuine `FabricError::Engine` so service
    /// code surfaces a real failure rather than a silently wrong
    /// outcome variant.
    pub(super) fn stage_dependency_err_to_outcome(
        err: EngineError,
    ) -> Result<StageDependencyOutcome, FabricError> {
        use flowfabric::core::engine_error::{ConflictKind, ContentionKind, StateKind};
        // Pattern-match on the typed variants first; fall back to
        // string inspection when FF ships a generic `Validation`.
        // `EngineError` variants are tuple-style for the nested
        // `*Kind` enums (`Contention(ContentionKind)`,
        // `Conflict(ConflictKind)`, `State(StateKind)`).
        Ok(match &err {
            EngineError::Contention(ContentionKind::StaleGraphRevision) => {
                StageDependencyOutcome::StaleGraphRevision
            }
            EngineError::Validation { detail, .. } if detail.contains("cycle_detected") => {
                StageDependencyOutcome::Cycle
            }
            EngineError::Validation { detail, .. } if detail.contains("self_referencing") => {
                StageDependencyOutcome::SelfReferencing
            }
            EngineError::Conflict(ConflictKind::DependencyAlreadyExists { .. }) => {
                StageDependencyOutcome::AlreadyExists
            }
            EngineError::NotFound { entity, .. } if *entity == "flow" => {
                StageDependencyOutcome::FlowNotFound
            }
            EngineError::State(StateKind::FlowAlreadyTerminal) => {
                StageDependencyOutcome::FlowAlreadyTerminal
            }
            EngineError::State(StateKind::ExecutionNotInFlow) => {
                StageDependencyOutcome::ExecutionNotInFlow
            }
            _ => return Err(FabricError::Engine(Box::new(err))),
        })
    }

    // ── Local helper: PublicState → wire string ────────────────────

    /// FF's `PublicState::as_str()` returns the canonical wire form
    /// (`"waiting"`, `"running"`, `"terminal_success"`, etc.). Cairn
    /// stores the raw string so forward-compatible additions don't
    /// get swallowed — the `Copy` `as_str()` → owned `String` hop is
    /// a single `.to_owned()`.
    fn public_state_wire_str(state: &flowfabric::core::state::PublicState) -> String {
        state.as_str().to_owned()
    }
}
