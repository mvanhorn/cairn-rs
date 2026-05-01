# PR-C4a — `PostgresControlPlane` method classification

Companion doc to PR-C4a. Enumerates every method on
`cairn_fabric::engine::PostgresControlPlane` and classifies it by
implementation strategy for this PR and its follow-up PR-C4b.

## Key

- **A** — direct delegate to `EngineBackend` trait method; no conversion
  between cairn mirror types and FF wire types required. One-liner:
  `self.backend.X(args).await.map_err(|e| FabricError::Engine(Box::new(e)))`.
- **B** — delegate with cairn-mirror ↔ FF conversion. File-local `mod
  conversions` owns the mappings. Still one-FF-call per method.
- **C** — no FF trait primitive covers this today. Returns
  `EngineError::Unavailable { op }` in PR-C4b. Tracked against FF#473
  (worker-registry parity) or scoped as later Postgres-native bodies.

## Scope of PR-C4a

Everything marked **A** or **B** below is **in scope for PR-C4a**.
Methods marked **C** stay at `unimplemented!("PR-C4")` in this PR; PR-C4b
flips them to `FabricError::Engine(Box::new(EngineError::Unavailable { op }))`
+ documents the gap in `docs/design/postgres-parity-gaps.md`.

## Engine trait (13 methods)

| # | cairn method                 | FF backend method                          | bucket | conversion                                                                         | C4a scope |
|---|------------------------------|--------------------------------------------|--------|------------------------------------------------------------------------------------|-----------|
| 1 | `describe_execution`         | `describe_execution`                       | B      | `ff::ExecutionSnapshot` → cairn `ExecutionSnapshot`: `PublicState::to_wire_str()`, drop `flow_id`, map lease/attempt, `current_lease_epoch` from lease summary. | **yes** |
| 2 | `describe_flow`              | `describe_flow`                            | B      | `ff::FlowSnapshot` → cairn `FlowSnapshot`: keep `flow_kind` as `kind`, drop `cancelled_at`/`cancel_reason`/`cancellation_policy`/`edge_groups` (not surfaced by cairn). | **yes** |
| 3 | `describe_edge`              | `describe_edge`                            | B      | `ff::EdgeSnapshot` → cairn `EdgeSnapshot`: keep edge id + flow id + upstream/downstream + dep kind, parse `edge_state` string into cairn `EdgeState` enum. | **yes** |
| 4 | `list_incoming_edges`        | `list_edges(flow_id, Incoming{to_node:id})`| C      | Two-step: first `resolve_execution_flow_id(id)` then `list_edges`. PG backend supports both, but the cairn trait takes only `execution_id` while FF needs `flow_id` upfront. Routable via compose → **leave to C4b** (needs `resolve_execution_flow_id` + conversion). | **no (C4b)** |
| 5 | `get_execution_tag`          | `get_execution_tag`                        | A      | None — same `Option<String>` shape.                                                | **yes** |
| 6 | `get_execution_lane_id`      | `describe_execution` + `.lane_id`          | B      | Single call to describe, extract `lane_id`. Or FF could add a targeted primitive later. For now: delegate via describe. | **yes** |
| 7 | `set_execution_tag`          | `set_execution_tag`                        | A      | None — same `(id, key, value)` -> `Result<(), _>`.                                 | **yes** |
| 8 | `set_flow_tag`               | `set_flow_tag`                             | A      | None — same shape.                                                                 | **yes** |
| 9 | `set_flow_tags` (bulk)       | `set_flow_tag` (loop)                      | B      | Validate whole batch first (all-or-nothing), then per-key loop. Atomicity slightly weaker than Valkey's variadic HSET; acceptable for PG until FF adds a bulk primitive. | **yes** |
| 10 | `register_worker`           | —                                          | C      | FF has no worker registry primitive. Cairn Valkey impl writes raw `ff:worker:*` hashes + TTL. Filed upstream as FF#473. | **no (C4b)** |
| 11 | `heartbeat_worker`          | —                                          | C      | Same as #10.                                                                       | **no (C4b)** |
| 12 | `mark_worker_dead`          | —                                          | C      | Same as #10.                                                                       | **no (C4b)** |
| 13 | `list_expired_leases`       | —                                          | C      | FF scanner-driven on both backends; cairn Valkey impl scans its own zset keys. No trait primitive; PG has scanners internally but doesn't expose an operator list surface. | **no (C4b)** |

## ControlPlaneBackend trait (22 methods)

| #  | cairn method                 | FF backend method                    | bucket | conversion                                                                                        | C4a scope |
|----|------------------------------|--------------------------------------|--------|---------------------------------------------------------------------------------------------------|-----------|
| 1  | `create_budget`              | `create_budget`                      | B      | cairn args → `CreateBudgetArgs{ dimensions: Vec<String>, hard_limits, soft_limits, …, now }`. Validate equal-length vectors (mirrored from Valkey impl). Map `CreateBudgetResult::Created\|AlreadySatisfied` → `Ok(budget_id)`. | **yes** |
| 2  | `record_spend`               | `record_spend`                       | B      | cairn `&[(&str,u64)]` → `BTreeMap<String,u64>` (reject duplicates, mirroring Valkey impl). Use `RecordSpendArgs::new`. Map `ReportUsageResult` → cairn `BudgetSpendOutcome`. | **yes** |
| 3  | `release_budget`             | `release_budget`                     | B      | `ReleaseBudgetArgs::new(budget_id, execution_id)` → `Result<(), _>`. | **yes** |
| 4  | `get_budget_status`          | `get_budget_status`                  | B      | `ff::BudgetStatus` → cairn `BudgetStatusSnapshot`. FF returns `BudgetStatus` directly (not `Option<_>`); PG returns `Err(NotFound)` when missing → map to `Ok(None)`. | **yes** |
| 5  | `create_quota_policy`        | `create_quota_policy`                | B      | `CreateQuotaPolicyArgs::new`. cairn trait also persists `scope_type`/`scope_id` on the def hash — FF's `CreateQuotaPolicyArgs` doesn't carry scope metadata. Accept this surface gap: PG backend ignores scope today; record in parity gaps. Cairn callers read scope back through other paths. | **yes** |
| 6  | `check_admission`            | `check_admission`                    | B      | cairn args → `CheckAdmissionArgs{execution_id, now, window_seconds, rate_limit, concurrency_cap, jitter_ms:None}`. Dimension `"default"`. Map `CheckAdmissionResult` → cairn `QuotaAdmission`. | **yes** |
| 7  | `rotate_waitpoint_hmac`      | `rotate_waitpoint_hmac_secret_all`   | B      | `RotateWaitpointHmacSecretAllArgs::new(kid, secret_hex, grace_ms)`. Map `RotateWaitpointHmacSecretAllResult` → cairn `RotationOutcome{rotated, noop, failed, new_kid}`. PG returns single-entry vec (partition=0). | **yes** |
| 8  | `create_run_execution`       | `create_execution`                   | B      | cairn `CreateRunExecutionInput` → `CreateExecutionArgs`: empty `input_payload`, `priority=0`, `execution_kind="run"`, tags hash-map, policy parsed from JSON or `None`, partition_id from `execution_partition()`. Map `CreateExecutionResult::Created`→`newly_created: true`, `Duplicate`→`newly_created: false`. | **yes** |
| 9  | `complete_run_execution`     | `complete_execution`                 | B      | cairn `CompleteRunInput{execution_id, lease}` → `CompleteExecutionArgs{execution_id, fence: LeaseFence?, attempt_index, result_payload:None, result_encoding:None, source: CancelSource::from_str(lease.source), now: TimestampMs::now()}`. `fence` is built by `lease_fence_from_context` helper: returns `None` iff all three of lease_id/epoch/attempt_id are empty (operator_override path); `Some` otherwise. | **yes** |
| 10 | `fail_run_execution`         | `fail_execution`                     | B      | cairn `FailRunInput` → `FailExecutionArgs`. Same fence logic as #9. Map `FailExecutionResult::RetryScheduled`→`FailExecutionOutcome::RetryScheduled`, `TerminalFailed`→`TerminalFailed`. | **yes** |
| 11 | `cancel_run_execution`       | `cancel_execution`                   | B      | cairn `CancelRunInput` → `CancelExecutionArgs{execution_id, reason: "override".into(), source: CancelSource::OperatorOverride, lease_id, lease_epoch, attempt_id, now}`. Fence fields split into `Option<_>` on the Args struct. | **yes** |
| 12 | `resume_run_execution`       | `resume_execution`                   | B      | cairn `ResumeRunInput` → `ResumeExecutionArgs{execution_id, trigger_type: input.resume_source, resume_delay_ms: 0}`. | **yes** |
| 13 | `deliver_approval_signal`    | `deliver_approval_signal`            | B      | Direct `DeliverApprovalSignalArgs::new` (same shape cairn Valkey uses). Drop FF's `DeliverSignalResult` variants → `Ok(())`. | **yes** |
| 14 | `create_flow`                | `create_flow`                        | B      | `CreateFlowArgs{flow_id, flow_kind, namespace, now}` → `Ok(())`. | **yes** |
| 15 | `cancel_flow`                | `cancel_flow_header`                 | B      | Use `cancel_flow_header` (not `cancel_flow`) to stay outside the dispatch/wait machinery. `CancelFlowArgs{flow_id, reason, cancellation_policy, now}`. Map `CancelFlowHeader::Cancelled`→`FlowCancelOutcome::Cancelled`, `AlreadyTerminal`→`AlreadyTerminal`. | **yes** |
| 16 | `issue_grant_and_claim`      | `issue_grant_and_claim`              | B      | `IssueGrantAndClaimArgs::new` (same shape cairn Valkey uses). Map `ClaimGrantOutcome{lease_id, lease_epoch, attempt_index}` directly. | **yes** |
| 17 | `submit_task_execution`      | `create_execution`                   | B      | Same as #8 but `execution_kind="task"`, `priority` from input, historical default retry policy when `policy_json` empty. | **yes** |
| 18 | `add_execution_to_flow`      | `create_flow` + `add_execution_to_flow` | B   | Two-step: idempotent `create_flow` (idempotent), then `add_execution_to_flow`. Mirrors cairn Valkey impl. | **yes** |
| 19 | `stage_dependency_edge`      | `stage_dependency_edge`              | B      | `StageDependencyEdgeArgs{..., dependency_kind, data_passing_ref: non_empty_option, expected_graph_revision, now}`. Map typed outcome — PG's `StageDependencyEdgeResult::Staged{new_graph_revision}` → `StageDependencyOutcome::Staged{..}`. Error variants (`Conflict{StaleGraphRevision}`, `Validation{Cycle}` etc) surface via `EngineError` classes — translate to matching cairn outcome variants. | **yes** |
| 20 | `apply_dependency_to_child`  | `apply_dependency_to_child`          | B      | `ApplyDependencyToChildArgs{..., now}`. Map `ApplyDependencyToChildResult::{Applied, AlreadyApplied}` → `Ok(())`. | **yes** |
| 21 | `evaluate_flow_eligibility`  | `evaluate_flow_eligibility`          | B      | `EvaluateFlowEligibilityArgs{execution_id}`. Map `Status{status}` → `EligibilityResult::{Eligible, BlockedByDependencies, Other(..)}` via the same match cairn Valkey uses. | **yes** |
| 22 | `renew_task_lease`           | `renew_lease`                        | B      | cairn `RenewLeaseInput` → `RenewLeaseArgs{execution_id, attempt_index, fence: Some(LeaseFence{..}), lease_ttl_ms, lease_history_grace_ms: default}`. `fence` is mandatory here (no operator override path on renew). | **yes** |

## Totals

- **Bucket A**: 3 methods (`get_execution_tag`, `set_execution_tag`, `set_flow_tag`)
- **Bucket B**: 27 methods (remaining described above, in-scope for C4a)
- **Bucket C**: 5 methods (`list_incoming_edges`, `register_worker`,
  `heartbeat_worker`, `mark_worker_dead`, `list_expired_leases`) — deferred to PR-C4b

That's 30 cairn trait methods routed (3 A + 27 B) + 5 deferred. The
total 35 of PR-C3's stubs is preserved.

## FF upstream parity gap

FF#473 tracks the four worker-registry primitives the FF trait lacks
(`register_worker`, `heartbeat_worker`, `mark_worker_dead`,
`list_expired_leases`). Closure on FF's side unblocks lifting them from
PR-C4b's `Unavailable` stance into direct trait delegates.
