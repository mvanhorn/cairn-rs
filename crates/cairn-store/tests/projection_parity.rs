//! RFC-025 Phase 0 projection-parity harness.
//!
//! For a representative subset of `RuntimeEvent` variants declared
//! `ProjectionStatus::Projected` in `projection_registry::REGISTRY`, emit
//! a canonical event instance into the in-memory store and the SQLite
//! store, then assert the read-model query surface returns identical
//! records across backends.
//!
//! ## Scope (Phase 0, deliberately subset-based)
//!
//! This file exercises the core state machines (session, run, task,
//! approval, and the tenant/workspace/project org hierarchy) — ~10 of
//! the 48 Projected variants. The framework and fixture pattern are in
//! place; Phase 1 (evals) and Phase 2a/2b extend the harness as each new
//! projection ships. Adding a variant is a ~30-line fixture block
//! following the shape below. See
//! `docs/design/rfcs/RFC-025-runtime-aggregate-backend-abstraction.md`
//! §"Phase 0" for the rationale.
//!
//! The existing registry-integrity tests (`every_registered_variant_has_a_status`,
//! `projected_variants_cite_a_table_or_shared_projection`, and
//! `fixture_variants_are_all_registered_as_projected`) cover every
//! Projected variant unconditionally, so a classification drift is
//! caught even before a fixture is written for that variant.
//!
//! ## Postgres
//!
//! pg backend parity is gated behind the `postgres` feature + the
//! `TEST_DATABASE_URL` env var (same pattern as
//! `tests/event_log_batch_append.rs`), and marked `#[ignore]` so
//! `cargo test` skips by default. Nightly CI runs it with
//! `cargo test -- --ignored` after exporting a live database URL.
//!
//! ## Invariants enforced by this harness
//!
//! 1. No `Projected` variant with a fixture below is silently missing
//!    from the sqlite applier.
//! 2. No `Projected` variant with a fixture below produces divergent
//!    read records across InMemory ↔ SQLite (direct field-by-field
//!    `assert_eq!` — no timestamp tolerance is currently applied
//!    because the fixtures construct static timestamps).
//! 3. Every registry entry carries a valid status at runtime (guards
//!    against a renamed variant silently falling out of the registry;
//!    the build.rs already enforces this at compile time, the harness
//!    enforces it at test time as defense-in-depth).

use cairn_store::projection_registry::{ProjectionStatus, REGISTRY};

#[test]
fn every_registered_variant_has_a_status() {
    for entry in REGISTRY {
        // Trivial but useful: pattern-match every variant so a future
        // `ProjectionStatus` addition forces an update here.
        match entry.status {
            ProjectionStatus::Projected { .. }
            | ProjectionStatus::Stubbed { .. }
            | ProjectionStatus::Ephemeral { .. } => {}
        }
    }
}

#[test]
fn projected_variants_cite_a_table_or_shared_projection() {
    // Catch registry drift: if a variant is declared Projected but its
    // entry is missing the `table` name, flag the drift loudly. The
    // harness parity tests below assume every Projected entry has at
    // minimum a backing table (or a deliberately `None` entry for a
    // shared-table projection — kept as an allowlist here so adding
    // `None` is an explicit choice, not an oversight).
    const ALLOW_NO_TABLE: &[&str] = &[
        // Currently empty — every Projected entry has a table. Add
        // entries here only with an inline comment explaining why.
    ];

    for entry in REGISTRY {
        if let ProjectionStatus::Projected { table } = entry.status {
            if table.is_none() && !ALLOW_NO_TABLE.contains(&entry.variant) {
                panic!(
                    "Projected variant {} has no table annotation. Either set \
                     ProjectionStatus::Projected {{ table: Some(..) }} or add to ALLOW_NO_TABLE \
                     with a justification.",
                    entry.variant
                );
            }
        }
    }
}

#[cfg(feature = "sqlite")]
mod in_memory_vs_sqlite {
    //! Cross-backend parity for Projected read models.

    use cairn_domain::{
        audit::AuditOutcome, events::ActualOutcome, workers::ExternalWorkerReport, ApprovalId,
        ApprovalRequested, ApprovalRequirement, AuditLogEntryRecorded, EventEnvelope, EventId,
        EventSource, ExternalWorkerReactivated, ExternalWorkerRegistered, ExternalWorkerReported,
        ExternalWorkerSuspended, OperatorId, OutcomeId, OutcomeRecorded, PlanApproved,
        PlanProposed, PlanRejected, PlanRevisionRequested, ProjectCreated, ProjectKey,
        ResourceShareRevoked, ResourceShared, RunCreated, RunId, RunState, RunStateChanged,
        RuntimeEvent, ScheduledTaskCreated, ScheduledTaskId, SessionCreated, SessionId,
        SessionState, SessionStateChanged, SignalId, SignalIngested, SoulPatchApplied,
        SoulPatchProposed, StateTransition, SubagentSpawned, TaskCreated, TaskId, TaskState,
        TaskStateChanged, TenantCreated, TenantId, ToolRecoveryPaused, UserMessageAppended,
        WorkerId, WorkspaceCreated, WorkspaceId,
    };
    use cairn_store::event_log::EventLog;
    use cairn_store::in_memory::InMemoryStore;
    use cairn_store::projections::{
        ApprovalReadModel, AuditLogReadModel, ExternalWorkerReadModel, OutcomeReadModel,
        PlanReviewReadModel, PlanReviewState, ResourceSharingReadModel, RunReadModel,
        ScheduledTaskReadModel, SessionReadModel, SignalReadModel, SoulPatchReadModel,
        SoulPatchState, SubagentSpawnReadModel, TaskReadModel, ToolRecoveryPauseReadModel,
        UserMessageReadModel,
    };
    use cairn_store::sqlite::SqliteAdapter;

    use std::sync::atomic::{AtomicU64, Ordering};

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    fn next_event_id() -> EventId {
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        EventId::new(format!("evt_{n}"))
    }

    fn env(event: RuntimeEvent) -> EventEnvelope<RuntimeEvent> {
        EventEnvelope::for_runtime_event(next_event_id(), EventSource::Runtime, event)
    }

    fn project() -> ProjectKey {
        ProjectKey::new("t_parity", "w_parity", "p_parity")
    }

    /// Append the same event list to both backends. Each call uses a
    /// fresh project scope so parallel tests don't collide on
    /// (session_id, run_id) uniqueness.
    async fn append_both(
        mem: &InMemoryStore,
        sqlite: &cairn_store::sqlite::SqliteEventLog,
        events: &[EventEnvelope<RuntimeEvent>],
    ) {
        mem.append(events).await.expect("in-memory append");
        sqlite.append(events).await.expect("sqlite append");
    }

    // ── SessionCreated + SessionStateChanged ──────────────────────────
    #[tokio::test]
    async fn session_projection_matches_across_backends() {
        let mem = InMemoryStore::new();
        let adapter = SqliteAdapter::in_memory().await.unwrap();
        let sqlite_log = cairn_store::sqlite::SqliteEventLog::new(adapter.pool().clone());

        let sid = SessionId::new("s_parity_1");
        let events = vec![
            env(RuntimeEvent::SessionCreated(SessionCreated {
                project: project(),
                session_id: sid.clone(),
            })),
            env(RuntimeEvent::SessionStateChanged(SessionStateChanged {
                project: project(),
                session_id: sid.clone(),
                transition: StateTransition {
                    from: Some(SessionState::Open),
                    to: SessionState::Completed,
                },
            })),
        ];
        append_both(&mem, &sqlite_log, &events).await;

        let mem_row = SessionReadModel::get(&mem, &sid).await.unwrap().unwrap();
        let sqlite_row = SessionReadModel::get(&adapter, &sid)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(mem_row.session_id, sqlite_row.session_id);
        assert_eq!(mem_row.project, sqlite_row.project);
        assert_eq!(mem_row.state, sqlite_row.state);
        // Both backends must observe the completed-state transition.
        assert_eq!(mem_row.state, SessionState::Completed);
    }

    // ── RunCreated + RunStateChanged ──────────────────────────────────
    #[tokio::test]
    async fn run_projection_matches_across_backends() {
        let mem = InMemoryStore::new();
        let adapter = SqliteAdapter::in_memory().await.unwrap();
        let sqlite_log = cairn_store::sqlite::SqliteEventLog::new(adapter.pool().clone());

        let sid = SessionId::new("s_parity_r1");
        let rid = RunId::new("r_parity_1");
        let events = vec![
            env(RuntimeEvent::SessionCreated(SessionCreated {
                project: project(),
                session_id: sid.clone(),
            })),
            env(RuntimeEvent::RunCreated(RunCreated {
                project: project(),
                session_id: sid,
                run_id: rid.clone(),
                parent_run_id: None,
                prompt_release_id: None,
                agent_role_id: None,
            })),
            env(RuntimeEvent::RunStateChanged(RunStateChanged {
                project: project(),
                run_id: rid.clone(),
                transition: StateTransition {
                    from: Some(RunState::Pending),
                    to: RunState::Running,
                },
                failure_class: None,
                pause_reason: None,
                resume_trigger: None,
            })),
        ];
        append_both(&mem, &sqlite_log, &events).await;

        let mem_row = RunReadModel::get(&mem, &rid).await.unwrap().unwrap();
        let sqlite_row = RunReadModel::get(&adapter, &rid).await.unwrap().unwrap();

        assert_eq!(mem_row.run_id, sqlite_row.run_id);
        assert_eq!(mem_row.session_id, sqlite_row.session_id);
        assert_eq!(mem_row.state, sqlite_row.state);
        assert_eq!(mem_row.failure_class, sqlite_row.failure_class);
        assert_eq!(mem_row.state, RunState::Running);
    }

    // ── TaskCreated + TaskStateChanged ───────────────────────────────
    #[tokio::test]
    async fn task_projection_matches_across_backends() {
        let mem = InMemoryStore::new();
        let adapter = SqliteAdapter::in_memory().await.unwrap();
        let sqlite_log = cairn_store::sqlite::SqliteEventLog::new(adapter.pool().clone());

        let sid = SessionId::new("s_parity_t1");
        let rid = RunId::new("r_parity_t1");
        let tid = TaskId::new("task_parity_1");
        let events = vec![
            env(RuntimeEvent::SessionCreated(SessionCreated {
                project: project(),
                session_id: sid.clone(),
            })),
            env(RuntimeEvent::RunCreated(RunCreated {
                project: project(),
                session_id: sid,
                run_id: rid.clone(),
                parent_run_id: None,
                prompt_release_id: None,
                agent_role_id: None,
            })),
            env(RuntimeEvent::TaskCreated(TaskCreated {
                project: project(),
                task_id: tid.clone(),
                parent_run_id: Some(rid),
                parent_task_id: None,
                prompt_release_id: None,
                session_id: None,
            })),
            env(RuntimeEvent::TaskStateChanged(TaskStateChanged {
                project: project(),
                task_id: tid.clone(),
                transition: StateTransition {
                    from: None,
                    to: TaskState::Completed,
                },
                failure_class: None,
                pause_reason: None,
                resume_trigger: None,
            })),
        ];
        append_both(&mem, &sqlite_log, &events).await;

        let mem_row = TaskReadModel::get(&mem, &tid).await.unwrap().unwrap();
        let sqlite_row = TaskReadModel::get(&adapter, &tid).await.unwrap().unwrap();

        assert_eq!(mem_row.task_id, sqlite_row.task_id);
        assert_eq!(mem_row.state, sqlite_row.state);
        assert_eq!(mem_row.parent_run_id, sqlite_row.parent_run_id);
        assert_eq!(mem_row.state, TaskState::Completed);
    }

    // ── ApprovalRequested ────────────────────────────────────────────
    #[tokio::test]
    async fn approval_projection_matches_across_backends() {
        let mem = InMemoryStore::new();
        let adapter = SqliteAdapter::in_memory().await.unwrap();
        let sqlite_log = cairn_store::sqlite::SqliteEventLog::new(adapter.pool().clone());

        let sid = SessionId::new("s_parity_ap1");
        let rid = RunId::new("r_parity_ap1");
        let aid = ApprovalId::new("ap_parity_1");
        let events = vec![
            env(RuntimeEvent::SessionCreated(SessionCreated {
                project: project(),
                session_id: sid.clone(),
            })),
            env(RuntimeEvent::RunCreated(RunCreated {
                project: project(),
                session_id: sid,
                run_id: rid.clone(),
                parent_run_id: None,
                prompt_release_id: None,
                agent_role_id: None,
            })),
            env(RuntimeEvent::ApprovalRequested(ApprovalRequested {
                project: project(),
                approval_id: aid.clone(),
                run_id: Some(rid),
                task_id: None,
                requirement: ApprovalRequirement::Required,
                title: Some("parity check".into()),
                description: None,
            })),
        ];
        append_both(&mem, &sqlite_log, &events).await;

        let mem_row = ApprovalReadModel::get(&mem, &aid).await.unwrap().unwrap();
        let sqlite_row = ApprovalReadModel::get(&adapter, &aid)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(mem_row.approval_id, sqlite_row.approval_id);
        assert_eq!(mem_row.requirement, sqlite_row.requirement);
        assert_eq!(mem_row.run_id, sqlite_row.run_id);
        assert_eq!(mem_row.decision, sqlite_row.decision);
        assert_eq!(mem_row.requirement, ApprovalRequirement::Required);
    }

    // ── TenantCreated / WorkspaceCreated / ProjectCreated ────────────
    #[tokio::test]
    async fn org_hierarchy_projection_matches_across_backends() {
        let mem = InMemoryStore::new();
        let adapter = SqliteAdapter::in_memory().await.unwrap();
        let sqlite_log = cairn_store::sqlite::SqliteEventLog::new(adapter.pool().clone());

        let tenant_id = TenantId::new("t_org_1");
        let workspace_id = WorkspaceId::new("w_org_1");
        let scope = ProjectKey::new(tenant_id.as_str(), workspace_id.as_str(), "p_org_1");
        let events = vec![
            env(RuntimeEvent::TenantCreated(TenantCreated {
                project: scope.clone(),
                tenant_id: tenant_id.clone(),
                name: "parity-tenant".into(),
                created_at: 1_000,
            })),
            env(RuntimeEvent::WorkspaceCreated(WorkspaceCreated {
                project: scope.clone(),
                tenant_id: tenant_id.clone(),
                workspace_id: workspace_id.clone(),
                name: "parity-workspace".into(),
                created_at: 2_000,
            })),
            env(RuntimeEvent::ProjectCreated(ProjectCreated {
                project: scope.clone(),
                name: "parity-project".into(),
                created_at: 3_000,
            })),
        ];
        append_both(&mem, &sqlite_log, &events).await;

        // The tenant/workspace/project hierarchy is projected into
        // dedicated tables on both sides. We don't have a public
        // read-model trait for tenants/workspaces/projects in the
        // cairn-store crate at the moment (they're queried via
        // cairn-app org routes), so assert via the event stream: both
        // backends must surface the same three envelopes in the same
        // order. Parity is established at the event-log layer; the
        // projection tables are exercised by downstream integration
        // tests in cairn-app.
        let mem_stream = mem.read_stream(None, 10).await.unwrap();
        let sqlite_stream = sqlite_log.read_stream(None, 10).await.unwrap();
        assert_eq!(mem_stream.len(), sqlite_stream.len());
        for (m, s) in mem_stream.iter().zip(sqlite_stream.iter()) {
            assert_eq!(m.envelope.payload, s.envelope.payload);
        }
    }

    // ── Registry integration: every Projected variant we've wired a
    //    fixture for must be in the registry, and none of the above
    //    tests exercise a Stubbed variant (would indicate drift).
    #[test]
    fn fixture_variants_are_all_registered_as_projected() {
        use cairn_store::projection_registry::{lookup, ProjectionStatus};

        // Variants actually exercised above.
        let exercised = [
            "SessionCreated",
            "SessionStateChanged",
            "RunCreated",
            "RunStateChanged",
            "TaskCreated",
            "TaskStateChanged",
            "ApprovalRequested",
            "TenantCreated",
            "WorkspaceCreated",
            "ProjectCreated",
            // RFC-025 Phase 1 milestone 8: eval fixtures below.
            "EvalRunStarted",
            "EvalRunCompleted",
            "EvalRunArchived",
            "EvalRunScored",
            "EvalRubricScored",
            // RFC-025 Phase 2a.1 milestone 1: credentials fixtures below.
            "CredentialStored",
            "CredentialRevoked",
            "CredentialKeyRotated",
            // RFC-025 Phase 2a.1 milestone 2: tenant-quota fixtures below.
            "TenantQuotaSet",
            "TenantQuotaViolated",
            // RFC-025 Phase 2a.1 milestone 3: provider-budget fixtures below.
            "ProviderBudgetSet",
            "ProviderBudgetAlertTriggered",
            "ProviderBudgetExceeded",
            // RFC-025 Phase 2a.1 milestone 4: license fixture below.
            "LicenseActivated",
            // RFC-025 Phase 3: provider binding + connection fixtures
            // below.
            "ProviderBindingCreated",
            "ProviderBindingStateChanged",
            "ProviderConnectionRegistered",
            "ProviderConnectionDeleted",
            // RFC-025 Phase 2b.1 m1: audit-log fixture below.
            "AuditLogEntryRecorded",
            // RFC-025 Phase 2b.1 m2: scheduled-tasks fixture below.
            "ScheduledTaskCreated",
            // RFC-025 Phase 2b.1 m3: outcomes fixture below.
            "OutcomeRecorded",
            // RFC-025 Phase 2b.1 m4: plan-review fixtures below.
            "PlanProposed",
            "PlanApproved",
            "PlanRejected",
            "PlanRevisionRequested",
            // RFC-025 Phase 2a.2 milestone 1: approval-delegation fixture
            // below.
            "ApprovalDelegated",
            // RFC-025 Phase 2a.2 milestone 2: guardrail fixtures below.
            "GuardrailPolicyCreated",
            "GuardrailPolicyEvaluated",
            // RFC-025 Phase 2a.2 milestone 3: retention fixture below.
            "RetentionPolicySet",
            // RFC-025 Phase 2a.2 milestone 4: entitlement override fixture
            // below.
            "EntitlementOverrideSet",
            // RFC-025 Phase 2b.2 m1: external-worker fixtures below.
            "ExternalWorkerRegistered",
            "ExternalWorkerReported",
            "ExternalWorkerSuspended",
            "ExternalWorkerReactivated",
            // RFC-025 Phase 2b.2b m1: resource-sharing fixtures below.
            "ResourceShared",
            "ResourceShareRevoked",
            // RFC-025 Phase 2b.2b m2: signal-ingestion fixture below.
            "SignalIngested",
            // RFC-025 Phase 2b.2b m3: subagent-spawn fixture below.
            "SubagentSpawned",
            // RFC-025 Phase 2b.2b m4: user-message fixture below.
            "UserMessageAppended",
            // RFC-025 Phase 2b.2b m5: soul-patch fixtures below.
            "SoulPatchProposed",
            "SoulPatchApplied",
            // RFC-025 Phase 2b.2b m6: tool-recovery-pause fixture below.
            "ToolRecoveryPaused",
        ];
        for v in exercised {
            let status = lookup(v).unwrap_or_else(|| panic!("{v} missing from registry"));
            assert!(
                matches!(status, ProjectionStatus::Projected { .. }),
                "{v} should be Projected per Phase 0/1 classification, got {status:?}"
            );
        }
    }

    // ── RFC-025 Phase 1 milestone 8: eval_runs projection parity.
    //    For each of the five eval lifecycle variants, emit the event
    //    into both backends and assert the read-model record is field-
    //    by-field equal. If pg/sqlite appliers drift (e.g. one reads
    //    archived_at as the event timestamp and the other as the
    //    stored_at time), this test catches it.
    use cairn_domain::{
        EvalMetrics, EvalRubricScored, EvalRunArchived, EvalRunCompleted, EvalRunId, EvalRunScored,
        EvalRunStarted,
    };
    use cairn_store::projections::EvalRunReadModel;

    #[tokio::test]
    async fn eval_run_projection_matches_across_backends_started_only() {
        let mem = InMemoryStore::new();
        let adapter = SqliteAdapter::in_memory().await.unwrap();
        let sqlite_log = cairn_store::sqlite::SqliteEventLog::new(adapter.pool().clone());

        let eval_run_id = EvalRunId::new("er_parity_started");
        let events = vec![env(RuntimeEvent::EvalRunStarted(EvalRunStarted {
            project: project(),
            eval_run_id: eval_run_id.clone(),
            subject_kind: "prompt_release".into(),
            evaluator_type: "accuracy".into(),
            started_at: 1_000_000,
            prompt_asset_id: Some(cairn_domain::PromptAssetId::new("pa_parity")),
            prompt_version_id: Some(cairn_domain::PromptVersionId::new("pv_parity")),
            prompt_release_id: Some(cairn_domain::PromptReleaseId::new("pr_parity")),
            created_by: Some(cairn_domain::OperatorId::new("op_parity")),
            dataset_id: Some("ds_parity".into()),
            rubric_id: Some("ru_parity".into()),
            baseline_id: Some("bl_parity".into()),
        }))];
        append_both(&mem, &sqlite_log, &events).await;

        let mem_rec = EvalRunReadModel::get(&mem, &eval_run_id)
            .await
            .unwrap()
            .expect("started run in memory");
        let sqlite_rec = EvalRunReadModel::get(&adapter, &eval_run_id)
            .await
            .unwrap()
            .expect("started run in sqlite");

        assert_eq!(mem_rec.eval_run_id, sqlite_rec.eval_run_id);
        assert_eq!(mem_rec.project, sqlite_rec.project);
        assert_eq!(mem_rec.subject_kind, sqlite_rec.subject_kind);
        assert_eq!(mem_rec.evaluator_type, sqlite_rec.evaluator_type);
        assert_eq!(mem_rec.started_at, sqlite_rec.started_at);
        assert_eq!(mem_rec.dataset_id, sqlite_rec.dataset_id);
        assert_eq!(mem_rec.rubric_id, sqlite_rec.rubric_id);
        assert_eq!(mem_rec.baseline_id, sqlite_rec.baseline_id);
        assert_eq!(mem_rec.prompt_asset_id, sqlite_rec.prompt_asset_id);
        assert_eq!(mem_rec.prompt_version_id, sqlite_rec.prompt_version_id);
        assert_eq!(mem_rec.prompt_release_id, sqlite_rec.prompt_release_id);
        assert_eq!(mem_rec.created_by, sqlite_rec.created_by);
        assert!(mem_rec.archived_at.is_none());
        assert!(sqlite_rec.archived_at.is_none());
        assert!(mem_rec.metrics.is_none());
        assert!(sqlite_rec.metrics.is_none());
    }

    #[tokio::test]
    async fn eval_run_projection_matches_across_backends_full_lifecycle() {
        let mem = InMemoryStore::new();
        let adapter = SqliteAdapter::in_memory().await.unwrap();
        let sqlite_log = cairn_store::sqlite::SqliteEventLog::new(adapter.pool().clone());

        let eval_run_id = EvalRunId::new("er_parity_full");
        let metrics = EvalMetrics {
            task_success_rate: Some(0.875),
            latency_p50_ms: Some(42),
            latency_p99_ms: Some(99),
            cost_per_run: Some(0.0042),
            policy_pass_rate: Some(0.91),
            retrieval_hit_at_k: Some(0.77),
            citation_coverage: None,
            source_diversity: None,
            retrieval_latency_ms: None,
            retrieval_cost: None,
        };
        let events = vec![
            env(RuntimeEvent::EvalRunStarted(EvalRunStarted {
                project: project(),
                eval_run_id: eval_run_id.clone(),
                subject_kind: "prompt_release".into(),
                evaluator_type: "accuracy".into(),
                started_at: 2_000_000,
                prompt_asset_id: None,
                prompt_version_id: None,
                prompt_release_id: None,
                created_by: None,
                dataset_id: None,
                rubric_id: None,
                baseline_id: None,
            })),
            env(RuntimeEvent::EvalRunScored(EvalRunScored {
                project: project(),
                eval_run_id: eval_run_id.clone(),
                metrics: metrics.clone(),
                recorded_at_ms: 2_000_100,
            })),
            env(RuntimeEvent::EvalRubricScored(EvalRubricScored {
                project: project(),
                eval_run_id: eval_run_id.clone(),
                rubric_id: "ru_parity_full".into(),
                dimension_scores: vec![("clarity".into(), 0.9), ("safety".into(), 0.8)],
                overall: 0.86,
                recorded_at_ms: 2_000_200,
            })),
            env(RuntimeEvent::EvalRunCompleted(EvalRunCompleted {
                project: project(),
                eval_run_id: eval_run_id.clone(),
                success: true,
                error_message: None,
                subject_node_id: None,
                completed_at: 2_000_300,
            })),
        ];
        append_both(&mem, &sqlite_log, &events).await;

        let mem_rec = EvalRunReadModel::get(&mem, &eval_run_id)
            .await
            .unwrap()
            .expect("full-lifecycle run in memory");
        let sqlite_rec = EvalRunReadModel::get(&adapter, &eval_run_id)
            .await
            .unwrap()
            .expect("full-lifecycle run in sqlite");

        // Identity + scope.
        assert_eq!(mem_rec.eval_run_id, sqlite_rec.eval_run_id);
        assert_eq!(mem_rec.project, sqlite_rec.project);
        // Lifecycle transitions.
        assert_eq!(mem_rec.success, sqlite_rec.success);
        assert_eq!(mem_rec.success, Some(true));
        assert_eq!(mem_rec.completed_at, sqlite_rec.completed_at);
        assert_eq!(mem_rec.completed_at, Some(2_000_300));
        // Metrics — field-by-field using to_bits to handle NaN parity.
        let mem_m = mem_rec.metrics.as_ref().expect("memory metrics set");
        let sq_m = sqlite_rec.metrics.as_ref().expect("sqlite metrics set");
        assert_eq!(
            mem_m.task_success_rate.map(f64::to_bits),
            sq_m.task_success_rate.map(f64::to_bits),
        );
        assert_eq!(mem_m.latency_p50_ms, sq_m.latency_p50_ms);
        assert_eq!(mem_m.latency_p99_ms, sq_m.latency_p99_ms);
        assert_eq!(
            mem_m.cost_per_run.map(f64::to_bits),
            sq_m.cost_per_run.map(f64::to_bits),
        );
        // Rubric verdict.
        let mem_r = mem_rec.rubric_score.as_ref().expect("memory rubric set");
        let sq_r = sqlite_rec.rubric_score.as_ref().expect("sqlite rubric set");
        assert_eq!(mem_r, sq_r);
    }

    #[tokio::test]
    async fn eval_run_archived_at_matches_across_backends() {
        let mem = InMemoryStore::new();
        let adapter = SqliteAdapter::in_memory().await.unwrap();
        let sqlite_log = cairn_store::sqlite::SqliteEventLog::new(adapter.pool().clone());

        let eval_run_id = EvalRunId::new("er_parity_archived");
        let events = vec![
            env(RuntimeEvent::EvalRunStarted(EvalRunStarted {
                project: project(),
                eval_run_id: eval_run_id.clone(),
                subject_kind: "prompt_release".into(),
                evaluator_type: "accuracy".into(),
                started_at: 3_000_000,
                prompt_asset_id: None,
                prompt_version_id: None,
                prompt_release_id: None,
                created_by: None,
                dataset_id: None,
                rubric_id: None,
                baseline_id: None,
            })),
            env(RuntimeEvent::EvalRunArchived(EvalRunArchived {
                project: project(),
                eval_run_id: eval_run_id.clone(),
                archived_at: 3_000_500,
            })),
            // Second archive — earliest-wins: both backends must keep
            // 3_000_500 (#336 Copilot rule). Pre-Phase 1 pg/sqlite were
            // log_stub no-ops and would silently diverge from memory.
            env(RuntimeEvent::EvalRunArchived(EvalRunArchived {
                project: project(),
                eval_run_id: eval_run_id.clone(),
                archived_at: 3_099_999,
            })),
        ];
        append_both(&mem, &sqlite_log, &events).await;

        let mem_rec = EvalRunReadModel::get(&mem, &eval_run_id)
            .await
            .unwrap()
            .unwrap();
        let sqlite_rec = EvalRunReadModel::get(&adapter, &eval_run_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(mem_rec.archived_at, Some(3_000_500));
        assert_eq!(sqlite_rec.archived_at, Some(3_000_500));
        assert_eq!(mem_rec.archived_at, sqlite_rec.archived_at);
    }

    // ── RFC-025 Phase 2a.1 milestone 1: credentials projection parity.
    //    For each of the three credential lifecycle variants, emit the
    //    event into both backends and assert the read-model record is
    //    field-by-field equal.
    use cairn_domain::credentials::CredentialRecord;
    use cairn_domain::{CredentialId, CredentialKeyRotated, CredentialRevoked, CredentialStored};
    use cairn_store::projections::{CredentialReadModel, CredentialRotationReadModel};

    fn assert_credential_records_match(mem: &CredentialRecord, sq: &CredentialRecord) {
        assert_eq!(mem.id, sq.id);
        assert_eq!(mem.tenant_id, sq.tenant_id);
        assert_eq!(mem.name, sq.name);
        assert_eq!(mem.provider_id, sq.provider_id);
        assert_eq!(mem.credential_type, sq.credential_type);
        assert_eq!(mem.encrypted_value, sq.encrypted_value);
        assert_eq!(mem.key_id, sq.key_id);
        assert_eq!(mem.key_version, sq.key_version);
        assert_eq!(mem.active, sq.active);
        assert_eq!(mem.encrypted_at_ms, sq.encrypted_at_ms);
        assert_eq!(mem.revoked_at_ms, sq.revoked_at_ms);
        // All three backends derive `created_at` and `updated_at`
        // directly from the event's `encrypted_at_ms` (on initial
        // store) and `revoked_at_ms` (on revoke), so they must agree
        // byte-for-byte. The pg/sqlite ON CONFLICT DO UPDATE path
        // refreshes `updated_at` from `EXCLUDED.updated_at`
        // (= encrypted_at_ms of the re-store) but preserves
        // `created_at`; the in_memory applier does the same via the
        // get_mut branch. The equality assertions below lock that
        // contract in place — Copilot PR #565 pushed back on the
        // earlier "treat as projection-policy delta" phrasing.
        assert_eq!(mem.created_at, sq.created_at);
        assert_eq!(mem.updated_at, sq.updated_at);
    }

    #[tokio::test]
    async fn credential_stored_projection_matches_across_backends() {
        let mem = InMemoryStore::new();
        let adapter = SqliteAdapter::in_memory().await.unwrap();
        let sqlite_log = cairn_store::sqlite::SqliteEventLog::new(adapter.pool().clone());

        let tenant_id = TenantId::new("t_parity_cred");
        let credential_id = CredentialId::new("cred_parity_1");
        let events = vec![env(RuntimeEvent::CredentialStored(CredentialStored {
            tenant_id: tenant_id.clone(),
            credential_id: credential_id.clone(),
            provider_id: "openai".into(),
            encrypted_value: vec![0xDE, 0xAD, 0xBE, 0xEF],
            key_id: Some("k1".into()),
            key_version: Some("v1".into()),
            encrypted_at_ms: 1_000_000,
        }))];
        append_both(&mem, &sqlite_log, &events).await;

        let mem_rec = CredentialReadModel::get(&mem, &credential_id)
            .await
            .unwrap()
            .expect("memory credential record");
        let sqlite_rec = CredentialReadModel::get(&adapter, &credential_id)
            .await
            .unwrap()
            .expect("sqlite credential record");

        assert_credential_records_match(&mem_rec, &sqlite_rec);
        assert!(mem_rec.active);
        assert!(sqlite_rec.active);
        // `encrypted_value` is a `RedactedCiphertext` (#579) that
        // derefs to `[u8]`; compare through the slice view.
        assert_eq!(
            &*mem_rec.encrypted_value,
            [0xDE, 0xAD, 0xBE, 0xEF].as_slice()
        );
        assert_eq!(
            &*sqlite_rec.encrypted_value,
            [0xDE, 0xAD, 0xBE, 0xEF].as_slice()
        );
        assert_eq!(mem_rec.key_id.as_deref(), Some("k1"));
        assert_eq!(sqlite_rec.key_id.as_deref(), Some("k1"));

        // Tenant list surface matches too.
        let mem_list = CredentialReadModel::list_by_tenant(&mem, &tenant_id, 10, 0)
            .await
            .unwrap();
        let sqlite_list = CredentialReadModel::list_by_tenant(&adapter, &tenant_id, 10, 0)
            .await
            .unwrap();
        assert_eq!(mem_list.len(), 1);
        assert_eq!(sqlite_list.len(), 1);
    }

    #[tokio::test]
    async fn credential_revoked_projection_matches_across_backends() {
        let mem = InMemoryStore::new();
        let adapter = SqliteAdapter::in_memory().await.unwrap();
        let sqlite_log = cairn_store::sqlite::SqliteEventLog::new(adapter.pool().clone());

        let tenant_id = TenantId::new("t_parity_cred_rev");
        let credential_id = CredentialId::new("cred_parity_rev");
        let events = vec![
            env(RuntimeEvent::CredentialStored(CredentialStored {
                tenant_id: tenant_id.clone(),
                credential_id: credential_id.clone(),
                provider_id: "anthropic".into(),
                encrypted_value: vec![0xAA, 0xBB],
                key_id: None,
                key_version: None,
                encrypted_at_ms: 2_000_000,
            })),
            env(RuntimeEvent::CredentialRevoked(CredentialRevoked {
                tenant_id: tenant_id.clone(),
                credential_id: credential_id.clone(),
                revoked_at_ms: 2_000_500,
            })),
            // Second revocation — latest-wins semantics (mirrors the
            // in_memory applier's `rec.revoked_at_ms = Some(..)` overwrite).
            // Both backends must reflect the latest timestamp.
            env(RuntimeEvent::CredentialRevoked(CredentialRevoked {
                tenant_id: tenant_id.clone(),
                credential_id: credential_id.clone(),
                revoked_at_ms: 2_900_000,
            })),
        ];
        append_both(&mem, &sqlite_log, &events).await;

        let mem_rec = CredentialReadModel::get(&mem, &credential_id)
            .await
            .unwrap()
            .unwrap();
        let sqlite_rec = CredentialReadModel::get(&adapter, &credential_id)
            .await
            .unwrap()
            .unwrap();

        assert!(!mem_rec.active);
        assert!(!sqlite_rec.active);
        assert_eq!(mem_rec.revoked_at_ms, Some(2_900_000));
        assert_eq!(sqlite_rec.revoked_at_ms, Some(2_900_000));

        // Active-only list must not surface the revoked credential.
        let mem_active = CredentialReadModel::list_all_active(&mem, 100)
            .await
            .unwrap()
            .unwrap();
        let sqlite_active = CredentialReadModel::list_all_active(&adapter, 100)
            .await
            .unwrap()
            .unwrap();
        assert!(
            !mem_active.iter().any(|c| c.id == credential_id),
            "revoked credential must not appear in list_all_active (memory)"
        );
        assert!(
            !sqlite_active.iter().any(|c| c.id == credential_id),
            "revoked credential must not appear in list_all_active (sqlite)"
        );
    }

    #[tokio::test]
    async fn credential_stored_after_revoked_preserves_revocation_across_backends() {
        // RFC-025 Phase 2a.1 milestone 1 (Copilot PR #565): a
        // `CredentialStored` replayed after `CredentialRevoked` must not
        // silently reactivate the credential on any backend. Lock the
        // contract in both on in-memory and sqlite.
        let mem = InMemoryStore::new();
        let adapter = SqliteAdapter::in_memory().await.unwrap();
        let sqlite_log = cairn_store::sqlite::SqliteEventLog::new(adapter.pool().clone());

        let tenant_id = TenantId::new("t_parity_restore");
        let credential_id = CredentialId::new("cred_parity_restore");
        let events = vec![
            env(RuntimeEvent::CredentialStored(CredentialStored {
                tenant_id: tenant_id.clone(),
                credential_id: credential_id.clone(),
                provider_id: "openai".into(),
                encrypted_value: vec![0xAA],
                key_id: Some("k_init".into()),
                key_version: Some("v1".into()),
                encrypted_at_ms: 1_000,
            })),
            env(RuntimeEvent::CredentialRevoked(CredentialRevoked {
                tenant_id: tenant_id.clone(),
                credential_id: credential_id.clone(),
                revoked_at_ms: 2_000,
            })),
            // Re-store with fresh material. Must NOT reactivate.
            env(RuntimeEvent::CredentialStored(CredentialStored {
                tenant_id: tenant_id.clone(),
                credential_id: credential_id.clone(),
                provider_id: "openai".into(),
                encrypted_value: vec![0xBB, 0xCC],
                key_id: Some("k_rotated".into()),
                key_version: Some("v2".into()),
                encrypted_at_ms: 3_000,
            })),
        ];
        append_both(&mem, &sqlite_log, &events).await;

        let mem_rec = CredentialReadModel::get(&mem, &credential_id)
            .await
            .unwrap()
            .unwrap();
        let sqlite_rec = CredentialReadModel::get(&adapter, &credential_id)
            .await
            .unwrap()
            .unwrap();

        // Revoked state preserved on both backends.
        assert!(!mem_rec.active);
        assert!(!sqlite_rec.active);
        assert_eq!(mem_rec.revoked_at_ms, Some(2_000));
        assert_eq!(sqlite_rec.revoked_at_ms, Some(2_000));
        // Fresh material landed — the re-store still updates the
        // encrypted payload and key bindings on the existing row.
        assert_eq!(&*mem_rec.encrypted_value, [0xBB, 0xCC].as_slice());
        assert_eq!(&*sqlite_rec.encrypted_value, [0xBB, 0xCC].as_slice());
        assert_eq!(mem_rec.key_id.as_deref(), Some("k_rotated"));
        assert_eq!(sqlite_rec.key_id.as_deref(), Some("k_rotated"));
        // `created_at` preserves the original store timestamp on both
        // backends (pg/sqlite via ON CONFLICT absent-update, in-memory
        // via the get_mut branch introduced in this milestone).
        assert_eq!(mem_rec.created_at, 1_000);
        assert_eq!(sqlite_rec.created_at, 1_000);
        assert_credential_records_match(&mem_rec, &sqlite_rec);
    }

    #[tokio::test]
    async fn credential_key_rotated_projection_matches_across_backends() {
        let mem = InMemoryStore::new();
        let adapter = SqliteAdapter::in_memory().await.unwrap();
        let sqlite_log = cairn_store::sqlite::SqliteEventLog::new(adapter.pool().clone());

        let tenant_id = TenantId::new("t_parity_rot");
        let events = vec![
            env(RuntimeEvent::CredentialKeyRotated(CredentialKeyRotated {
                tenant_id: tenant_id.clone(),
                rotation_id: "rot_parity_1".into(),
                old_key_id: "k1".into(),
                new_key_id: "k2".into(),
                credential_ids_rotated: vec!["cred_a".into(), "cred_b".into()],
            })),
            env(RuntimeEvent::CredentialKeyRotated(CredentialKeyRotated {
                tenant_id: tenant_id.clone(),
                rotation_id: "rot_parity_2".into(),
                old_key_id: "k2".into(),
                new_key_id: "k3".into(),
                credential_ids_rotated: vec!["cred_a".into()],
            })),
        ];
        append_both(&mem, &sqlite_log, &events).await;

        let mem_list = CredentialRotationReadModel::list_rotations(&mem, &tenant_id)
            .await
            .unwrap();
        let sqlite_list = CredentialRotationReadModel::list_rotations(&adapter, &tenant_id)
            .await
            .unwrap();

        assert_eq!(mem_list.len(), 2);
        assert_eq!(sqlite_list.len(), 2);
        // Both backends order by rotated_at ASC (pg/sqlite index) — the
        // in-memory side keeps insertion order. Compare by set of rotation
        // ids to avoid coupling the assertion to a brittle ordering claim.
        let mem_ids: std::collections::BTreeSet<_> =
            mem_list.iter().map(|r| r.rotation_id.clone()).collect();
        let sqlite_ids: std::collections::BTreeSet<_> =
            sqlite_list.iter().map(|r| r.rotation_id.clone()).collect();
        assert_eq!(mem_ids, sqlite_ids);
        for r in &sqlite_list {
            assert_eq!(r.tenant_id, tenant_id);
        }
    }

    // ── RFC-025 Phase 2a.1 milestone 2: tenant-quota projection parity.
    use cairn_domain::{TenantQuotaSet, TenantQuotaViolated};
    use cairn_store::projections::{QuotaReadModel, QuotaViolationReadModel};

    #[tokio::test]
    async fn tenant_quota_set_projection_matches_across_backends() {
        let mem = InMemoryStore::new();
        let adapter = SqliteAdapter::in_memory().await.unwrap();
        let sqlite_log = cairn_store::sqlite::SqliteEventLog::new(adapter.pool().clone());

        let tenant_id = TenantId::new("t_parity_quota");
        let events = vec![
            env(RuntimeEvent::TenantQuotaSet(TenantQuotaSet {
                tenant_id: tenant_id.clone(),
                max_concurrent_runs: 25,
                max_sessions_per_hour: 100,
                max_tasks_per_run: 200,
            })),
            // Upsert: a second set must replace the baseline.
            env(RuntimeEvent::TenantQuotaSet(TenantQuotaSet {
                tenant_id: tenant_id.clone(),
                max_concurrent_runs: 40,
                max_sessions_per_hour: 120,
                max_tasks_per_run: 250,
            })),
        ];
        append_both(&mem, &sqlite_log, &events).await;

        let mem_q = QuotaReadModel::get_quota(&mem, &tenant_id)
            .await
            .unwrap()
            .unwrap();
        let sqlite_q = QuotaReadModel::get_quota(&adapter, &tenant_id)
            .await
            .unwrap()
            .unwrap();

        // Baseline fields must reflect the latest set.
        assert_eq!(mem_q.tenant_id, sqlite_q.tenant_id);
        assert_eq!(mem_q.max_concurrent_runs, 40);
        assert_eq!(sqlite_q.max_concurrent_runs, 40);
        assert_eq!(mem_q.max_sessions_per_hour, 120);
        assert_eq!(sqlite_q.max_sessions_per_hour, 120);
        assert_eq!(mem_q.max_tasks_per_run, 250);
        assert_eq!(sqlite_q.max_tasks_per_run, 250);
        // Dynamic counters are zero with no sessions or runs in either
        // backend.
        assert_eq!(mem_q.current_active_runs, 0);
        assert_eq!(sqlite_q.current_active_runs, 0);
        assert_eq!(mem_q.sessions_this_hour, 0);
        assert_eq!(sqlite_q.sessions_this_hour, 0);
    }

    #[tokio::test]
    async fn tenant_quota_violation_projection_matches_across_backends() {
        let mem = InMemoryStore::new();
        let adapter = SqliteAdapter::in_memory().await.unwrap();
        let sqlite_log = cairn_store::sqlite::SqliteEventLog::new(adapter.pool().clone());

        let tenant_id = TenantId::new("t_parity_viol");
        let events = vec![
            env(RuntimeEvent::TenantQuotaViolated(TenantQuotaViolated {
                tenant_id: tenant_id.clone(),
                quota_type: "max_concurrent_runs".into(),
                current: 10,
                limit: 10,
                occurred_at_ms: 1_000,
            })),
            env(RuntimeEvent::TenantQuotaViolated(TenantQuotaViolated {
                tenant_id: tenant_id.clone(),
                quota_type: "max_sessions_per_hour".into(),
                current: 50,
                limit: 50,
                occurred_at_ms: 2_000,
            })),
        ];
        append_both(&mem, &sqlite_log, &events).await;

        let mem_list = QuotaViolationReadModel::list_violations(&mem, &tenant_id, 100)
            .await
            .unwrap();
        let sqlite_list = QuotaViolationReadModel::list_violations(&adapter, &tenant_id, 100)
            .await
            .unwrap();
        assert_eq!(mem_list.len(), 2);
        assert_eq!(sqlite_list.len(), 2);
        // Most-recent first: occurred_at_ms = 2_000 sorts ahead of 1_000.
        assert_eq!(mem_list[0].occurred_at_ms, 2_000);
        assert_eq!(sqlite_list[0].occurred_at_ms, 2_000);
        assert_eq!(mem_list[0].quota_type, "max_sessions_per_hour");
        assert_eq!(sqlite_list[0].quota_type, "max_sessions_per_hour");
        assert_eq!(mem_list[1].occurred_at_ms, 1_000);
        assert_eq!(sqlite_list[1].occurred_at_ms, 1_000);
        assert_eq!(mem_list, sqlite_list);
    }

    // ── RFC-025 Phase 2a.1 milestone 3: provider-budget projection parity.
    use cairn_domain::providers::ProviderBudgetPeriod;
    use cairn_domain::{ProviderBudgetAlertTriggered, ProviderBudgetExceeded, ProviderBudgetSet};
    use cairn_store::projections::ProviderBudgetReadModel;

    #[tokio::test]
    async fn provider_budget_set_projection_matches_across_backends() {
        let mem = InMemoryStore::new();
        let adapter = SqliteAdapter::in_memory().await.unwrap();
        let sqlite_log = cairn_store::sqlite::SqliteEventLog::new(adapter.pool().clone());

        let tenant_id = TenantId::new("t_parity_budget");
        let events = vec![env(RuntimeEvent::ProviderBudgetSet(ProviderBudgetSet {
            tenant_id: tenant_id.clone(),
            budget_id: "bg_parity_1".into(),
            period: ProviderBudgetPeriod::Monthly,
            limit_micros: 5_000_000,
            alert_threshold_percent: Some(75),
        }))];
        append_both(&mem, &sqlite_log, &events).await;

        let mem_b = ProviderBudgetReadModel::get_by_tenant_period(
            &mem,
            &tenant_id,
            ProviderBudgetPeriod::Monthly,
        )
        .await
        .unwrap()
        .expect("memory budget");
        let sqlite_b = ProviderBudgetReadModel::get_by_tenant_period(
            &adapter,
            &tenant_id,
            ProviderBudgetPeriod::Monthly,
        )
        .await
        .unwrap()
        .expect("sqlite budget");

        assert_eq!(mem_b.tenant_id, sqlite_b.tenant_id);
        assert_eq!(mem_b.period, sqlite_b.period);
        assert_eq!(mem_b.limit_micros, 5_000_000);
        assert_eq!(sqlite_b.limit_micros, 5_000_000);
        assert_eq!(mem_b.alert_threshold_percent, 75);
        assert_eq!(sqlite_b.alert_threshold_percent, 75);
        assert_eq!(mem_b.current_spend_micros, 0);
        assert_eq!(sqlite_b.current_spend_micros, 0);
    }

    #[tokio::test]
    async fn provider_budget_alert_triggered_projection_matches_across_backends() {
        let mem = InMemoryStore::new();
        let adapter = SqliteAdapter::in_memory().await.unwrap();
        let sqlite_log = cairn_store::sqlite::SqliteEventLog::new(adapter.pool().clone());

        let tenant_id = TenantId::new("t_parity_budget_alert");
        let events = vec![
            env(RuntimeEvent::ProviderBudgetSet(ProviderBudgetSet {
                tenant_id: tenant_id.clone(),
                budget_id: "bg_alert_1".into(),
                period: ProviderBudgetPeriod::Daily,
                limit_micros: 1_000_000,
                alert_threshold_percent: Some(80),
            })),
            env(RuntimeEvent::ProviderBudgetAlertTriggered(
                ProviderBudgetAlertTriggered {
                    budget_id: "bg_alert_1".into(),
                    current_micros: 800_000,
                    limit_micros: 1_000_000,
                    triggered_at_ms: 5_000,
                },
            )),
        ];
        append_both(&mem, &sqlite_log, &events).await;

        let mem_b = ProviderBudgetReadModel::get_by_tenant_period(
            &mem,
            &tenant_id,
            ProviderBudgetPeriod::Daily,
        )
        .await
        .unwrap()
        .unwrap();
        let sqlite_b = ProviderBudgetReadModel::get_by_tenant_period(
            &adapter,
            &tenant_id,
            ProviderBudgetPeriod::Daily,
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(mem_b.current_spend_micros, 800_000);
        assert_eq!(sqlite_b.current_spend_micros, 800_000);
    }

    #[tokio::test]
    async fn provider_budget_exceeded_projection_matches_across_backends() {
        let mem = InMemoryStore::new();
        let adapter = SqliteAdapter::in_memory().await.unwrap();
        let sqlite_log = cairn_store::sqlite::SqliteEventLog::new(adapter.pool().clone());

        let tenant_id = TenantId::new("t_parity_budget_excess");
        let events = vec![
            env(RuntimeEvent::ProviderBudgetSet(ProviderBudgetSet {
                tenant_id: tenant_id.clone(),
                budget_id: "bg_excess_1".into(),
                period: ProviderBudgetPeriod::Monthly,
                limit_micros: 2_000_000,
                alert_threshold_percent: None,
            })),
            env(RuntimeEvent::ProviderBudgetExceeded(
                ProviderBudgetExceeded {
                    budget_id: "bg_excess_1".into(),
                    exceeded_by_micros: 500_000,
                    exceeded_at_ms: 9_000,
                },
            )),
        ];
        append_both(&mem, &sqlite_log, &events).await;

        let mem_b = ProviderBudgetReadModel::get_by_tenant_period(
            &mem,
            &tenant_id,
            ProviderBudgetPeriod::Monthly,
        )
        .await
        .unwrap()
        .unwrap();
        let sqlite_b = ProviderBudgetReadModel::get_by_tenant_period(
            &adapter,
            &tenant_id,
            ProviderBudgetPeriod::Monthly,
        )
        .await
        .unwrap()
        .unwrap();

        // current_spend = limit + exceeded_by on both backends.
        assert_eq!(mem_b.current_spend_micros, 2_500_000);
        assert_eq!(sqlite_b.current_spend_micros, 2_500_000);
        // Default threshold is 80 when the event omits it.
        assert_eq!(mem_b.alert_threshold_percent, 80);
        assert_eq!(sqlite_b.alert_threshold_percent, 80);
    }

    // ── RFC-025 Phase 2a.2 milestone 1: approval-delegation parity.
    use cairn_domain::ApprovalDelegated;
    use cairn_store::projections::ApprovalDelegationReadModel;

    #[tokio::test]
    async fn approval_delegation_projection_matches_across_backends() {
        let mem = InMemoryStore::new();
        let adapter = SqliteAdapter::in_memory().await.unwrap();
        let sqlite_log = cairn_store::sqlite::SqliteEventLog::new(adapter.pool().clone());

        let approval_id = ApprovalId::new("ap_parity_delegate");
        let events = vec![
            env(RuntimeEvent::ApprovalDelegated(ApprovalDelegated {
                approval_id: approval_id.clone(),
                delegated_to: "op_first".into(),
                delegated_at_ms: 1_000,
                delegation_id: "deleg_ap_parity_delegate_1000_first".into(),
            })),
            env(RuntimeEvent::ApprovalDelegated(ApprovalDelegated {
                approval_id: approval_id.clone(),
                delegated_to: "op_second".into(),
                delegated_at_ms: 2_000,
                delegation_id: "deleg_ap_parity_delegate_2000_second".into(),
            })),
            // Idempotency probe: replaying the same delegation with the
            // same (approval_id, delegation_id) must be a no-op on both
            // backends (composite PK collapse).
            env(RuntimeEvent::ApprovalDelegated(ApprovalDelegated {
                approval_id: approval_id.clone(),
                delegated_to: "op_first".into(),
                delegated_at_ms: 1_000,
                delegation_id: "deleg_ap_parity_delegate_1000_first".into(),
            })),
        ];
        append_both(&mem, &sqlite_log, &events).await;

        let mem_rows = ApprovalDelegationReadModel::list_for_approval(&mem, &approval_id)
            .await
            .unwrap();
        let sqlite_rows = ApprovalDelegationReadModel::list_for_approval(&adapter, &approval_id)
            .await
            .unwrap();
        assert_eq!(mem_rows.len(), 2);
        assert_eq!(sqlite_rows.len(), 2);
        assert_eq!(mem_rows, sqlite_rows);
        assert_eq!(mem_rows[0].delegated_to, "op_first");
        assert_eq!(mem_rows[1].delegated_to, "op_second");
    }

    // ── RFC-025 Phase 2a.2 milestone 2: guardrail parity ─────────────
    use cairn_domain::policy::{
        GuardrailDecisionKind, GuardrailRule, GuardrailRuleEffect, GuardrailSubjectType,
    };
    use cairn_domain::{GuardrailPolicyCreated, GuardrailPolicyEvaluated};
    use cairn_store::projections::{GuardrailEvaluationReadModel, GuardrailReadModel};

    #[tokio::test]
    async fn guardrail_policy_projection_matches_across_backends() {
        let mem = InMemoryStore::new();
        let adapter = SqliteAdapter::in_memory().await.unwrap();
        let sqlite_log = cairn_store::sqlite::SqliteEventLog::new(adapter.pool().clone());

        let tenant_id = TenantId::new("t_parity_gr_policy");
        let rules = vec![GuardrailRule {
            subject_type: GuardrailSubjectType::Tool,
            subject_id: Some("fs.delete".into()),
            action: "invoke".into(),
            effect: GuardrailRuleEffect::Deny,
            conditions: vec!["cwd=/".into()],
        }];
        let events = vec![env(RuntimeEvent::GuardrailPolicyCreated(
            GuardrailPolicyCreated {
                tenant_id: tenant_id.clone(),
                policy_id: "gr_parity_1".into(),
                name: "delete-fence".into(),
                rules: rules.clone(),
            },
        ))];
        append_both(&mem, &sqlite_log, &events).await;

        let mem_pol = GuardrailReadModel::get_policy(&mem, "gr_parity_1")
            .await
            .unwrap()
            .expect("mem policy");
        let sq_pol = GuardrailReadModel::get_policy(&adapter, "gr_parity_1")
            .await
            .unwrap()
            .expect("sqlite policy");
        assert_eq!(mem_pol.policy_id, sq_pol.policy_id);
        assert_eq!(mem_pol.name, sq_pol.name);
        assert_eq!(mem_pol.rules, sq_pol.rules);
        assert_eq!(mem_pol.enabled, sq_pol.enabled);

        let mem_list = GuardrailReadModel::list_policies(&mem, &tenant_id, 100, 0)
            .await
            .unwrap();
        let sq_list = GuardrailReadModel::list_policies(&adapter, &tenant_id, 100, 0)
            .await
            .unwrap();
        assert_eq!(mem_list.len(), 1);
        assert_eq!(sq_list.len(), 1);
        // Tenant isolation: an unknown tenant must see zero policies on
        // both backends.
        let other = TenantId::new("t_parity_gr_other");
        let mem_other = GuardrailReadModel::list_policies(&mem, &other, 100, 0)
            .await
            .unwrap();
        let sq_other = GuardrailReadModel::list_policies(&adapter, &other, 100, 0)
            .await
            .unwrap();
        assert!(mem_other.is_empty());
        assert!(sq_other.is_empty());
    }

    #[tokio::test]
    async fn guardrail_evaluation_projection_matches_across_backends() {
        let mem = InMemoryStore::new();
        let adapter = SqliteAdapter::in_memory().await.unwrap();
        let sqlite_log = cairn_store::sqlite::SqliteEventLog::new(adapter.pool().clone());

        let tenant_id = TenantId::new("t_parity_gr_eval");
        let events = vec![
            env(RuntimeEvent::GuardrailPolicyEvaluated(
                GuardrailPolicyEvaluated {
                    tenant_id: tenant_id.clone(),
                    policy_id: "gr_eval_1".into(),
                    subject_type: GuardrailSubjectType::Tool,
                    subject_id: Some("fs.delete".into()),
                    action: "invoke".into(),
                    decision: GuardrailDecisionKind::Denied,
                    reason: Some("matched deny rule".into()),
                    evaluated_at_ms: 1_000,
                },
            )),
            // Idempotency probe: composite PK collapse on replay.
            env(RuntimeEvent::GuardrailPolicyEvaluated(
                GuardrailPolicyEvaluated {
                    tenant_id: tenant_id.clone(),
                    policy_id: "gr_eval_1".into(),
                    subject_type: GuardrailSubjectType::Tool,
                    subject_id: Some("fs.delete".into()),
                    action: "invoke".into(),
                    decision: GuardrailDecisionKind::Denied,
                    reason: Some("matched deny rule".into()),
                    evaluated_at_ms: 1_000,
                },
            )),
            env(RuntimeEvent::GuardrailPolicyEvaluated(
                GuardrailPolicyEvaluated {
                    tenant_id: tenant_id.clone(),
                    policy_id: "gr_eval_1".into(),
                    subject_type: GuardrailSubjectType::Tool,
                    subject_id: None,
                    action: "list".into(),
                    decision: GuardrailDecisionKind::Allowed,
                    reason: None,
                    evaluated_at_ms: 2_000,
                },
            )),
        ];
        append_both(&mem, &sqlite_log, &events).await;

        let mem_rows = GuardrailEvaluationReadModel::list_evaluations(&mem, &tenant_id, 100)
            .await
            .unwrap();
        let sq_rows = GuardrailEvaluationReadModel::list_evaluations(&adapter, &tenant_id, 100)
            .await
            .unwrap();
        assert_eq!(mem_rows.len(), 2, "idempotent replay");
        assert_eq!(sq_rows.len(), 2);
        // Most-recent first on both backends.
        assert_eq!(mem_rows[0].evaluated_at_ms, 2_000);
        assert_eq!(sq_rows[0].evaluated_at_ms, 2_000);
        // Subject-id empty-string sentinel round-trips to None.
        assert!(mem_rows[0].subject_id.is_none());
        assert!(sq_rows[0].subject_id.is_none());
        assert_eq!(mem_rows[1].subject_id.as_deref(), Some("fs.delete"));
        assert_eq!(sq_rows[1].subject_id.as_deref(), Some("fs.delete"));
        assert_eq!(mem_rows, sq_rows);
    }

    // ── RFC-025 Phase 2a.2 milestone 3: retention parity ─────────────
    use cairn_domain::RetentionPolicySet;
    use cairn_store::projections::RetentionPolicyReadModel;

    #[tokio::test]
    async fn retention_policy_projection_matches_across_backends() {
        let mem = InMemoryStore::new();
        let adapter = SqliteAdapter::in_memory().await.unwrap();
        let sqlite_log = cairn_store::sqlite::SqliteEventLog::new(adapter.pool().clone());

        let tenant_id = TenantId::new("t_parity_ret");
        let events = vec![
            env(RuntimeEvent::RetentionPolicySet(RetentionPolicySet {
                tenant_id: tenant_id.clone(),
                policy_id: "ret_parity_1".into(),
                full_history_days: 7,
                current_state_days: 30,
                max_events_per_entity: Some(1_000),
            })),
            // Upsert — second set must replace the first on both backends.
            env(RuntimeEvent::RetentionPolicySet(RetentionPolicySet {
                tenant_id: tenant_id.clone(),
                policy_id: "ret_parity_2".into(),
                full_history_days: 14,
                current_state_days: 60,
                max_events_per_entity: None,
            })),
        ];
        append_both(&mem, &sqlite_log, &events).await;

        let mem_pol = RetentionPolicyReadModel::get_by_tenant(&mem, &tenant_id)
            .await
            .unwrap()
            .expect("memory policy");
        let sq_pol = RetentionPolicyReadModel::get_by_tenant(&adapter, &tenant_id)
            .await
            .unwrap()
            .expect("sqlite policy");
        assert_eq!(mem_pol.policy_id, "ret_parity_2");
        assert_eq!(sq_pol.policy_id, "ret_parity_2");
        assert_eq!(mem_pol.full_history_days, 14);
        assert_eq!(sq_pol.full_history_days, 14);
        assert_eq!(mem_pol.current_state_days, 60);
        assert_eq!(sq_pol.current_state_days, 60);
        // Option::None → 0 sentinel on both backends.
        assert_eq!(mem_pol.max_events_per_entity, 0);
        assert_eq!(sq_pol.max_events_per_entity, 0);
    }

    // ── RFC-025 Phase 2a.2 milestone 4: entitlement overrides parity ─

    use cairn_domain::EntitlementOverrideSet;

    #[tokio::test]
    async fn entitlement_override_projection_matches_across_backends() {
        let mem = InMemoryStore::new();
        let adapter = SqliteAdapter::in_memory().await.unwrap();
        let sqlite_log = cairn_store::sqlite::SqliteEventLog::new(adapter.pool().clone());

        let tenant_id = TenantId::new("t_parity_ov");
        let events = vec![
            env(RuntimeEvent::EntitlementOverrideSet(
                EntitlementOverrideSet {
                    tenant_id: tenant_id.clone(),
                    feature: "eval_matrices".into(),
                    allowed: true,
                    reason: Some("pilot".into()),
                    set_at_ms: 1_000,
                },
            )),
            env(RuntimeEvent::EntitlementOverrideSet(
                EntitlementOverrideSet {
                    tenant_id: tenant_id.clone(),
                    feature: "multi_provider".into(),
                    allowed: true,
                    reason: None,
                    set_at_ms: 2_000,
                },
            )),
            // Upsert on (tenant, feature) — second write to
            // `eval_matrices` must replace the first.
            env(RuntimeEvent::EntitlementOverrideSet(
                EntitlementOverrideSet {
                    tenant_id: tenant_id.clone(),
                    feature: "eval_matrices".into(),
                    allowed: false,
                    reason: Some("revoked".into()),
                    set_at_ms: 3_000,
                },
            )),
        ];
        append_both(&mem, &sqlite_log, &events).await;

        let mem_rows = cairn_store::projections::LicenseReadModel::list_overrides(&mem, &tenant_id)
            .await
            .unwrap();
        let sq_rows =
            cairn_store::projections::LicenseReadModel::list_overrides(&adapter, &tenant_id)
                .await
                .unwrap();

        assert_eq!(mem_rows.len(), 2);
        assert_eq!(sq_rows.len(), 2);
        // Both backends sort by `feature` ASC.
        assert_eq!(mem_rows[0].feature, "eval_matrices");
        assert_eq!(sq_rows[0].feature, "eval_matrices");
        assert_eq!(mem_rows[1].feature, "multi_provider");
        assert_eq!(sq_rows[1].feature, "multi_provider");
        // Upsert effect: eval_matrices now allowed = false, reason = revoked,
        // set_at_ms = 3_000 on both backends.
        assert!(!mem_rows[0].allowed);
        assert!(!sq_rows[0].allowed);
        assert_eq!(mem_rows[0].reason.as_deref(), Some("revoked"));
        assert_eq!(sq_rows[0].reason.as_deref(), Some("revoked"));
        assert_eq!(mem_rows[0].set_at_ms, 3_000);
        assert_eq!(sq_rows[0].set_at_ms, 3_000);
        // Byte-equal field-by-field.
        assert_eq!(mem_rows, sq_rows);

        // Different tenant returns empty.
        let other = TenantId::new("t_parity_ov_other");
        let mem_empty = cairn_store::projections::LicenseReadModel::list_overrides(&mem, &other)
            .await
            .unwrap();
        let sq_empty = cairn_store::projections::LicenseReadModel::list_overrides(&adapter, &other)
            .await
            .unwrap();
        assert!(mem_empty.is_empty());
        assert!(sq_empty.is_empty());
    }

    // ── RFC-025 Phase 2a.1 milestone 4: licenses projection parity.
    use cairn_domain::commercial::ProductTier;
    use cairn_domain::LicenseActivated;
    use cairn_store::projections::LicenseReadModel;

    #[tokio::test]
    async fn license_activated_projection_matches_across_backends() {
        let mem = InMemoryStore::new();
        let adapter = SqliteAdapter::in_memory().await.unwrap();
        let sqlite_log = cairn_store::sqlite::SqliteEventLog::new(adapter.pool().clone());

        let tenant_id = TenantId::new("t_parity_lic");
        let events = vec![
            env(RuntimeEvent::LicenseActivated(LicenseActivated {
                tenant_id: tenant_id.clone(),
                license_id: "lic_parity_1".into(),
                tier: ProductTier::TeamSelfHosted,
                valid_from_ms: 1_000,
                valid_until_ms: Some(10_000),
            })),
            // Upsert: a re-activation must replace the row.
            env(RuntimeEvent::LicenseActivated(LicenseActivated {
                tenant_id: tenant_id.clone(),
                license_id: "lic_parity_2".into(),
                tier: ProductTier::EnterpriseSelfHosted,
                valid_from_ms: 2_000,
                valid_until_ms: None,
            })),
        ];
        append_both(&mem, &sqlite_log, &events).await;

        let mem_lic = LicenseReadModel::get_active(&mem, &tenant_id)
            .await
            .unwrap()
            .expect("memory license");
        let sqlite_lic = LicenseReadModel::get_active(&adapter, &tenant_id)
            .await
            .unwrap()
            .expect("sqlite license");

        assert_eq!(mem_lic.tenant_id, sqlite_lic.tenant_id);
        assert_eq!(mem_lic.tier, ProductTier::EnterpriseSelfHosted);
        assert_eq!(sqlite_lic.tier, ProductTier::EnterpriseSelfHosted);
        assert_eq!(mem_lic.issued_at, 2_000);
        assert_eq!(sqlite_lic.issued_at, 2_000);
        assert_eq!(mem_lic.expires_at, None);
        assert_eq!(sqlite_lic.expires_at, None);
        assert_eq!(mem_lic.license_key.as_deref(), Some("lic_parity_2"));
        assert_eq!(sqlite_lic.license_key.as_deref(), Some("lic_parity_2"));
    }

    // ── RFC-025 Phase 3: provider_connections + provider_bindings parity. ───
    use cairn_domain::providers::{
        OperationKind, ProviderBindingSettings, ProviderConnectionStatus, StructuredOutputMode,
    };
    use cairn_domain::tenancy::TenantKey;
    use cairn_domain::{
        ProviderBindingCreated, ProviderBindingId, ProviderBindingStateChanged,
        ProviderConnectionDeleted, ProviderConnectionId, ProviderConnectionRegistered,
        ProviderModelId,
    };
    use cairn_store::projections::{ProviderBindingReadModel, ProviderConnectionReadModel};

    fn provider_settings_fixture() -> ProviderBindingSettings {
        ProviderBindingSettings {
            temperature_milli: Some(700),
            max_output_tokens: Some(4096),
            timeout_ms: Some(30_000),
            structured_output_mode: StructuredOutputMode::Preferred,
            required_capabilities: vec![],
            disabled_capabilities: vec![],
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn provider_connection_registered_projection_matches_across_backends() {
        let mem = InMemoryStore::new();
        let adapter = SqliteAdapter::in_memory().await.unwrap();
        let sqlite_log = cairn_store::sqlite::SqliteEventLog::new(adapter.pool().clone());

        let tenant_id = TenantId::new("t_parity_conn");
        let conn_id = ProviderConnectionId::new("conn_parity_1");
        let events = vec![env(RuntimeEvent::ProviderConnectionRegistered(
            ProviderConnectionRegistered {
                tenant: TenantKey::new(tenant_id.as_str()),
                provider_connection_id: conn_id.clone(),
                provider_family: "openai".to_owned(),
                adapter_type: "responses".to_owned(),
                supported_models: vec!["gpt-4o".to_owned(), "gpt-4o-mini".to_owned()],
                status: ProviderConnectionStatus::Active,
                registered_at: 1_700_000_000_000,
            },
        ))];
        append_both(&mem, &sqlite_log, &events).await;

        let mem_c = ProviderConnectionReadModel::get(&mem, &conn_id)
            .await
            .unwrap()
            .expect("memory connection");
        let sqlite_c = ProviderConnectionReadModel::get(&adapter, &conn_id)
            .await
            .unwrap()
            .expect("sqlite connection");

        assert_eq!(
            mem_c, sqlite_c,
            "connection records diverged across backends"
        );
        assert_eq!(mem_c.provider_family, "openai");
        assert_eq!(mem_c.adapter_type, "responses");
        assert_eq!(mem_c.supported_models, vec!["gpt-4o", "gpt-4o-mini"]);
        assert_eq!(mem_c.status, ProviderConnectionStatus::Active);
        assert_eq!(mem_c.created_at, 1_700_000_000_000);
    }

    #[tokio::test]
    async fn provider_connection_deleted_removes_row_across_backends() {
        let mem = InMemoryStore::new();
        let adapter = SqliteAdapter::in_memory().await.unwrap();
        let sqlite_log = cairn_store::sqlite::SqliteEventLog::new(adapter.pool().clone());

        let tenant_id = TenantId::new("t_parity_conn_del");
        let conn_id = ProviderConnectionId::new("conn_parity_del_1");
        let events = vec![
            env(RuntimeEvent::ProviderConnectionRegistered(
                ProviderConnectionRegistered {
                    tenant: TenantKey::new(tenant_id.as_str()),
                    provider_connection_id: conn_id.clone(),
                    provider_family: "anthropic".to_owned(),
                    adapter_type: "messages".to_owned(),
                    supported_models: vec!["claude-opus-4".to_owned()],
                    status: ProviderConnectionStatus::Active,
                    registered_at: 1_700_000_100_000,
                },
            )),
            env(RuntimeEvent::ProviderConnectionDeleted(
                ProviderConnectionDeleted {
                    tenant: TenantKey::new(tenant_id.as_str()),
                    provider_connection_id: conn_id.clone(),
                    deleted_at: 1_700_000_200_000,
                },
            )),
        ];
        append_both(&mem, &sqlite_log, &events).await;

        // Both backends hard-delete the row (F40: id may be re-created).
        let mem_c = ProviderConnectionReadModel::get(&mem, &conn_id)
            .await
            .unwrap();
        let sqlite_c = ProviderConnectionReadModel::get(&adapter, &conn_id)
            .await
            .unwrap();
        assert!(mem_c.is_none(), "in-memory should hard-delete the row");
        assert!(sqlite_c.is_none(), "sqlite should hard-delete the row");
    }

    #[tokio::test]
    async fn provider_binding_created_projection_matches_across_backends() {
        let mem = InMemoryStore::new();
        let adapter = SqliteAdapter::in_memory().await.unwrap();
        let sqlite_log = cairn_store::sqlite::SqliteEventLog::new(adapter.pool().clone());

        let proj = ProjectKey::new("t_parity_bind", "w_parity_bind", "p_parity_bind");
        let binding_id = ProviderBindingId::new("pb_parity_1");
        let conn_id = ProviderConnectionId::new("conn_parity_bind_1");
        let model_id = ProviderModelId::new("gpt-4o");
        let events = vec![env(RuntimeEvent::ProviderBindingCreated(
            ProviderBindingCreated {
                project: proj.clone(),
                provider_binding_id: binding_id.clone(),
                provider_connection_id: conn_id.clone(),
                provider_model_id: model_id.clone(),
                operation_kind: OperationKind::Generate,
                settings: provider_settings_fixture(),
                policy_id: None,
                active: true,
                created_at: 1_700_000_300_000,
                estimated_cost_micros: None,
            },
        ))];
        append_both(&mem, &sqlite_log, &events).await;

        let mem_b = ProviderBindingReadModel::get(&mem, &binding_id)
            .await
            .unwrap()
            .expect("memory binding");
        let sqlite_b = ProviderBindingReadModel::get(&adapter, &binding_id)
            .await
            .unwrap()
            .expect("sqlite binding");

        assert_eq!(mem_b, sqlite_b, "binding records diverged across backends");
        assert_eq!(mem_b.provider_binding_id, binding_id);
        assert_eq!(mem_b.project, proj);
        assert_eq!(mem_b.provider_connection_id, conn_id);
        assert_eq!(mem_b.provider_model_id, model_id);
        assert_eq!(mem_b.operation_kind, OperationKind::Generate);
        assert!(mem_b.active);
        assert_eq!(mem_b.settings, provider_settings_fixture());
    }

    #[tokio::test]
    async fn provider_binding_state_changed_projection_matches_across_backends() {
        let mem = InMemoryStore::new();
        let adapter = SqliteAdapter::in_memory().await.unwrap();
        let sqlite_log = cairn_store::sqlite::SqliteEventLog::new(adapter.pool().clone());

        let proj = ProjectKey::new(
            "t_parity_bind_state",
            "w_parity_bind_state",
            "p_parity_bind_state",
        );
        let binding_id = ProviderBindingId::new("pb_parity_state_1");
        let events = vec![
            env(RuntimeEvent::ProviderBindingCreated(
                ProviderBindingCreated {
                    project: proj.clone(),
                    provider_binding_id: binding_id.clone(),
                    provider_connection_id: ProviderConnectionId::new("conn_state"),
                    provider_model_id: ProviderModelId::new("gpt-4o"),
                    operation_kind: OperationKind::Embed,
                    settings: provider_settings_fixture(),
                    policy_id: None,
                    active: true,
                    created_at: 1_700_000_400_000,
                    estimated_cost_micros: None,
                },
            )),
            env(RuntimeEvent::ProviderBindingStateChanged(
                ProviderBindingStateChanged {
                    project: proj.clone(),
                    provider_binding_id: binding_id.clone(),
                    active: false,
                    changed_at: 1_700_000_500_000,
                },
            )),
        ];
        append_both(&mem, &sqlite_log, &events).await;

        let mem_b = ProviderBindingReadModel::get(&mem, &binding_id)
            .await
            .unwrap()
            .expect("memory binding");
        let sqlite_b = ProviderBindingReadModel::get(&adapter, &binding_id)
            .await
            .unwrap()
            .expect("sqlite binding");

        assert!(!mem_b.active, "in-memory: StateChanged must flip active");
        assert!(!sqlite_b.active, "sqlite: StateChanged must flip active");
        assert_eq!(
            mem_b, sqlite_b,
            "binding after state change diverged across backends"
        );

        // list_active with the binding's operation must exclude a
        // deactivated binding on both backends.
        let mem_active = ProviderBindingReadModel::list_active(&mem, &proj, OperationKind::Embed)
            .await
            .unwrap();
        let sqlite_active =
            ProviderBindingReadModel::list_active(&adapter, &proj, OperationKind::Embed)
                .await
                .unwrap();
        assert!(mem_active.is_empty(), "in-memory list_active must be empty");
        assert!(sqlite_active.is_empty(), "sqlite list_active must be empty");
    }

    #[tokio::test]
    async fn provider_binding_list_active_sort_matches_across_backends() {
        // Regression guard: the `list_active` tiebreaker must be
        // (created_at ASC, provider_binding_id ASC) across all three
        // backends. Any drift breaks deterministic routing.
        let mem = InMemoryStore::new();
        let adapter = SqliteAdapter::in_memory().await.unwrap();
        let sqlite_log = cairn_store::sqlite::SqliteEventLog::new(adapter.pool().clone());

        let proj = ProjectKey::new("t_sort", "w_sort", "p_sort");

        // Two bindings with the same created_at — ties break on id ASC.
        // A third binding has a later created_at to confirm temporal
        // ordering wins over id ordering.
        let events = vec![
            env(RuntimeEvent::ProviderBindingCreated(
                ProviderBindingCreated {
                    project: proj.clone(),
                    provider_binding_id: ProviderBindingId::new("pb_sort_b"),
                    provider_connection_id: ProviderConnectionId::new("conn_sort"),
                    provider_model_id: ProviderModelId::new("gpt-4o"),
                    operation_kind: OperationKind::Generate,
                    settings: provider_settings_fixture(),
                    policy_id: None,
                    active: true,
                    created_at: 1_700_000_000_000,
                    estimated_cost_micros: None,
                },
            )),
            env(RuntimeEvent::ProviderBindingCreated(
                ProviderBindingCreated {
                    project: proj.clone(),
                    provider_binding_id: ProviderBindingId::new("pb_sort_a"),
                    provider_connection_id: ProviderConnectionId::new("conn_sort"),
                    provider_model_id: ProviderModelId::new("gpt-4o"),
                    operation_kind: OperationKind::Generate,
                    settings: provider_settings_fixture(),
                    policy_id: None,
                    active: true,
                    created_at: 1_700_000_000_000,
                    estimated_cost_micros: None,
                },
            )),
            env(RuntimeEvent::ProviderBindingCreated(
                ProviderBindingCreated {
                    project: proj.clone(),
                    provider_binding_id: ProviderBindingId::new("pb_sort_c"),
                    provider_connection_id: ProviderConnectionId::new("conn_sort"),
                    provider_model_id: ProviderModelId::new("gpt-4o"),
                    operation_kind: OperationKind::Generate,
                    settings: provider_settings_fixture(),
                    policy_id: None,
                    active: true,
                    created_at: 1_700_000_100_000,
                    estimated_cost_micros: None,
                },
            )),
        ];
        append_both(&mem, &sqlite_log, &events).await;

        let mem_list = ProviderBindingReadModel::list_active(&mem, &proj, OperationKind::Generate)
            .await
            .unwrap();
        let sqlite_list =
            ProviderBindingReadModel::list_active(&adapter, &proj, OperationKind::Generate)
                .await
                .unwrap();

        let mem_ids: Vec<&str> = mem_list
            .iter()
            .map(|b| b.provider_binding_id.as_str())
            .collect();
        let sqlite_ids: Vec<&str> = sqlite_list
            .iter()
            .map(|b| b.provider_binding_id.as_str())
            .collect();
        // Expected order: pb_sort_a, pb_sort_b (same created_at; id ASC),
        // then pb_sort_c (later created_at).
        assert_eq!(mem_ids, vec!["pb_sort_a", "pb_sort_b", "pb_sort_c"]);
        assert_eq!(sqlite_ids, vec!["pb_sort_a", "pb_sort_b", "pb_sort_c"]);
    }

    // ── RFC-025 Phase 2b.1 m1: audit_log_entries projection parity. ──

    /// Emit the same `AuditLogEntryRecorded` event into both backends and
    /// assert list_by_tenant / list_by_resource return field-equal rows.
    /// Also checks the trait-documented newest-first ordering — the
    /// in-memory impl pre-Phase-2b.1 returned insertion order, which
    /// drifted from the pg/sqlite `ORDER BY occurred_at_ms DESC`
    /// semantics. Parity here proves the fix landed on every backend.
    #[tokio::test]
    async fn audit_log_projection_matches_across_backends() {
        let mem = InMemoryStore::new();
        let adapter = SqliteAdapter::in_memory().await.unwrap();
        let sqlite_log = cairn_store::sqlite::SqliteEventLog::new(adapter.pool().clone());

        let tenant = TenantId::new("t_audit_parity");
        let events = vec![
            env(RuntimeEvent::AuditLogEntryRecorded(AuditLogEntryRecorded {
                entry_id: "audit_parity_1".to_owned(),
                tenant_id: tenant.clone(),
                actor_id: "op_alice".to_owned(),
                action: "create_tenant".to_owned(),
                resource_type: "tenant".to_owned(),
                resource_id: "t_audit_parity".to_owned(),
                outcome: AuditOutcome::Success,
                occurred_at_ms: 1_700_000_001_000,
            })),
            env(RuntimeEvent::AuditLogEntryRecorded(AuditLogEntryRecorded {
                entry_id: "audit_parity_2".to_owned(),
                tenant_id: tenant.clone(),
                actor_id: "op_bob".to_owned(),
                action: "revoke_credential".to_owned(),
                resource_type: "credential".to_owned(),
                resource_id: "cred_123".to_owned(),
                outcome: AuditOutcome::Failure,
                occurred_at_ms: 1_700_000_002_000,
            })),
        ];
        append_both(&mem, &sqlite_log, &events).await;

        // list_by_tenant — newest-first, both entries in scope.
        let mem_rows = AuditLogReadModel::list_by_tenant(&mem, &tenant, None, None, 10)
            .await
            .unwrap();
        let sqlite_rows = AuditLogReadModel::list_by_tenant(&adapter, &tenant, None, None, 10)
            .await
            .unwrap();
        assert_eq!(mem_rows.len(), 2);
        assert_eq!(sqlite_rows.len(), 2);
        // Newest first: audit_parity_2 (t=2000) then audit_parity_1.
        assert_eq!(mem_rows[0].entry_id, "audit_parity_2");
        assert_eq!(sqlite_rows[0].entry_id, "audit_parity_2");
        assert_eq!(mem_rows[1].entry_id, "audit_parity_1");
        assert_eq!(sqlite_rows[1].entry_id, "audit_parity_1");

        // Field-by-field parity across both rows.
        for idx in 0..2 {
            assert_eq!(mem_rows[idx].entry_id, sqlite_rows[idx].entry_id);
            assert_eq!(mem_rows[idx].tenant_id, sqlite_rows[idx].tenant_id);
            assert_eq!(mem_rows[idx].actor_id, sqlite_rows[idx].actor_id);
            assert_eq!(mem_rows[idx].action, sqlite_rows[idx].action);
            assert_eq!(mem_rows[idx].resource_type, sqlite_rows[idx].resource_type);
            assert_eq!(mem_rows[idx].resource_id, sqlite_rows[idx].resource_id);
            assert_eq!(mem_rows[idx].outcome, sqlite_rows[idx].outcome);
            assert_eq!(
                mem_rows[idx].occurred_at_ms,
                sqlite_rows[idx].occurred_at_ms
            );
            assert_eq!(mem_rows[idx].metadata, sqlite_rows[idx].metadata);
            // `request_id` / `ip_address` are always None (event does
            // not carry them). Verified across both backends.
            assert!(mem_rows[idx].request_id.is_none());
            assert!(sqlite_rows[idx].request_id.is_none());
        }

        // since_ms / before_ms window.
        let windowed =
            AuditLogReadModel::list_by_tenant(&adapter, &tenant, Some(1_700_000_001_500), None, 10)
                .await
                .unwrap();
        assert_eq!(windowed.len(), 1);
        assert_eq!(windowed[0].entry_id, "audit_parity_2");

        // list_by_resource — targets entry 2 only.
        let mem_by_res = AuditLogReadModel::list_by_resource(&mem, "credential", "cred_123")
            .await
            .unwrap();
        let sqlite_by_res = AuditLogReadModel::list_by_resource(&adapter, "credential", "cred_123")
            .await
            .unwrap();
        assert_eq!(mem_by_res.len(), 1);
        assert_eq!(sqlite_by_res.len(), 1);
        assert_eq!(mem_by_res[0].entry_id, "audit_parity_2");
        assert_eq!(sqlite_by_res[0].entry_id, "audit_parity_2");
    }

    // ── RFC-025 Phase 2b.1 m2: scheduled_tasks projection parity. ────

    /// `ScheduledTaskCreated` fired once must produce a field-equal
    /// row across InMemory ↔ SQLite. Covers: defaulted columns
    /// (last_run_at=NULL, updated_at=created_at, enabled=true),
    /// list_by_tenant ordering (created_at ASC), list_due filter.
    #[tokio::test]
    async fn scheduled_task_projection_matches_across_backends() {
        let mem = InMemoryStore::new();
        let adapter = SqliteAdapter::in_memory().await.unwrap();
        let sqlite_log = cairn_store::sqlite::SqliteEventLog::new(adapter.pool().clone());

        let tenant = TenantId::new("t_sched_parity");
        let task_id_1 = ScheduledTaskId::new("sched_parity_1");
        let task_id_2 = ScheduledTaskId::new("sched_parity_2");

        let events = vec![
            env(RuntimeEvent::ScheduledTaskCreated(ScheduledTaskCreated {
                tenant_id: tenant.clone(),
                scheduled_task_id: task_id_1.clone(),
                name: "weekly_reflection".to_owned(),
                cron_expression: "0 9 * * 1".to_owned(),
                next_run_at: Some(1_700_000_100_000),
                created_at: 1_700_000_000_000,
            })),
            env(RuntimeEvent::ScheduledTaskCreated(ScheduledTaskCreated {
                tenant_id: tenant.clone(),
                scheduled_task_id: task_id_2.clone(),
                name: "daily_cleanup".to_owned(),
                cron_expression: "0 2 * * *".to_owned(),
                next_run_at: None,
                created_at: 1_700_000_050_000,
            })),
        ];
        append_both(&mem, &sqlite_log, &events).await;

        // get() parity.
        let mem_row = ScheduledTaskReadModel::get(&mem, &task_id_1)
            .await
            .unwrap()
            .unwrap();
        let sqlite_row = ScheduledTaskReadModel::get(&adapter, &task_id_1)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(mem_row, sqlite_row);
        assert_eq!(mem_row.name, "weekly_reflection");
        assert!(mem_row.enabled);
        assert_eq!(mem_row.last_run_at, None);
        assert_eq!(mem_row.updated_at, mem_row.created_at);

        // list_by_tenant parity (created_at ASC tiebreak).
        let mem_list = ScheduledTaskReadModel::list_by_tenant(&mem, &tenant, 10, 0)
            .await
            .unwrap();
        let sqlite_list = ScheduledTaskReadModel::list_by_tenant(&adapter, &tenant, 10, 0)
            .await
            .unwrap();
        assert_eq!(mem_list, sqlite_list);
        assert_eq!(mem_list[0].scheduled_task_id, task_id_1);
        assert_eq!(mem_list[1].scheduled_task_id, task_id_2);

        // list_due parity — only task_1 has a next_run_at, so bumping
        // `now_ms` past it should surface exactly one record.
        let mem_due = ScheduledTaskReadModel::list_due(&mem, 1_700_000_100_000, 10)
            .await
            .unwrap();
        let sqlite_due = ScheduledTaskReadModel::list_due(&adapter, 1_700_000_100_000, 10)
            .await
            .unwrap();
        assert_eq!(mem_due, sqlite_due);
        assert_eq!(mem_due.len(), 1);
        assert_eq!(mem_due[0].scheduled_task_id, task_id_1);
    }

    // ── RFC-025 Phase 2b.1 m4: plan_reviews projection parity. ──────

    /// `PlanProposed` → `PlanApproved` path: both backends observe the
    /// row transition state=Approved with resolver identity + timestamp.
    #[tokio::test]
    async fn plan_review_proposed_then_approved_matches_across_backends() {
        let mem = InMemoryStore::new();
        let adapter = SqliteAdapter::in_memory().await.unwrap();
        let sqlite_log = cairn_store::sqlite::SqliteEventLog::new(adapter.pool().clone());

        let proj = project();
        let sid = SessionId::new("s_plan_parity");
        let plan_run_id = RunId::new("r_plan_parity_1");
        let events = vec![
            env(RuntimeEvent::PlanProposed(PlanProposed {
                project: proj.clone(),
                plan_run_id: plan_run_id.clone(),
                session_id: sid.clone(),
                plan_markdown: "## Step 1\nDo the thing".to_owned(),
                proposed_at: 1_700_000_001_000,
            })),
            env(RuntimeEvent::PlanApproved(PlanApproved {
                project: proj.clone(),
                plan_run_id: plan_run_id.clone(),
                approved_by: OperatorId::new("op_reviewer"),
                reviewer_comments: Some("LGTM".to_owned()),
                approved_at: 1_700_000_002_000,
            })),
        ];
        append_both(&mem, &sqlite_log, &events).await;

        let mem_row = PlanReviewReadModel::get(&mem, &plan_run_id)
            .await
            .unwrap()
            .unwrap();
        let sqlite_row = PlanReviewReadModel::get(&adapter, &plan_run_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(mem_row, sqlite_row);
        assert_eq!(mem_row.state, PlanReviewState::Approved);
        assert_eq!(mem_row.resolved_by, Some(OperatorId::new("op_reviewer")));
        assert_eq!(mem_row.reviewer_comments.as_deref(), Some("LGTM"));
        assert!(mem_row.rejection_reason.is_none());
        assert!(mem_row.revision_run_id.is_none());
    }

    /// `PlanRejected` populates `rejection_reason` + resolver fields.
    /// The in-memory + sqlite state transitions stay lockstep.
    #[tokio::test]
    async fn plan_review_rejected_matches_across_backends() {
        let mem = InMemoryStore::new();
        let adapter = SqliteAdapter::in_memory().await.unwrap();
        let sqlite_log = cairn_store::sqlite::SqliteEventLog::new(adapter.pool().clone());

        let proj = project();
        let sid = SessionId::new("s_plan_rej");
        let plan_run_id = RunId::new("r_plan_rej_1");
        let events = vec![
            env(RuntimeEvent::PlanProposed(PlanProposed {
                project: proj.clone(),
                plan_run_id: plan_run_id.clone(),
                session_id: sid.clone(),
                plan_markdown: "bad plan".to_owned(),
                proposed_at: 1_700_000_010_000,
            })),
            env(RuntimeEvent::PlanRejected(PlanRejected {
                project: proj.clone(),
                plan_run_id: plan_run_id.clone(),
                rejected_by: OperatorId::new("op_rejector"),
                reason: "missing rollback steps".to_owned(),
                rejected_at: 1_700_000_011_000,
            })),
        ];
        append_both(&mem, &sqlite_log, &events).await;

        let mem_row = PlanReviewReadModel::get(&mem, &plan_run_id)
            .await
            .unwrap()
            .unwrap();
        let sqlite_row = PlanReviewReadModel::get(&adapter, &plan_run_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(mem_row, sqlite_row);
        assert_eq!(mem_row.state, PlanReviewState::Rejected);
        assert_eq!(
            mem_row.rejection_reason.as_deref(),
            Some("missing rollback steps")
        );
    }

    /// `PlanRevisionRequested` creates a new plan run and links the
    /// predecessor via `revision_run_id`. list_by_session walks the
    /// chain oldest-first.
    #[tokio::test]
    async fn plan_review_revision_requested_matches_across_backends() {
        let mem = InMemoryStore::new();
        let adapter = SqliteAdapter::in_memory().await.unwrap();
        let sqlite_log = cairn_store::sqlite::SqliteEventLog::new(adapter.pool().clone());

        let proj = project();
        let sid = SessionId::new("s_plan_rev");
        let original_id = RunId::new("r_plan_rev_orig");
        let new_id = RunId::new("r_plan_rev_new");
        let events = vec![
            env(RuntimeEvent::PlanProposed(PlanProposed {
                project: proj.clone(),
                plan_run_id: original_id.clone(),
                session_id: sid.clone(),
                plan_markdown: "v1".to_owned(),
                proposed_at: 1_700_000_100_000,
            })),
            env(RuntimeEvent::PlanRevisionRequested(PlanRevisionRequested {
                project: proj.clone(),
                original_plan_run_id: original_id.clone(),
                new_plan_run_id: new_id.clone(),
                reviewer_comments: "needs v2".to_owned(),
                requested_at: 1_700_000_101_000,
            })),
            env(RuntimeEvent::PlanProposed(PlanProposed {
                project: proj.clone(),
                plan_run_id: new_id.clone(),
                session_id: sid.clone(),
                plan_markdown: "v2".to_owned(),
                proposed_at: 1_700_000_102_000,
            })),
        ];
        append_both(&mem, &sqlite_log, &events).await;

        let mem_orig = PlanReviewReadModel::get(&mem, &original_id)
            .await
            .unwrap()
            .unwrap();
        let sqlite_orig = PlanReviewReadModel::get(&adapter, &original_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(mem_orig, sqlite_orig);
        assert_eq!(mem_orig.state, PlanReviewState::RevisionRequested);
        assert_eq!(mem_orig.revision_run_id.as_ref(), Some(&new_id));

        // The new plan run sits in Proposed.
        let mem_new = PlanReviewReadModel::get(&mem, &new_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(mem_new.state, PlanReviewState::Proposed);

        // list_by_session surfaces both, oldest-first.
        let mem_chain = PlanReviewReadModel::list_by_session(&mem, &sid, 10)
            .await
            .unwrap();
        let sqlite_chain = PlanReviewReadModel::list_by_session(&adapter, &sid, 10)
            .await
            .unwrap();
        assert_eq!(mem_chain.len(), 2);
        assert_eq!(sqlite_chain.len(), 2);
        assert_eq!(mem_chain[0].plan_run_id, original_id);
        assert_eq!(mem_chain[1].plan_run_id, new_id);
        assert_eq!(sqlite_chain[0].plan_run_id, original_id);
        assert_eq!(sqlite_chain[1].plan_run_id, new_id);
    }

    /// Idempotency + terminal-resolution invariant: a late duplicate
    /// resolution (e.g. PlanRejected applied after PlanApproved) must
    /// NOT overwrite the terminal state. Covers the
    /// `WHERE state = 'proposed'` guard on pg/sqlite and the
    /// `if rec.state == Proposed` guard in-memory.
    #[tokio::test]
    async fn plan_review_late_resolution_does_not_overwrite_terminal() {
        let mem = InMemoryStore::new();
        let adapter = SqliteAdapter::in_memory().await.unwrap();
        let sqlite_log = cairn_store::sqlite::SqliteEventLog::new(adapter.pool().clone());

        let proj = project();
        let sid = SessionId::new("s_plan_late");
        let plan_run_id = RunId::new("r_plan_late_1");
        let events = vec![
            env(RuntimeEvent::PlanProposed(PlanProposed {
                project: proj.clone(),
                plan_run_id: plan_run_id.clone(),
                session_id: sid.clone(),
                plan_markdown: "plan".to_owned(),
                proposed_at: 1_700_000_200_000,
            })),
            env(RuntimeEvent::PlanApproved(PlanApproved {
                project: proj.clone(),
                plan_run_id: plan_run_id.clone(),
                approved_by: OperatorId::new("op_first"),
                reviewer_comments: None,
                approved_at: 1_700_000_201_000,
            })),
            // Late duplicate: should be a no-op on both backends.
            env(RuntimeEvent::PlanRejected(PlanRejected {
                project: proj.clone(),
                plan_run_id: plan_run_id.clone(),
                rejected_by: OperatorId::new("op_second"),
                reason: "too late".to_owned(),
                rejected_at: 1_700_000_202_000,
            })),
        ];
        append_both(&mem, &sqlite_log, &events).await;

        let mem_row = PlanReviewReadModel::get(&mem, &plan_run_id)
            .await
            .unwrap()
            .unwrap();
        let sqlite_row = PlanReviewReadModel::get(&adapter, &plan_run_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(mem_row, sqlite_row);
        // Approved wins — the late reject is dropped by the guard.
        assert_eq!(mem_row.state, PlanReviewState::Approved);
        assert_eq!(mem_row.resolved_by, Some(OperatorId::new("op_first")));
        assert!(mem_row.rejection_reason.is_none());
    }

    // ── RFC-025 Phase 2b.1 m3: outcomes projection parity. ──────────

    /// `OutcomeRecorded` emits produce field-equal rows across InMemory
    /// ↔ SQLite. Compared field-by-field because `OutcomeRecord` does
    /// not derive `PartialEq`.
    #[tokio::test]
    async fn outcome_projection_matches_across_backends() {
        let mem = InMemoryStore::new();
        let adapter = SqliteAdapter::in_memory().await.unwrap();
        let sqlite_log = cairn_store::sqlite::SqliteEventLog::new(adapter.pool().clone());

        let proj = project();
        let run_id = RunId::new("r_outcome_parity");
        let events = vec![
            env(RuntimeEvent::OutcomeRecorded(OutcomeRecorded {
                project: proj.clone(),
                outcome_id: OutcomeId::new("out_parity_1"),
                run_id: run_id.clone(),
                agent_type: "code_review".to_owned(),
                predicted_confidence: 0.85,
                actual_outcome: ActualOutcome::Success,
                recorded_at: 1_700_000_001_000,
            })),
            env(RuntimeEvent::OutcomeRecorded(OutcomeRecorded {
                project: proj.clone(),
                outcome_id: OutcomeId::new("out_parity_2"),
                run_id: run_id.clone(),
                agent_type: "research".to_owned(),
                predicted_confidence: 0.55,
                actual_outcome: ActualOutcome::Partial,
                recorded_at: 1_700_000_002_000,
            })),
            env(RuntimeEvent::OutcomeRecorded(OutcomeRecorded {
                project: proj.clone(),
                outcome_id: OutcomeId::new("out_parity_3"),
                run_id: run_id.clone(),
                agent_type: "planner".to_owned(),
                predicted_confidence: 0.10,
                actual_outcome: ActualOutcome::Failure,
                recorded_at: 1_700_000_003_000,
            })),
        ];
        append_both(&mem, &sqlite_log, &events).await;

        let mem_list = OutcomeReadModel::list_by_run(&mem, &run_id, 10)
            .await
            .unwrap();
        let sqlite_list = OutcomeReadModel::list_by_run(&adapter, &run_id, 10)
            .await
            .unwrap();
        assert_eq!(mem_list.len(), 3);
        assert_eq!(sqlite_list.len(), 3);

        for (i, (m, s)) in mem_list.iter().zip(sqlite_list.iter()).enumerate() {
            assert_eq!(m.outcome_id, s.outcome_id, "row {i}: outcome_id");
            assert_eq!(m.run_id, s.run_id, "row {i}: run_id");
            assert_eq!(m.project, s.project, "row {i}: project");
            assert_eq!(m.agent_type, s.agent_type, "row {i}: agent_type");
            assert!(
                (m.predicted_confidence - s.predicted_confidence).abs() < f64::EPSILON,
                "row {i}: predicted_confidence drift"
            );
            assert_eq!(
                m.actual_outcome, s.actual_outcome,
                "row {i}: actual_outcome"
            );
            assert_eq!(m.recorded_at, s.recorded_at, "row {i}: recorded_at");
        }

        // list_by_project parity — same rows, different filter surface.
        let mem_proj = OutcomeReadModel::list_by_project(&mem, &proj, 10, 0)
            .await
            .unwrap();
        let sqlite_proj = OutcomeReadModel::list_by_project(&adapter, &proj, 10, 0)
            .await
            .unwrap();
        assert_eq!(mem_proj.len(), 3);
        assert_eq!(sqlite_proj.len(), 3);
        assert_eq!(
            mem_proj[0].outcome_id.as_str(),
            sqlite_proj[0].outcome_id.as_str()
        );

        // get() parity on a single id.
        let mem_one = OutcomeReadModel::get(&mem, &OutcomeId::new("out_parity_2"))
            .await
            .unwrap()
            .unwrap();
        let sqlite_one = OutcomeReadModel::get(&adapter, &OutcomeId::new("out_parity_2"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(mem_one.agent_type, sqlite_one.agent_type);
        assert_eq!(mem_one.actual_outcome, sqlite_one.actual_outcome);
    }

    /// Replay safety: re-delivering the same `ScheduledTaskCreated`
    /// must not double-insert. Covers the `ON CONFLICT (scheduled_task_id)
    /// DO NOTHING` clause on pg/sqlite and the `entry().or_insert_with`
    /// on in-memory.
    #[tokio::test]
    async fn scheduled_task_replay_is_idempotent() {
        let mem = InMemoryStore::new();
        let adapter = SqliteAdapter::in_memory().await.unwrap();
        let sqlite_log = cairn_store::sqlite::SqliteEventLog::new(adapter.pool().clone());

        let tenant = TenantId::new("t_sched_replay");
        let task_id = ScheduledTaskId::new("sched_replay_1");
        let payload = ScheduledTaskCreated {
            tenant_id: tenant.clone(),
            scheduled_task_id: task_id.clone(),
            name: "noop".to_owned(),
            cron_expression: "0 0 * * *".to_owned(),
            next_run_at: None,
            created_at: 1_700_000_200_000,
        };
        let events = vec![
            env(RuntimeEvent::ScheduledTaskCreated(payload.clone())),
            env(RuntimeEvent::ScheduledTaskCreated(payload)),
        ];
        append_both(&mem, &sqlite_log, &events).await;

        let mem_list = ScheduledTaskReadModel::list_by_tenant(&mem, &tenant, 10, 0)
            .await
            .unwrap();
        let sqlite_list = ScheduledTaskReadModel::list_by_tenant(&adapter, &tenant, 10, 0)
            .await
            .unwrap();
        assert_eq!(mem_list.len(), 1);
        assert_eq!(sqlite_list.len(), 1);
    }

    /// Idempotency regression: replaying the same `AuditLogEntryRecorded`
    /// twice (as happens during recovery / SSE catch-up) must not
    /// duplicate the row. Covers the `ON CONFLICT (entry_id) DO NOTHING`
    /// clause on pg/sqlite and the `entry().or_insert_with` on in-memory.
    #[tokio::test]
    async fn audit_log_replay_is_idempotent() {
        let mem = InMemoryStore::new();
        let adapter = SqliteAdapter::in_memory().await.unwrap();
        let sqlite_log = cairn_store::sqlite::SqliteEventLog::new(adapter.pool().clone());

        let tenant = TenantId::new("t_audit_replay");
        let payload = AuditLogEntryRecorded {
            entry_id: "audit_replay_1".to_owned(),
            tenant_id: tenant.clone(),
            actor_id: "op".to_owned(),
            action: "noop".to_owned(),
            resource_type: "tenant".to_owned(),
            resource_id: "t_audit_replay".to_owned(),
            outcome: AuditOutcome::Success,
            occurred_at_ms: 1_700_000_003_000,
        };
        // Append twice (different event ids so append accepts both).
        let events = vec![
            env(RuntimeEvent::AuditLogEntryRecorded(payload.clone())),
            env(RuntimeEvent::AuditLogEntryRecorded(payload)),
        ];
        append_both(&mem, &sqlite_log, &events).await;

        let mem_rows = AuditLogReadModel::list_by_tenant(&mem, &tenant, None, None, 10)
            .await
            .unwrap();
        let sqlite_rows = AuditLogReadModel::list_by_tenant(&adapter, &tenant, None, None, 10)
            .await
            .unwrap();
        assert_eq!(
            mem_rows.len(),
            1,
            "in-memory projection must dedupe on entry_id"
        );
        assert_eq!(
            sqlite_rows.len(),
            1,
            "sqlite projection must dedupe on entry_id"
        );
    }

    #[tokio::test]
    async fn provider_binding_replay_of_created_after_state_change_preserves_active() {
        // Idempotency regression: replaying `ProviderBindingCreated` after
        // `ProviderBindingStateChanged` must NOT reset active to the
        // creation-time flag. The ON CONFLICT clause explicitly omits
        // `active` on upsert; this test asserts it.
        let adapter = SqliteAdapter::in_memory().await.unwrap();
        let sqlite_log = cairn_store::sqlite::SqliteEventLog::new(adapter.pool().clone());

        let proj = ProjectKey::new("t_replay", "w_replay", "p_replay");
        let binding_id = ProviderBindingId::new("pb_replay_1");
        let created_once = RuntimeEvent::ProviderBindingCreated(ProviderBindingCreated {
            project: proj.clone(),
            provider_binding_id: binding_id.clone(),
            provider_connection_id: ProviderConnectionId::new("conn_replay"),
            provider_model_id: ProviderModelId::new("gpt-4o"),
            operation_kind: OperationKind::Generate,
            settings: provider_settings_fixture(),
            policy_id: None,
            active: true,
            created_at: 1_700_000_600_000,
            estimated_cost_micros: None,
        });
        let events = vec![
            env(created_once.clone()),
            env(RuntimeEvent::ProviderBindingStateChanged(
                ProviderBindingStateChanged {
                    project: proj.clone(),
                    provider_binding_id: binding_id.clone(),
                    active: false,
                    changed_at: 1_700_000_700_000,
                },
            )),
            // Replay the creation event (different envelope id so append
            // accepts it). On pg/sqlite this triggers the ON CONFLICT DO
            // UPDATE path; `active` must stay false.
            env(created_once),
        ];
        sqlite_log.append(&events).await.expect("sqlite append");

        let binding = ProviderBindingReadModel::get(&adapter, &binding_id)
            .await
            .unwrap()
            .expect("binding present");
        assert!(
            !binding.active,
            "replay of ProviderBindingCreated after StateChanged must preserve active=false"
        );
    }

    // ── RFC-025 Phase 2b.2 m1: external_workers projection parity. ──

    /// Sentinel ProjectKey used on tenant-scoped `ExternalWorker*` events
    /// (mirrors the helper in `external_worker_lifecycle.rs`). Worker
    /// events are tenant-scoped so the event carries a placeholder
    /// `(tenant, "_", "_")` triplet rather than a real project.
    fn sentinel(tenant: &str) -> ProjectKey {
        ProjectKey::new(tenant, "_", "_")
    }

    /// Full lifecycle (Register → Heartbeat → Suspend → Reactivate) on
    /// both backends must yield a field-equal record. Covers the
    /// canonicalisation/clearing semantics exercised by the applier:
    /// status reset to "active" on registration replay, heartbeat
    /// setting `is_alive` + `current_task_id`, suspension toggling
    /// `status`, and reactivation restoring `status = "active"`.
    #[tokio::test]
    async fn external_worker_full_lifecycle_matches_across_backends() {
        let mem = InMemoryStore::new();
        let adapter = SqliteAdapter::in_memory().await.unwrap();
        let sqlite_log = cairn_store::sqlite::SqliteEventLog::new(adapter.pool().clone());

        let tenant = TenantId::new("t_worker_parity");
        let worker_id = WorkerId::new("w_parity_1");

        let events = vec![
            env(RuntimeEvent::ExternalWorkerRegistered(
                ExternalWorkerRegistered {
                    sentinel_project: sentinel("t_worker_parity"),
                    worker_id: worker_id.clone(),
                    tenant_id: tenant.clone(),
                    display_name: "Parity Bot".to_owned(),
                    registered_at: 1_700_000_000_000,
                },
            )),
            env(RuntimeEvent::ExternalWorkerReported(
                ExternalWorkerReported {
                    report: ExternalWorkerReport {
                        project: sentinel("t_worker_parity"),
                        worker_id: worker_id.clone(),
                        run_id: None,
                        task_id: TaskId::new("task_parity_1"),
                        lease_token: 1,
                        reported_at_ms: 1_700_000_001_000,
                        progress: None,
                        outcome: None,
                    },
                },
            )),
            env(RuntimeEvent::ExternalWorkerSuspended(
                ExternalWorkerSuspended {
                    sentinel_project: sentinel("t_worker_parity"),
                    worker_id: worker_id.clone(),
                    tenant_id: tenant.clone(),
                    suspended_at: 1_700_000_002_000,
                    reason: Some("maintenance".to_owned()),
                },
            )),
            env(RuntimeEvent::ExternalWorkerReactivated(
                ExternalWorkerReactivated {
                    sentinel_project: sentinel("t_worker_parity"),
                    worker_id: worker_id.clone(),
                    tenant_id: tenant.clone(),
                    reactivated_at: 1_700_000_003_000,
                },
            )),
        ];
        append_both(&mem, &sqlite_log, &events).await;

        // get() parity — every field except `updated_at` (wall-clock at
        // applier-run time differs across the two appends, which is
        // expected and documented in the applier comments).
        let mem_row = ExternalWorkerReadModel::get(&mem, &worker_id)
            .await
            .unwrap()
            .unwrap();
        let sqlite_row = ExternalWorkerReadModel::get(&adapter, &worker_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(mem_row.worker_id, sqlite_row.worker_id);
        assert_eq!(mem_row.tenant_id, sqlite_row.tenant_id);
        assert_eq!(mem_row.display_name, sqlite_row.display_name);
        assert_eq!(mem_row.status, sqlite_row.status);
        assert_eq!(mem_row.registered_at, sqlite_row.registered_at);
        assert_eq!(
            mem_row.health.last_heartbeat_ms,
            sqlite_row.health.last_heartbeat_ms
        );
        assert_eq!(mem_row.health.is_alive, sqlite_row.health.is_alive);
        assert_eq!(
            mem_row.health.active_task_count,
            sqlite_row.health.active_task_count
        );
        assert_eq!(mem_row.current_task_id, sqlite_row.current_task_id);

        // Terminal state: reactivated so status is "active"; last
        // heartbeat preserves is_alive=true; current_task_id is still
        // set (no terminal outcome reported on task_parity_1).
        assert_eq!(mem_row.status, "active");
        assert!(mem_row.health.is_alive);
        assert_eq!(mem_row.health.last_heartbeat_ms, 1_700_000_001_000);
        assert_eq!(mem_row.current_task_id, Some(TaskId::new("task_parity_1")));
    }

    /// Heartbeat with terminal outcome must clear `current_task_id` on
    /// both backends (mirrors the `outcome.is_none()` branch in the
    /// applier). Regression: an earlier draft unconditionally set
    /// `current_task_id = Some(..)`, which left stale task pointers on
    /// completed workers.
    #[tokio::test]
    async fn external_worker_terminal_report_clears_current_task_across_backends() {
        use cairn_domain::workers::ExternalWorkerOutcome;

        let mem = InMemoryStore::new();
        let adapter = SqliteAdapter::in_memory().await.unwrap();
        let sqlite_log = cairn_store::sqlite::SqliteEventLog::new(adapter.pool().clone());

        let tenant = TenantId::new("t_term_parity");
        let worker_id = WorkerId::new("w_term_parity");

        let events = vec![
            env(RuntimeEvent::ExternalWorkerRegistered(
                ExternalWorkerRegistered {
                    sentinel_project: sentinel("t_term_parity"),
                    worker_id: worker_id.clone(),
                    tenant_id: tenant.clone(),
                    display_name: "Finisher".to_owned(),
                    registered_at: 1_700_000_100_000,
                },
            )),
            // Heartbeat with an active report — sets current_task_id.
            env(RuntimeEvent::ExternalWorkerReported(
                ExternalWorkerReported {
                    report: ExternalWorkerReport {
                        project: sentinel("t_term_parity"),
                        worker_id: worker_id.clone(),
                        run_id: None,
                        task_id: TaskId::new("task_term_1"),
                        lease_token: 1,
                        reported_at_ms: 1_700_000_101_000,
                        progress: None,
                        outcome: None,
                    },
                },
            )),
            // Terminal report — must clear current_task_id on both backends.
            env(RuntimeEvent::ExternalWorkerReported(
                ExternalWorkerReported {
                    report: ExternalWorkerReport {
                        project: sentinel("t_term_parity"),
                        worker_id: worker_id.clone(),
                        run_id: None,
                        task_id: TaskId::new("task_term_1"),
                        lease_token: 1,
                        reported_at_ms: 1_700_000_102_000,
                        progress: None,
                        outcome: Some(ExternalWorkerOutcome::Completed),
                    },
                },
            )),
        ];
        append_both(&mem, &sqlite_log, &events).await;

        let mem_row = ExternalWorkerReadModel::get(&mem, &worker_id)
            .await
            .unwrap()
            .unwrap();
        let sqlite_row = ExternalWorkerReadModel::get(&adapter, &worker_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(mem_row.current_task_id, None);
        assert_eq!(sqlite_row.current_task_id, None);
        assert_eq!(
            mem_row.health.last_heartbeat_ms,
            sqlite_row.health.last_heartbeat_ms
        );
        assert_eq!(mem_row.health.is_alive, sqlite_row.health.is_alive);
    }

    /// `list_by_tenant` ordering must match across backends under
    /// same-millisecond `registered_at` collisions. Tiebreaker is
    /// ASCENDING `worker_id` (asserted in both adapters + the
    /// in-memory impl).
    #[tokio::test]
    async fn external_worker_list_by_tenant_ordering_matches_across_backends() {
        let mem = InMemoryStore::new();
        let adapter = SqliteAdapter::in_memory().await.unwrap();
        let sqlite_log = cairn_store::sqlite::SqliteEventLog::new(adapter.pool().clone());

        let tenant = TenantId::new("t_list_parity");
        // Same `registered_at` forces the worker_id tiebreak path.
        let events = vec![
            env(RuntimeEvent::ExternalWorkerRegistered(
                ExternalWorkerRegistered {
                    sentinel_project: sentinel("t_list_parity"),
                    worker_id: WorkerId::new("w_list_c"),
                    tenant_id: tenant.clone(),
                    display_name: "C".to_owned(),
                    registered_at: 1_700_000_500_000,
                },
            )),
            env(RuntimeEvent::ExternalWorkerRegistered(
                ExternalWorkerRegistered {
                    sentinel_project: sentinel("t_list_parity"),
                    worker_id: WorkerId::new("w_list_a"),
                    tenant_id: tenant.clone(),
                    display_name: "A".to_owned(),
                    registered_at: 1_700_000_500_000,
                },
            )),
            env(RuntimeEvent::ExternalWorkerRegistered(
                ExternalWorkerRegistered {
                    sentinel_project: sentinel("t_list_parity"),
                    worker_id: WorkerId::new("w_list_b"),
                    tenant_id: tenant.clone(),
                    display_name: "B".to_owned(),
                    registered_at: 1_700_000_500_000,
                },
            )),
        ];
        append_both(&mem, &sqlite_log, &events).await;

        let mem_list = ExternalWorkerReadModel::list_by_tenant(&mem, &tenant, 10, 0)
            .await
            .unwrap();
        let sqlite_list = ExternalWorkerReadModel::list_by_tenant(&adapter, &tenant, 10, 0)
            .await
            .unwrap();
        assert_eq!(mem_list.len(), 3);
        assert_eq!(sqlite_list.len(), 3);
        let mem_ids: Vec<_> = mem_list.iter().map(|r| r.worker_id.as_str()).collect();
        let sqlite_ids: Vec<_> = sqlite_list.iter().map(|r| r.worker_id.as_str()).collect();
        assert_eq!(mem_ids, sqlite_ids, "ordering must match across backends");
        // The two backends must agree on the in-memory order, which is
        // ASCENDING worker_id under same-ms registered_at.
        assert_eq!(mem_ids, vec!["w_list_a", "w_list_b", "w_list_c"]);

        // Pagination parity: `limit=2, offset=1` must return the same
        // two rows on both backends.
        let mem_page = ExternalWorkerReadModel::list_by_tenant(&mem, &tenant, 2, 1)
            .await
            .unwrap();
        let sqlite_page = ExternalWorkerReadModel::list_by_tenant(&adapter, &tenant, 2, 1)
            .await
            .unwrap();
        let mem_page_ids: Vec<_> = mem_page.iter().map(|r| r.worker_id.as_str()).collect();
        let sqlite_page_ids: Vec<_> = sqlite_page.iter().map(|r| r.worker_id.as_str()).collect();
        assert_eq!(mem_page_ids, sqlite_page_ids);
        assert_eq!(mem_page_ids, vec!["w_list_b", "w_list_c"]);
    }

    /// Re-registration parity (Copilot #580 regression): the in-memory
    /// applier overwrites the entire record on `ExternalWorkerRegistered`
    /// — health resets to `WorkerHealth::default()` (is_alive=false,
    /// last_heartbeat_ms=0, active_task_count=0) and current_task_id
    /// returns to `None`. The pg/sqlite appliers must match that
    /// semantic on `ON CONFLICT ... DO UPDATE` or persistent backends
    /// silently diverge from InMemory. This test registers a worker,
    /// heartbeats it (setting is_alive + current_task_id), then
    /// re-registers and asserts every field is back to its zero-value
    /// on both backends.
    #[tokio::test]
    async fn external_worker_re_registration_resets_health_across_backends() {
        let mem = InMemoryStore::new();
        let adapter = SqliteAdapter::in_memory().await.unwrap();
        let sqlite_log = cairn_store::sqlite::SqliteEventLog::new(adapter.pool().clone());

        let tenant = TenantId::new("t_rereg");
        let worker_id = WorkerId::new("w_rereg_1");

        let events = vec![
            env(RuntimeEvent::ExternalWorkerRegistered(
                ExternalWorkerRegistered {
                    sentinel_project: sentinel("t_rereg"),
                    worker_id: worker_id.clone(),
                    tenant_id: tenant.clone(),
                    display_name: "Initial Name".to_owned(),
                    registered_at: 1_700_000_000_000,
                },
            )),
            env(RuntimeEvent::ExternalWorkerReported(
                ExternalWorkerReported {
                    report: ExternalWorkerReport {
                        project: sentinel("t_rereg"),
                        worker_id: worker_id.clone(),
                        run_id: None,
                        task_id: TaskId::new("task_pre_rereg"),
                        lease_token: 1,
                        reported_at_ms: 1_700_000_001_000,
                        progress: None,
                        outcome: None,
                    },
                },
            )),
            env(RuntimeEvent::ExternalWorkerSuspended(
                ExternalWorkerSuspended {
                    sentinel_project: sentinel("t_rereg"),
                    worker_id: worker_id.clone(),
                    tenant_id: tenant.clone(),
                    suspended_at: 1_700_000_002_000,
                    reason: None,
                },
            )),
            // Re-registration — must reset the whole record.
            env(RuntimeEvent::ExternalWorkerRegistered(
                ExternalWorkerRegistered {
                    sentinel_project: sentinel("t_rereg"),
                    worker_id: worker_id.clone(),
                    tenant_id: tenant.clone(),
                    display_name: "Renamed Bot".to_owned(),
                    registered_at: 1_700_000_003_000,
                },
            )),
        ];
        append_both(&mem, &sqlite_log, &events).await;

        let mem_row = ExternalWorkerReadModel::get(&mem, &worker_id)
            .await
            .unwrap()
            .unwrap();
        let sqlite_row = ExternalWorkerReadModel::get(&adapter, &worker_id)
            .await
            .unwrap()
            .unwrap();

        // Latest registration wins on all four canonicalised columns.
        assert_eq!(mem_row.display_name, "Renamed Bot");
        assert_eq!(sqlite_row.display_name, "Renamed Bot");
        assert_eq!(mem_row.status, "active", "re-register forces active");
        assert_eq!(sqlite_row.status, "active");
        assert_eq!(mem_row.registered_at, 1_700_000_003_000);
        assert_eq!(sqlite_row.registered_at, 1_700_000_003_000);

        // Health + current_task_id reset on both backends (the Copilot
        // #580 catch — pg/sqlite previously left these stale).
        assert!(
            !mem_row.health.is_alive,
            "in-memory re-register resets is_alive"
        );
        assert!(
            !sqlite_row.health.is_alive,
            "sqlite re-register must reset is_alive"
        );
        assert_eq!(mem_row.health.last_heartbeat_ms, 0);
        assert_eq!(sqlite_row.health.last_heartbeat_ms, 0);
        assert_eq!(mem_row.health.active_task_count, 0);
        assert_eq!(sqlite_row.health.active_task_count, 0);
        assert_eq!(mem_row.current_task_id, None);
        assert_eq!(
            sqlite_row.current_task_id, None,
            "sqlite re-register must clear current_task_id"
        );
    }

    // ── RFC-025 Phase 2b.2b m1: resource_shares parity (RFC 008) ──

    /// Share → list → get_share_for_resource → revoke lifecycle must
    /// agree byte-for-byte across in-memory and sqlite. Revocation
    /// DELETEs the row, so after revoke every backend must return None.
    #[tokio::test]
    async fn resource_share_lifecycle_matches_across_backends() {
        let mem = InMemoryStore::new();
        let adapter = SqliteAdapter::in_memory().await.unwrap();
        let sqlite_log = cairn_store::sqlite::SqliteEventLog::new(adapter.pool().clone());

        let tenant = TenantId::new("t_share");
        let src = WorkspaceId::new("ws_src");
        let tgt = WorkspaceId::new("ws_tgt");

        let events = vec![
            env(RuntimeEvent::ResourceShared(ResourceShared {
                share_id: "share_parity_1".to_owned(),
                tenant_id: tenant.clone(),
                source_workspace_id: src.clone(),
                target_workspace_id: tgt.clone(),
                resource_type: "prompt_asset".to_owned(),
                resource_id: "asset_alpha".to_owned(),
                permissions: vec!["read".to_owned(), "version".to_owned()],
                grantee: String::new(),
                shared_at_ms: 1_700_100_000_000,
            })),
            env(RuntimeEvent::ResourceShared(ResourceShared {
                share_id: "share_parity_2".to_owned(),
                tenant_id: tenant.clone(),
                source_workspace_id: src.clone(),
                target_workspace_id: tgt.clone(),
                resource_type: "corpus".to_owned(),
                resource_id: "corpus_beta".to_owned(),
                permissions: vec![],
                grantee: String::new(),
                shared_at_ms: 1_700_100_001_000,
            })),
        ];
        append_both(&mem, &sqlite_log, &events).await;

        // get_share parity.
        let mem_row = ResourceSharingReadModel::get_share(&mem, "share_parity_1")
            .await
            .unwrap()
            .unwrap();
        let sqlite_row = ResourceSharingReadModel::get_share(&adapter, "share_parity_1")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(mem_row, sqlite_row);
        assert_eq!(
            mem_row.permissions,
            vec!["read".to_owned(), "version".to_owned()]
        );

        // list_shares_for_workspace parity — sorted by (shared_at_ms, share_id).
        let mem_list = ResourceSharingReadModel::list_shares_for_workspace(&mem, &tenant, &tgt)
            .await
            .unwrap();
        let sqlite_list =
            ResourceSharingReadModel::list_shares_for_workspace(&adapter, &tenant, &tgt)
                .await
                .unwrap();
        assert_eq!(mem_list.len(), 2);
        assert_eq!(mem_list, sqlite_list);
        assert_eq!(mem_list[0].share_id, "share_parity_1");
        assert_eq!(mem_list[1].share_id, "share_parity_2");

        // get_share_for_resource parity.
        let mem_asset = ResourceSharingReadModel::get_share_for_resource(
            &mem,
            &tenant,
            &tgt,
            "prompt_asset",
            "asset_alpha",
        )
        .await
        .unwrap();
        let sqlite_asset = ResourceSharingReadModel::get_share_for_resource(
            &adapter,
            &tenant,
            &tgt,
            "prompt_asset",
            "asset_alpha",
        )
        .await
        .unwrap();
        assert_eq!(mem_asset, sqlite_asset);
        assert_eq!(
            mem_asset.as_ref().map(|s| s.share_id.as_str()),
            Some("share_parity_1")
        );

        // Revoke share_parity_1 — row DELETEd on both backends.
        let revoke = vec![env(RuntimeEvent::ResourceShareRevoked(
            ResourceShareRevoked {
                share_id: "share_parity_1".to_owned(),
                tenant_id: tenant.clone(),
                revoked_at_ms: 1_700_100_002_000,
            },
        ))];
        append_both(&mem, &sqlite_log, &revoke).await;

        let mem_after = ResourceSharingReadModel::get_share(&mem, "share_parity_1")
            .await
            .unwrap();
        let sqlite_after = ResourceSharingReadModel::get_share(&adapter, "share_parity_1")
            .await
            .unwrap();
        assert_eq!(mem_after, None);
        assert_eq!(sqlite_after, None);

        // list remains with just share_parity_2.
        let mem_list2 = ResourceSharingReadModel::list_shares_for_workspace(&mem, &tenant, &tgt)
            .await
            .unwrap();
        let sqlite_list2 =
            ResourceSharingReadModel::list_shares_for_workspace(&adapter, &tenant, &tgt)
                .await
                .unwrap();
        assert_eq!(mem_list2.len(), 1);
        assert_eq!(mem_list2, sqlite_list2);
        assert_eq!(mem_list2[0].share_id, "share_parity_2");
    }

    /// Pin the observed `Shared → Revoked → (replayed Shared)` cross-
    /// backend contract. Revocation is NOT terminal under event-log
    /// replay on any backend:
    ///   * in-memory: `HashMap::insert` on the replayed Shared event
    ///     re-creates the row (the prior Revoke's `remove` cleared it);
    ///   * pg/sqlite: the applier uses `ON CONFLICT (share_id) DO
    ///     NOTHING`, but the DELETE from the prior Revoke means the
    ///     conflict target is empty, so the INSERT succeeds and the
    ///     row is back.
    ///
    /// The test asserts all three backends agree on the final row
    /// presence (all re-created) so a future applier change that
    /// silently drifts one backend from the other is caught. The
    /// service layer allocates a fresh `share_id` per live share call
    /// (`next_share_id()`), so the replay path is reachable only by
    /// boot-time event-log walking. If we ever want "revocation is
    /// terminal" semantics, we'd need a tombstone table or a
    /// dedicated revoked state column — a design change out of scope
    /// for Phase 2b.2b.
    #[tokio::test]
    async fn resource_share_replay_after_revoke_recreates_on_all_backends() {
        let mem = InMemoryStore::new();
        let adapter = SqliteAdapter::in_memory().await.unwrap();
        let sqlite_log = cairn_store::sqlite::SqliteEventLog::new(adapter.pool().clone());

        let tenant = TenantId::new("t_replay");
        let src = WorkspaceId::new("ws_src_replay");
        let tgt = WorkspaceId::new("ws_tgt_replay");

        // The in-memory applier's `remove(&share_id)` idempotently
        // clears the row; a replayed Shared *would* re-insert there if
        // the applier were purely additive, so this test also pins
        // the in-memory contract.
        //
        // For this scenario, we need to exercise the "replay" path
        // deliberately. The cleanest way is to append Shared + Revoked
        // together, then append a second Shared with the same
        // share_id — the service layer would never do this (share_id
        // is sequence-backed and unique), but the event-log replay
        // path effectively does when a snapshot-less boot walks every
        // event in order.
        let events = vec![
            env(RuntimeEvent::ResourceShared(ResourceShared {
                share_id: "share_replay_1".to_owned(),
                tenant_id: tenant.clone(),
                source_workspace_id: src.clone(),
                target_workspace_id: tgt.clone(),
                resource_type: "prompt_asset".to_owned(),
                resource_id: "asset_replay".to_owned(),
                permissions: vec!["read".to_owned()],
                grantee: String::new(),
                shared_at_ms: 1_700_200_000_000,
            })),
            env(RuntimeEvent::ResourceShareRevoked(ResourceShareRevoked {
                share_id: "share_replay_1".to_owned(),
                tenant_id: tenant.clone(),
                revoked_at_ms: 1_700_200_001_000,
            })),
            // Replayed Shared — same share_id. In-memory: insert
            // re-creates the row. Pg/sqlite ON CONFLICT DO NOTHING
            // would also re-create since the row is gone. This is a
            // known replay asymmetry — the test pins the OBSERVED
            // behaviour so future appliers don't silently drift.
            env(RuntimeEvent::ResourceShared(ResourceShared {
                share_id: "share_replay_1".to_owned(),
                tenant_id: tenant.clone(),
                source_workspace_id: src.clone(),
                target_workspace_id: tgt.clone(),
                resource_type: "prompt_asset".to_owned(),
                resource_id: "asset_replay".to_owned(),
                permissions: vec!["read".to_owned()],
                grantee: String::new(),
                shared_at_ms: 1_700_200_002_000,
            })),
        ];
        append_both(&mem, &sqlite_log, &events).await;

        // Both backends re-create the row on the second Shared (the
        // Revoke DELETEd it, so ON CONFLICT does not fire). Cross-
        // backend parity: either both see the row or neither does.
        let mem_row = ResourceSharingReadModel::get_share(&mem, "share_replay_1")
            .await
            .unwrap();
        let sqlite_row = ResourceSharingReadModel::get_share(&adapter, "share_replay_1")
            .await
            .unwrap();
        assert_eq!(
            mem_row.is_some(),
            sqlite_row.is_some(),
            "pg/sqlite/in-memory must agree on final row presence"
        );
        assert_eq!(mem_row, sqlite_row);
    }

    // ── RFC-025 Phase 2b.2b m2: signal_ingestions parity ──

    /// Ingest a signal with a JSON object payload and assert
    /// `SignalReadModel::get` + `list_by_project` return the same
    /// record across in-memory and sqlite. Repeats the same signal_id
    /// a second time to exercise the ON CONFLICT DO NOTHING idempotency
    /// path (the persistent backends keep the first row; in-memory
    /// `HashMap::insert` overwrites — but with the same event payload
    /// the resulting record is identical, so parity still holds).
    #[tokio::test]
    async fn signal_ingestion_projection_matches_across_backends() {
        let mem = InMemoryStore::new();
        let adapter = SqliteAdapter::in_memory().await.unwrap();
        let sqlite_log = cairn_store::sqlite::SqliteEventLog::new(adapter.pool().clone());

        let scope = ProjectKey::new("t_sig", "w_sig", "p_sig");
        let other_scope = ProjectKey::new("t_sig", "w_sig", "p_other");

        let events = vec![
            env(RuntimeEvent::SignalIngested(SignalIngested {
                project: scope.clone(),
                signal_id: SignalId::new("sig_parity_1"),
                source: "webhook".into(),
                payload: serde_json::json!({"kind": "push", "ref": "main"}),
                timestamp_ms: 1_700_300_000_000,
            })),
            env(RuntimeEvent::SignalIngested(SignalIngested {
                project: scope.clone(),
                signal_id: SignalId::new("sig_parity_2"),
                source: "trigger".into(),
                payload: serde_json::Value::Null,
                timestamp_ms: 1_700_300_001_000,
            })),
            // Out-of-project signal — must be filtered out by list_by_project.
            env(RuntimeEvent::SignalIngested(SignalIngested {
                project: other_scope.clone(),
                signal_id: SignalId::new("sig_parity_other"),
                source: "webhook".into(),
                payload: serde_json::json!({}),
                timestamp_ms: 1_700_300_002_000,
            })),
        ];
        append_both(&mem, &sqlite_log, &events).await;

        // get() parity.
        let mem_row = SignalReadModel::get(&mem, &SignalId::new("sig_parity_1"))
            .await
            .unwrap()
            .unwrap();
        let sqlite_row = SignalReadModel::get(&adapter, &SignalId::new("sig_parity_1"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(mem_row, sqlite_row);
        assert_eq!(mem_row.source, "webhook");
        assert_eq!(mem_row.payload["kind"], "push");

        // list_by_project parity — sorted by (timestamp_ms, signal_id).
        // Filters out the other_scope entry.
        let mem_list = SignalReadModel::list_by_project(&mem, &scope, 10, 0)
            .await
            .unwrap();
        let sqlite_list = SignalReadModel::list_by_project(&adapter, &scope, 10, 0)
            .await
            .unwrap();
        assert_eq!(mem_list.len(), 2);
        assert_eq!(mem_list, sqlite_list);
        assert_eq!(mem_list[0].id.as_str(), "sig_parity_1");
        assert_eq!(mem_list[1].id.as_str(), "sig_parity_2");

        // Pagination: offset=1, limit=1 yields the second row only.
        let mem_page = SignalReadModel::list_by_project(&mem, &scope, 1, 1)
            .await
            .unwrap();
        let sqlite_page = SignalReadModel::list_by_project(&adapter, &scope, 1, 1)
            .await
            .unwrap();
        assert_eq!(mem_page, sqlite_page);
        assert_eq!(mem_page.len(), 1);
        assert_eq!(mem_page[0].id.as_str(), "sig_parity_2");
    }

    // ── RFC-025 Phase 2b.2b m3: subagent_spawns parity (RFC 014) ──

    /// Spawn a child subagent and assert:
    /// * pg/sqlite/in-memory agree on get_by_child_task (all fields
    ///   except spawned_at_ms which is wall-clock at applier-run time,
    ///   mirroring the `external_worker_full_lifecycle` pattern);
    /// * list_by_parent_run returns the spawn ordered by
    ///   (spawned_at_ms, child_task_id) with optional parent_task_id
    ///   and child_run_id preserved across backends.
    #[tokio::test]
    async fn subagent_spawn_projection_matches_across_backends() {
        let mem = InMemoryStore::new();
        let adapter = SqliteAdapter::in_memory().await.unwrap();
        let sqlite_log = cairn_store::sqlite::SqliteEventLog::new(adapter.pool().clone());

        let scope = ProjectKey::new("t_sub", "w_sub", "p_sub");
        let parent_run = RunId::new("run_parent_sub");
        let child_task = TaskId::new("task_child_sub_1");
        let child_session = SessionId::new("sess_parent_sub");

        let events = vec![env(RuntimeEvent::SubagentSpawned(SubagentSpawned {
            project: scope.clone(),
            parent_run_id: parent_run.clone(),
            parent_task_id: None,
            child_task_id: child_task.clone(),
            child_session_id: child_session.clone(),
            child_run_id: Some(RunId::new("run_child_sub_1")),
        }))];
        append_both(&mem, &sqlite_log, &events).await;

        // get_by_child_task parity.
        let mem_row = SubagentSpawnReadModel::get_by_child_task(&mem, &child_task)
            .await
            .unwrap()
            .unwrap();
        let sqlite_row = SubagentSpawnReadModel::get_by_child_task(&adapter, &child_task)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(mem_row.child_task_id, sqlite_row.child_task_id);
        assert_eq!(mem_row.project, sqlite_row.project);
        assert_eq!(mem_row.parent_run_id, sqlite_row.parent_run_id);
        assert_eq!(mem_row.parent_task_id, sqlite_row.parent_task_id);
        assert_eq!(mem_row.child_session_id, sqlite_row.child_session_id);
        assert_eq!(mem_row.child_run_id, sqlite_row.child_run_id);
        // spawned_at_ms is wall-clock at applier time and may drift a
        // few ms across the two appends — parity is "both > 0".
        assert!(mem_row.spawned_at_ms > 0);
        assert!(sqlite_row.spawned_at_ms > 0);

        // list_by_parent_run parity.
        let mem_list = SubagentSpawnReadModel::list_by_parent_run(&mem, &parent_run)
            .await
            .unwrap();
        let sqlite_list = SubagentSpawnReadModel::list_by_parent_run(&adapter, &parent_run)
            .await
            .unwrap();
        assert_eq!(mem_list.len(), 1);
        assert_eq!(sqlite_list.len(), 1);
        assert_eq!(mem_list[0].child_task_id, sqlite_list[0].child_task_id);
    }

    /// Replay on the same child_task_id is idempotent on all backends:
    /// pg/sqlite via ON CONFLICT DO NOTHING, in-memory via
    /// `or_insert_with`. A second Spawned event with a DIFFERENT
    /// parent is silently dropped — child_task_id is single-parent by
    /// the RFC 014 contract.
    #[tokio::test]
    async fn subagent_spawn_replay_is_idempotent_across_backends() {
        let mem = InMemoryStore::new();
        let adapter = SqliteAdapter::in_memory().await.unwrap();
        let sqlite_log = cairn_store::sqlite::SqliteEventLog::new(adapter.pool().clone());

        let scope = ProjectKey::new("t_sub_dup", "w_sub_dup", "p_sub_dup");
        let parent_run_a = RunId::new("run_parent_a");
        let parent_run_b = RunId::new("run_parent_b");
        let child_task = TaskId::new("task_child_dup");

        let events = vec![
            env(RuntimeEvent::SubagentSpawned(SubagentSpawned {
                project: scope.clone(),
                parent_run_id: parent_run_a.clone(),
                parent_task_id: None,
                child_task_id: child_task.clone(),
                child_session_id: SessionId::new("sess_dup"),
                child_run_id: None,
            })),
            // Bogus second spawn with same child_task_id but different
            // parent — idempotent: first write wins.
            env(RuntimeEvent::SubagentSpawned(SubagentSpawned {
                project: scope.clone(),
                parent_run_id: parent_run_b.clone(),
                parent_task_id: None,
                child_task_id: child_task.clone(),
                child_session_id: SessionId::new("sess_dup"),
                child_run_id: None,
            })),
        ];
        append_both(&mem, &sqlite_log, &events).await;

        let mem_row = SubagentSpawnReadModel::get_by_child_task(&mem, &child_task)
            .await
            .unwrap()
            .unwrap();
        let sqlite_row = SubagentSpawnReadModel::get_by_child_task(&adapter, &child_task)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            mem_row.parent_run_id, parent_run_a,
            "in-memory first-write-wins on child_task_id"
        );
        assert_eq!(
            sqlite_row.parent_run_id, parent_run_a,
            "sqlite ON CONFLICT DO NOTHING keeps first-write"
        );

        // parent_run_b's list is empty — the second event was dropped.
        let b_list = SubagentSpawnReadModel::list_by_parent_run(&mem, &parent_run_b)
            .await
            .unwrap();
        let b_list_sqlite = SubagentSpawnReadModel::list_by_parent_run(&adapter, &parent_run_b)
            .await
            .unwrap();
        assert!(b_list.is_empty());
        assert!(b_list_sqlite.is_empty());
    }

    /// Replay of the same `signal_id` keeps the first ingested row on
    /// pg/sqlite (ON CONFLICT DO NOTHING) and overwrites the in-memory
    /// map. With an identical event payload the resulting records are
    /// equal, so parity holds — this test pins the contract.
    #[tokio::test]
    async fn signal_ingestion_replay_is_idempotent_across_backends() {
        let mem = InMemoryStore::new();
        let adapter = SqliteAdapter::in_memory().await.unwrap();
        let sqlite_log = cairn_store::sqlite::SqliteEventLog::new(adapter.pool().clone());

        let scope = ProjectKey::new("t_sig_dup", "w_sig_dup", "p_sig_dup");
        let events = vec![
            env(RuntimeEvent::SignalIngested(SignalIngested {
                project: scope.clone(),
                signal_id: SignalId::new("sig_replay"),
                source: "original".into(),
                payload: serde_json::json!({"n": 1}),
                timestamp_ms: 1_700_400_000_000,
            })),
            // Replay — exact duplicate. Idempotent on all backends.
            env(RuntimeEvent::SignalIngested(SignalIngested {
                project: scope.clone(),
                signal_id: SignalId::new("sig_replay"),
                source: "original".into(),
                payload: serde_json::json!({"n": 1}),
                timestamp_ms: 1_700_400_000_000,
            })),
        ];
        append_both(&mem, &sqlite_log, &events).await;

        let mem_row = SignalReadModel::get(&mem, &SignalId::new("sig_replay"))
            .await
            .unwrap()
            .unwrap();
        let sqlite_row = SignalReadModel::get(&adapter, &SignalId::new("sig_replay"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(mem_row, sqlite_row);

        // Exactly one row on list.
        let mem_list = SignalReadModel::list_by_project(&mem, &scope, 10, 0)
            .await
            .unwrap();
        let sqlite_list = SignalReadModel::list_by_project(&adapter, &scope, 10, 0)
            .await
            .unwrap();
        assert_eq!(mem_list.len(), 1);
        assert_eq!(sqlite_list.len(), 1);
    }

    // ── RFC-025 Phase 2b.2b m4: user_messages parity ──

    /// Append three user messages on one run + one on a sibling run.
    /// Assert both backends return the same sorted list per run and
    /// the same count.
    #[tokio::test]
    async fn user_message_projection_matches_across_backends() {
        let mem = InMemoryStore::new();
        let adapter = SqliteAdapter::in_memory().await.unwrap();
        let sqlite_log = cairn_store::sqlite::SqliteEventLog::new(adapter.pool().clone());

        let scope = ProjectKey::new("t_um", "w_um", "p_um");
        let run_a = RunId::new("run_um_a");
        let run_b = RunId::new("run_um_b");
        let session = SessionId::new("sess_um");

        let events = vec![
            env(RuntimeEvent::UserMessageAppended(UserMessageAppended {
                project: scope.clone(),
                session_id: session.clone(),
                run_id: run_a.clone(),
                content: "hello".into(),
                sequence: 1,
                appended_at_ms: 1_700_500_000_000,
            })),
            env(RuntimeEvent::UserMessageAppended(UserMessageAppended {
                project: scope.clone(),
                session_id: session.clone(),
                run_id: run_a.clone(),
                content: "follow-up".into(),
                sequence: 2,
                appended_at_ms: 1_700_500_001_000,
            })),
            env(RuntimeEvent::UserMessageAppended(UserMessageAppended {
                project: scope.clone(),
                session_id: session.clone(),
                run_id: run_a.clone(),
                content: "more".into(),
                sequence: 3,
                appended_at_ms: 1_700_500_002_000,
            })),
            env(RuntimeEvent::UserMessageAppended(UserMessageAppended {
                project: scope.clone(),
                session_id: session.clone(),
                run_id: run_b.clone(),
                content: "on another run".into(),
                sequence: 1,
                appended_at_ms: 1_700_500_003_000,
            })),
        ];
        append_both(&mem, &sqlite_log, &events).await;

        // list_by_run parity for run_a.
        let mem_list = UserMessageReadModel::list_by_run(&mem, &run_a, 10, 0)
            .await
            .unwrap();
        let sqlite_list = UserMessageReadModel::list_by_run(&adapter, &run_a, 10, 0)
            .await
            .unwrap();
        // event_id differs per backend — sqlite preserves envelope.event_id,
        // in-memory also preserves envelope.event_id (same value since both
        // ran through append_both with the same EventEnvelope). But the
        // in-memory record's event_id is just whatever envelope we built
        // in `env(..)`, which is the same bytes — so records are equal.
        assert_eq!(mem_list.len(), 3);
        assert_eq!(mem_list, sqlite_list);
        // sequence order 1, 2, 3.
        assert_eq!(mem_list[0].content, "hello");
        assert_eq!(mem_list[1].content, "follow-up");
        assert_eq!(mem_list[2].content, "more");

        // count_by_run parity.
        let mem_count = UserMessageReadModel::count_by_run(&mem, &run_a)
            .await
            .unwrap();
        let sqlite_count = UserMessageReadModel::count_by_run(&adapter, &run_a)
            .await
            .unwrap();
        assert_eq!(mem_count, 3);
        assert_eq!(sqlite_count, 3);

        // run_b isolated.
        let mem_b = UserMessageReadModel::list_by_run(&mem, &run_b, 10, 0)
            .await
            .unwrap();
        let sqlite_b = UserMessageReadModel::list_by_run(&adapter, &run_b, 10, 0)
            .await
            .unwrap();
        assert_eq!(mem_b.len(), 1);
        assert_eq!(mem_b, sqlite_b);

        // Pagination.
        let mem_page = UserMessageReadModel::list_by_run(&mem, &run_a, 2, 1)
            .await
            .unwrap();
        let sqlite_page = UserMessageReadModel::list_by_run(&adapter, &run_a, 2, 1)
            .await
            .unwrap();
        assert_eq!(mem_page, sqlite_page);
        assert_eq!(mem_page.len(), 2);
        assert_eq!(mem_page[0].sequence, 2);
        assert_eq!(mem_page[1].sequence, 3);
    }

    /// Idempotent replay on same (run_id, sequence) — first write wins
    /// on all backends.
    #[tokio::test]
    async fn user_message_replay_is_idempotent_across_backends() {
        let mem = InMemoryStore::new();
        let adapter = SqliteAdapter::in_memory().await.unwrap();
        let sqlite_log = cairn_store::sqlite::SqliteEventLog::new(adapter.pool().clone());

        let scope = ProjectKey::new("t_um_r", "w_um_r", "p_um_r");
        let run = RunId::new("run_um_r");
        let session = SessionId::new("sess_um_r");

        let events = vec![
            env(RuntimeEvent::UserMessageAppended(UserMessageAppended {
                project: scope.clone(),
                session_id: session.clone(),
                run_id: run.clone(),
                content: "original".into(),
                sequence: 1,
                appended_at_ms: 1_700_600_000_000,
            })),
            // Replay with same sequence — dropped on all backends.
            env(RuntimeEvent::UserMessageAppended(UserMessageAppended {
                project: scope.clone(),
                session_id: session.clone(),
                run_id: run.clone(),
                content: "replayed-different-content".into(),
                sequence: 1,
                appended_at_ms: 1_700_600_001_000,
            })),
        ];
        append_both(&mem, &sqlite_log, &events).await;

        let mem_list = UserMessageReadModel::list_by_run(&mem, &run, 10, 0)
            .await
            .unwrap();
        let sqlite_list = UserMessageReadModel::list_by_run(&adapter, &run, 10, 0)
            .await
            .unwrap();
        assert_eq!(mem_list.len(), 1);
        assert_eq!(sqlite_list.len(), 1);
        assert_eq!(
            mem_list[0].content, "original",
            "first write wins on in-memory"
        );
        assert_eq!(
            sqlite_list[0].content, "original",
            "first write wins on sqlite via ON CONFLICT DO NOTHING"
        );
    }

    // ── RFC-025 Phase 2b.2b m5: soul_patches parity ──

    /// Propose → Apply lifecycle must yield state='applied',
    /// applied_at_ms=<t>, new_version=<v> on both backends with all
    /// original patch_content / requires_approval / proposed_at_ms
    /// fields preserved.
    #[tokio::test]
    async fn soul_patch_lifecycle_matches_across_backends() {
        let mem = InMemoryStore::new();
        let adapter = SqliteAdapter::in_memory().await.unwrap();
        let sqlite_log = cairn_store::sqlite::SqliteEventLog::new(adapter.pool().clone());

        let scope = ProjectKey::new("t_sp", "w_sp", "p_sp");

        let events = vec![
            env(RuntimeEvent::SoulPatchProposed(SoulPatchProposed {
                project: scope.clone(),
                patch_id: "patch_parity_1".into(),
                patch_content: "pin-memory: true".into(),
                requires_approval: true,
                proposed_at: 1_700_700_000_000,
            })),
            env(RuntimeEvent::SoulPatchApplied(SoulPatchApplied {
                project: scope.clone(),
                patch_id: "patch_parity_1".into(),
                new_version: 7,
                applied_at: 1_700_700_005_000,
            })),
        ];
        append_both(&mem, &sqlite_log, &events).await;

        let mem_row = SoulPatchReadModel::get(&mem, "patch_parity_1")
            .await
            .unwrap()
            .unwrap();
        let sqlite_row = SoulPatchReadModel::get(&adapter, "patch_parity_1")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(mem_row, sqlite_row);
        assert_eq!(mem_row.state, SoulPatchState::Applied);
        assert_eq!(mem_row.patch_content, "pin-memory: true");
        assert!(mem_row.requires_approval);
        assert_eq!(mem_row.proposed_at_ms, 1_700_700_000_000);
        assert_eq!(mem_row.applied_at_ms, Some(1_700_700_005_000));
        assert_eq!(mem_row.new_version, Some(7));
    }

    /// A patch stays in 'proposed' state until the Applied event
    /// arrives; list_by_project is newest-first.
    #[tokio::test]
    async fn soul_patch_list_by_project_newest_first() {
        let mem = InMemoryStore::new();
        let adapter = SqliteAdapter::in_memory().await.unwrap();
        let sqlite_log = cairn_store::sqlite::SqliteEventLog::new(adapter.pool().clone());

        let scope = ProjectKey::new("t_sp_l", "w_sp_l", "p_sp_l");

        let events = vec![
            env(RuntimeEvent::SoulPatchProposed(SoulPatchProposed {
                project: scope.clone(),
                patch_id: "patch_a".into(),
                patch_content: "a".into(),
                requires_approval: false,
                proposed_at: 1_700_800_000_000,
            })),
            env(RuntimeEvent::SoulPatchProposed(SoulPatchProposed {
                project: scope.clone(),
                patch_id: "patch_b".into(),
                patch_content: "b".into(),
                requires_approval: true,
                proposed_at: 1_700_800_002_000,
            })),
            env(RuntimeEvent::SoulPatchProposed(SoulPatchProposed {
                project: scope.clone(),
                patch_id: "patch_c".into(),
                patch_content: "c".into(),
                requires_approval: true,
                proposed_at: 1_700_800_001_000,
            })),
        ];
        append_both(&mem, &sqlite_log, &events).await;

        let mem_list = SoulPatchReadModel::list_by_project(&mem, &scope, 10, 0)
            .await
            .unwrap();
        let sqlite_list = SoulPatchReadModel::list_by_project(&adapter, &scope, 10, 0)
            .await
            .unwrap();
        assert_eq!(mem_list.len(), 3);
        assert_eq!(mem_list, sqlite_list);
        // Newest-first.
        assert_eq!(mem_list[0].patch_id, "patch_b");
        assert_eq!(mem_list[1].patch_id, "patch_c");
        assert_eq!(mem_list[2].patch_id, "patch_a");
        // All still proposed.
        for rec in &mem_list {
            assert_eq!(rec.state, SoulPatchState::Proposed);
            assert_eq!(rec.applied_at_ms, None);
            assert_eq!(rec.new_version, None);
        }
    }

    /// Replayed `SoulPatchProposed` after a `SoulPatchApplied`
    /// preserves the applied state — the applier's ON CONFLICT DO
    /// NOTHING on Proposed keeps the existing row (which is already
    /// in applied state) untouched.
    #[tokio::test]
    async fn soul_patch_replay_after_apply_preserves_applied() {
        let mem = InMemoryStore::new();
        let adapter = SqliteAdapter::in_memory().await.unwrap();
        let sqlite_log = cairn_store::sqlite::SqliteEventLog::new(adapter.pool().clone());

        let scope = ProjectKey::new("t_sp_r", "w_sp_r", "p_sp_r");

        let events = vec![
            env(RuntimeEvent::SoulPatchProposed(SoulPatchProposed {
                project: scope.clone(),
                patch_id: "patch_replay".into(),
                patch_content: "x".into(),
                requires_approval: true,
                proposed_at: 1_700_900_000_000,
            })),
            env(RuntimeEvent::SoulPatchApplied(SoulPatchApplied {
                project: scope.clone(),
                patch_id: "patch_replay".into(),
                new_version: 3,
                applied_at: 1_700_900_005_000,
            })),
            // Replayed Proposed — must not reset to 'proposed'.
            env(RuntimeEvent::SoulPatchProposed(SoulPatchProposed {
                project: scope.clone(),
                patch_id: "patch_replay".into(),
                patch_content: "x".into(),
                requires_approval: true,
                proposed_at: 1_700_900_000_000,
            })),
        ];
        append_both(&mem, &sqlite_log, &events).await;

        let mem_row = SoulPatchReadModel::get(&mem, "patch_replay")
            .await
            .unwrap()
            .unwrap();
        let sqlite_row = SoulPatchReadModel::get(&adapter, "patch_replay")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(mem_row, sqlite_row);
        assert_eq!(mem_row.state, SoulPatchState::Applied);
        assert_eq!(mem_row.new_version, Some(3));
    }

    // ── RFC-025 Phase 2b.2b m6: tool_recovery_pauses parity ──

    /// Pause two tool calls on one run (distinct tool_call_ids) and
    /// assert both backends surface the same list/get results, then
    /// replay one of the events and assert first-write-wins.
    #[tokio::test]
    async fn tool_recovery_pause_projection_matches_across_backends() {
        let mem = InMemoryStore::new();
        let adapter = SqliteAdapter::in_memory().await.unwrap();
        let sqlite_log = cairn_store::sqlite::SqliteEventLog::new(adapter.pool().clone());

        let scope = ProjectKey::new("t_trp", "w_trp", "p_trp");
        let run = RunId::new("run_trp");
        let task = TaskId::new("task_trp");

        let events = vec![
            env(RuntimeEvent::ToolRecoveryPaused(ToolRecoveryPaused {
                project: scope.clone(),
                run_id: run.clone(),
                task_id: Some(task.clone()),
                tool_name: "shell".into(),
                tool_call_id: "tc_1".into(),
                reason: "DangerousPause tool with no cached result on recovery".into(),
                paused_at_ms: 1_700_100_000_000,
            })),
            env(RuntimeEvent::ToolRecoveryPaused(ToolRecoveryPaused {
                project: scope.clone(),
                run_id: run.clone(),
                task_id: None,
                tool_name: "http".into(),
                tool_call_id: "tc_2".into(),
                reason: "DangerousPause".into(),
                paused_at_ms: 1_700_100_001_000,
            })),
            // Replay of tc_1 — first write wins.
            env(RuntimeEvent::ToolRecoveryPaused(ToolRecoveryPaused {
                project: scope.clone(),
                run_id: run.clone(),
                task_id: Some(task.clone()),
                tool_name: "shell".into(),
                tool_call_id: "tc_1".into(),
                reason: "different reason".into(),
                paused_at_ms: 1_700_100_002_000,
            })),
        ];
        append_both(&mem, &sqlite_log, &events).await;

        // get() parity.
        let mem_row = ToolRecoveryPauseReadModel::get(&mem, "tc_1")
            .await
            .unwrap()
            .unwrap();
        let sqlite_row = ToolRecoveryPauseReadModel::get(&adapter, "tc_1")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(mem_row, sqlite_row);
        assert_eq!(
            mem_row.reason, "DangerousPause tool with no cached result on recovery",
            "first write wins on in-memory"
        );
        assert_eq!(sqlite_row.reason, mem_row.reason);

        // list_by_run parity.
        let mem_list = ToolRecoveryPauseReadModel::list_by_run(&mem, &run)
            .await
            .unwrap();
        let sqlite_list = ToolRecoveryPauseReadModel::list_by_run(&adapter, &run)
            .await
            .unwrap();
        assert_eq!(mem_list.len(), 2);
        assert_eq!(mem_list, sqlite_list);
        assert_eq!(mem_list[0].tool_call_id, "tc_1");
        assert_eq!(mem_list[1].tool_call_id, "tc_2");
    }

    // ── RFC-025 Phase 2b.3 m1: ingest_jobs parity ─────────────────────

    #[tokio::test]
    async fn ingest_job_projection_matches_across_backends() {
        use cairn_domain::{IngestJobCompleted, IngestJobId, IngestJobStarted, SourceId};
        use cairn_store::projections::IngestJobReadModel;

        let mem = InMemoryStore::new();
        let adapter = SqliteAdapter::in_memory().await.unwrap();
        let sqlite_log = cairn_store::sqlite::SqliteEventLog::new(adapter.pool().clone());

        let scope = ProjectKey::new("t_ij", "w_ij", "p_ij");
        let j1 = IngestJobId::new("job_ij_1");
        let j2 = IngestJobId::new("job_ij_2");

        let events = vec![
            env(RuntimeEvent::IngestJobStarted(IngestJobStarted {
                project: scope.clone(),
                job_id: j1.clone(),
                source_id: Some(SourceId::new("src_parity")),
                document_count: 3,
                started_at: 1_700_200_000_000,
            })),
            env(RuntimeEvent::IngestJobStarted(IngestJobStarted {
                project: scope.clone(),
                job_id: j2.clone(),
                source_id: None,
                document_count: 5,
                started_at: 1_700_200_001_000,
            })),
            env(RuntimeEvent::IngestJobCompleted(IngestJobCompleted {
                project: scope.clone(),
                job_id: j1.clone(),
                success: true,
                error_message: None,
                completed_at: 1_700_200_100_000,
            })),
            env(RuntimeEvent::IngestJobCompleted(IngestJobCompleted {
                project: scope.clone(),
                job_id: j2.clone(),
                success: false,
                error_message: Some("embedder 503".to_owned()),
                completed_at: 1_700_200_101_000,
            })),
        ];
        append_both(&mem, &sqlite_log, &events).await;

        // get() parity for both terminal shapes.
        let mem_j1 = IngestJobReadModel::get(&mem, &j1).await.unwrap().unwrap();
        let sqlite_j1 = IngestJobReadModel::get(&adapter, &j1)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(mem_j1, sqlite_j1);

        let mem_j2 = IngestJobReadModel::get(&mem, &j2).await.unwrap().unwrap();
        let sqlite_j2 = IngestJobReadModel::get(&adapter, &j2)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(mem_j2, sqlite_j2);

        // list_by_project parity: ordered by (created_at_ms ASC, job_id ASC).
        let mem_list = IngestJobReadModel::list_by_project(&mem, &scope, 10, 0)
            .await
            .unwrap();
        let sqlite_list = IngestJobReadModel::list_by_project(&adapter, &scope, 10, 0)
            .await
            .unwrap();
        assert_eq!(mem_list, sqlite_list);
        assert_eq!(mem_list.len(), 2);
        assert_eq!(mem_list[0].id, j1);
        assert_eq!(mem_list[1].id, j2);
    }

    /// Same-millisecond-collision tiebreak: two jobs created at the same
    /// `started_at`, which the SQL backends disambiguate via the secondary
    /// `job_id ASC` sort key. In-memory must agree byte-for-byte.
    #[tokio::test]
    async fn ingest_job_same_ms_collision_tiebreak_matches_across_backends() {
        use cairn_domain::{IngestJobId, IngestJobStarted};
        use cairn_store::projections::IngestJobReadModel;

        let mem = InMemoryStore::new();
        let adapter = SqliteAdapter::in_memory().await.unwrap();
        let sqlite_log = cairn_store::sqlite::SqliteEventLog::new(adapter.pool().clone());

        let scope = ProjectKey::new("t_tie", "w_tie", "p_tie");
        let same_ts = 1_700_300_000_000;

        // IDs chosen so a naive filesystem-iteration order would differ
        // from the sorted (`job_beta` < `job_zeta`) order; the secondary
        // sort key must win.
        let events = vec![
            env(RuntimeEvent::IngestJobStarted(IngestJobStarted {
                project: scope.clone(),
                job_id: IngestJobId::new("job_zeta"),
                source_id: None,
                document_count: 1,
                started_at: same_ts,
            })),
            env(RuntimeEvent::IngestJobStarted(IngestJobStarted {
                project: scope.clone(),
                job_id: IngestJobId::new("job_beta"),
                source_id: None,
                document_count: 1,
                started_at: same_ts,
            })),
        ];
        append_both(&mem, &sqlite_log, &events).await;

        let mem_list = IngestJobReadModel::list_by_project(&mem, &scope, 10, 0)
            .await
            .unwrap();
        let sqlite_list = IngestJobReadModel::list_by_project(&adapter, &scope, 10, 0)
            .await
            .unwrap();
        assert_eq!(mem_list, sqlite_list, "same-ms tiebreak must be byte-equal");
        assert_eq!(mem_list[0].id.as_str(), "job_beta");
        assert_eq!(mem_list[1].id.as_str(), "job_zeta");
    }

    // ── RFC-025 Phase 2b.3 m2: default_settings parity ─────────────────

    /// DefaultSetting events carry no `project` — they are
    /// `OwnershipKey::System` — so the parity helper needs a raw envelope.
    fn sys_env(event: RuntimeEvent) -> EventEnvelope<RuntimeEvent> {
        EventEnvelope {
            event_id: next_event_id(),
            source: EventSource::Runtime,
            ownership: cairn_domain::OwnershipKey::System,
            causation_id: None,
            correlation_id: None,
            payload: event,
        }
    }

    #[tokio::test]
    async fn default_settings_projection_matches_across_backends() {
        use cairn_domain::{DefaultSettingCleared, DefaultSettingSet, Scope};
        use cairn_store::projections::DefaultsReadModel;

        let mem = InMemoryStore::new();
        let adapter = SqliteAdapter::in_memory().await.unwrap();
        let sqlite_log = cairn_store::sqlite::SqliteEventLog::new(adapter.pool().clone());

        let scope_id = "tenant_parity";
        let events = vec![
            sys_env(RuntimeEvent::DefaultSettingSet(DefaultSettingSet {
                scope: Scope::Tenant,
                scope_id: scope_id.to_owned(),
                key: "zeta_key".to_owned(),
                value: serde_json::json!({"nested": {"v": 1}}),
            })),
            sys_env(RuntimeEvent::DefaultSettingSet(DefaultSettingSet {
                scope: Scope::Tenant,
                scope_id: scope_id.to_owned(),
                key: "alpha_key".to_owned(),
                value: serde_json::json!("string_value"),
            })),
            sys_env(RuntimeEvent::DefaultSettingSet(DefaultSettingSet {
                scope: Scope::Tenant,
                scope_id: scope_id.to_owned(),
                key: "middle_key".to_owned(),
                value: serde_json::json!(42),
            })),
            // Clear the middle key.
            sys_env(RuntimeEvent::DefaultSettingCleared(DefaultSettingCleared {
                scope: Scope::Tenant,
                scope_id: scope_id.to_owned(),
                key: "middle_key".to_owned(),
            })),
            // Upsert alpha_key — last write wins.
            sys_env(RuntimeEvent::DefaultSettingSet(DefaultSettingSet {
                scope: Scope::Tenant,
                scope_id: scope_id.to_owned(),
                key: "alpha_key".to_owned(),
                value: serde_json::json!("updated"),
            })),
        ];
        append_both(&mem, &sqlite_log, &events).await;

        // get() parity for each surviving key.
        for key in ["alpha_key", "zeta_key"] {
            let mem_r = DefaultsReadModel::get(&mem, Scope::Tenant, scope_id, key)
                .await
                .unwrap()
                .unwrap();
            let sqlite_r = DefaultsReadModel::get(&adapter, Scope::Tenant, scope_id, key)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(mem_r, sqlite_r, "get({key}) must match across backends");
        }

        // Cleared key: both return None.
        let mem_mid = DefaultsReadModel::get(&mem, Scope::Tenant, scope_id, "middle_key")
            .await
            .unwrap();
        let sqlite_mid = DefaultsReadModel::get(&adapter, Scope::Tenant, scope_id, "middle_key")
            .await
            .unwrap();
        assert!(mem_mid.is_none());
        assert!(sqlite_mid.is_none());

        // list_by_scope parity: key ASC ordering.
        let mem_list = DefaultsReadModel::list_by_scope(&mem, Scope::Tenant, scope_id)
            .await
            .unwrap();
        let sqlite_list = DefaultsReadModel::list_by_scope(&adapter, Scope::Tenant, scope_id)
            .await
            .unwrap();
        assert_eq!(mem_list, sqlite_list);
        assert_eq!(mem_list.len(), 2);
        assert_eq!(mem_list[0].key, "alpha_key");
        assert_eq!(mem_list[0].value, serde_json::json!("updated"));
        assert_eq!(mem_list[1].key, "zeta_key");
    }

    // ── RFC-025 Phase 2b.3 m3: channels + channel_messages parity ────

    #[tokio::test]
    async fn channel_projection_matches_across_backends() {
        use cairn_domain::{ChannelCreated, ChannelId, ChannelMessageConsumed, ChannelMessageSent};
        use cairn_store::projections::ChannelReadModel;

        let mem = InMemoryStore::new();
        let adapter = SqliteAdapter::in_memory().await.unwrap();
        let sqlite_log = cairn_store::sqlite::SqliteEventLog::new(adapter.pool().clone());

        let scope = ProjectKey::new("t_chp", "w_chp", "p_chp");
        let c1 = ChannelId::new("chan_parity_1");
        let c2 = ChannelId::new("chan_parity_2");

        let events = vec![
            env(RuntimeEvent::ChannelCreated(ChannelCreated {
                channel_id: c1.clone(),
                project: scope.clone(),
                name: "alpha".to_owned(),
                capacity: 16,
                created_at_ms: 1_700_600_000_000,
            })),
            // Same-ms collision on channel create: tiebreak on channel_id.
            env(RuntimeEvent::ChannelCreated(ChannelCreated {
                channel_id: c2.clone(),
                project: scope.clone(),
                name: "beta".to_owned(),
                capacity: 32,
                created_at_ms: 1_700_600_000_000,
            })),
            env(RuntimeEvent::ChannelMessageSent(ChannelMessageSent {
                channel_id: c1.clone(),
                project: scope.clone(),
                message_id: "m_z".to_owned(),
                sender_id: "alice".to_owned(),
                body: "hello".to_owned(),
                sent_at_ms: 1_700_600_010_000,
            })),
            env(RuntimeEvent::ChannelMessageSent(ChannelMessageSent {
                channel_id: c1.clone(),
                project: scope.clone(),
                message_id: "m_a".to_owned(),
                sender_id: "bob".to_owned(),
                body: "world".to_owned(),
                sent_at_ms: 1_700_600_010_000,
            })),
            env(RuntimeEvent::ChannelMessageConsumed(
                ChannelMessageConsumed {
                    channel_id: c1.clone(),
                    project: scope.clone(),
                    message_id: "m_a".to_owned(),
                    consumed_by: "worker_1".to_owned(),
                    consumed_at_ms: 1_700_600_020_000,
                },
            )),
            // Replayed Sent for m_a — first-write-wins.
            env(RuntimeEvent::ChannelMessageSent(ChannelMessageSent {
                channel_id: c1.clone(),
                project: scope.clone(),
                message_id: "m_a".to_owned(),
                sender_id: "IMPOSTOR".to_owned(),
                body: "DIFFERENT".to_owned(),
                sent_at_ms: 1_700_600_030_000,
            })),
        ];
        append_both(&mem, &sqlite_log, &events).await;

        // get_channel parity.
        let mem_c1 = ChannelReadModel::get_channel(&mem, &c1)
            .await
            .unwrap()
            .unwrap();
        let sqlite_c1 = ChannelReadModel::get_channel(&adapter, &c1)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(mem_c1, sqlite_c1);

        // list_channels parity: sorted by (created_at ASC, channel_id ASC).
        let mem_list = ChannelReadModel::list_channels(&mem, &scope, 10, 0)
            .await
            .unwrap();
        let sqlite_list = ChannelReadModel::list_channels(&adapter, &scope, 10, 0)
            .await
            .unwrap();
        assert_eq!(mem_list, sqlite_list);
        assert_eq!(mem_list.len(), 2);
        assert_eq!(mem_list[0].channel_id, c1); // chan_parity_1 < chan_parity_2
        assert_eq!(mem_list[1].channel_id, c2);

        // list_messages parity: ordered (sent_at_ms ASC, message_id ASC).
        // m_a < m_z at the same timestamp, so m_a comes first.
        let mem_msgs = ChannelReadModel::list_messages(&mem, &c1, 10)
            .await
            .unwrap();
        let sqlite_msgs = ChannelReadModel::list_messages(&adapter, &c1, 10)
            .await
            .unwrap();
        assert_eq!(mem_msgs, sqlite_msgs);
        assert_eq!(mem_msgs.len(), 2);
        assert_eq!(mem_msgs[0].message_id, "m_a");
        assert_eq!(
            mem_msgs[0].sender_id, "bob",
            "first Sent write wins; replayed Sent for m_a must not clobber"
        );
        assert_eq!(mem_msgs[0].body, "world");
        assert_eq!(mem_msgs[0].consumed_by.as_deref(), Some("worker_1"));
        assert_eq!(mem_msgs[1].message_id, "m_z");
        assert_eq!(mem_msgs[1].consumed_by, None);
    }

    // ── RFC-025 Phase 2b.3 m4: notifications parity ────────────────────

    #[tokio::test]
    async fn notification_preferences_projection_matches_across_backends() {
        use cairn_domain::notification_prefs::NotificationChannel;
        use cairn_domain::NotificationPreferenceSet;
        use cairn_store::projections::NotificationReadModel;

        let mem = InMemoryStore::new();
        let adapter = SqliteAdapter::in_memory().await.unwrap();
        let sqlite_log = cairn_store::sqlite::SqliteEventLog::new(adapter.pool().clone());

        let tenant = cairn_domain::TenantId::new("t_np_parity");

        let events = vec![
            sys_env(RuntimeEvent::NotificationPreferenceSet(
                NotificationPreferenceSet {
                    tenant_id: tenant.clone(),
                    operator_id: "zeta".to_owned(),
                    event_types: vec!["a".to_owned(), "b".to_owned()],
                    channels: vec![NotificationChannel {
                        kind: "email".to_owned(),
                        target: "z@x".to_owned(),
                    }],
                    set_at_ms: 100,
                },
            )),
            sys_env(RuntimeEvent::NotificationPreferenceSet(
                NotificationPreferenceSet {
                    tenant_id: tenant.clone(),
                    operator_id: "alpha".to_owned(),
                    event_types: vec!["c".to_owned()],
                    channels: vec![],
                    set_at_ms: 200,
                },
            )),
            // Upsert alpha
            sys_env(RuntimeEvent::NotificationPreferenceSet(
                NotificationPreferenceSet {
                    tenant_id: tenant.clone(),
                    operator_id: "alpha".to_owned(),
                    event_types: vec!["updated".to_owned()],
                    channels: vec![NotificationChannel {
                        kind: "slack".to_owned(),
                        target: "#a".to_owned(),
                    }],
                    set_at_ms: 300,
                },
            )),
        ];
        append_both(&mem, &sqlite_log, &events).await;

        for op in ["alpha", "zeta"] {
            let m = NotificationReadModel::get_preferences(&mem, &tenant, op)
                .await
                .unwrap()
                .unwrap();
            let s = NotificationReadModel::get_preferences(&adapter, &tenant, op)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(m, s, "pref({op}) must match across backends");
        }

        let mem_list = NotificationReadModel::list_preferences_by_tenant(&mem, &tenant)
            .await
            .unwrap();
        let sqlite_list = NotificationReadModel::list_preferences_by_tenant(&adapter, &tenant)
            .await
            .unwrap();
        assert_eq!(mem_list, sqlite_list);
        assert_eq!(mem_list.len(), 2);
        assert_eq!(mem_list[0].operator_id, "alpha");
        assert_eq!(mem_list[0].event_types, vec!["updated".to_owned()]);
        assert_eq!(mem_list[1].operator_id, "zeta");
    }

    #[tokio::test]
    async fn notification_sent_projection_matches_across_backends() {
        use cairn_domain::NotificationSent;
        use cairn_store::projections::NotificationReadModel;

        let mem = InMemoryStore::new();
        let adapter = SqliteAdapter::in_memory().await.unwrap();
        let sqlite_log = cairn_store::sqlite::SqliteEventLog::new(adapter.pool().clone());

        let tenant = cairn_domain::TenantId::new("t_ns_parity");

        let events = vec![
            sys_env(RuntimeEvent::NotificationSent(NotificationSent {
                record_id: "rec_z".to_owned(),
                tenant_id: tenant.clone(),
                operator_id: "alice".to_owned(),
                event_type: "e1".to_owned(),
                channel_kind: "email".to_owned(),
                channel_target: "a@x".to_owned(),
                payload: serde_json::json!({"n": 1}),
                sent_at_ms: 1_000,
                delivered: true,
                delivery_error: None,
            })),
            // Same ts as rec_z — tiebreak on record_id ASC.
            sys_env(RuntimeEvent::NotificationSent(NotificationSent {
                record_id: "rec_a".to_owned(),
                tenant_id: tenant.clone(),
                operator_id: "bob".to_owned(),
                event_type: "e2".to_owned(),
                channel_kind: "webhook".to_owned(),
                channel_target: "https://x".to_owned(),
                payload: serde_json::json!({"n": 2}),
                sent_at_ms: 1_000,
                delivered: false,
                delivery_error: Some("oops".to_owned()),
            })),
            sys_env(RuntimeEvent::NotificationSent(NotificationSent {
                record_id: "rec_m".to_owned(),
                tenant_id: tenant.clone(),
                operator_id: "alice".to_owned(),
                event_type: "e3".to_owned(),
                channel_kind: "email".to_owned(),
                channel_target: "a@x".to_owned(),
                payload: serde_json::json!({}),
                sent_at_ms: 2_000,
                delivered: true,
                delivery_error: None,
            })),
            // Replayed rec_a — first-write-wins.
            sys_env(RuntimeEvent::NotificationSent(NotificationSent {
                record_id: "rec_a".to_owned(),
                tenant_id: tenant.clone(),
                operator_id: "IMPOSTOR".to_owned(),
                event_type: "IMPOSTOR".to_owned(),
                channel_kind: "email".to_owned(),
                channel_target: "x".to_owned(),
                payload: serde_json::json!({}),
                sent_at_ms: 9_999,
                delivered: true,
                delivery_error: None,
            })),
        ];
        append_both(&mem, &sqlite_log, &events).await;

        let mem_all = NotificationReadModel::list_sent_notifications(&mem, &tenant, 0)
            .await
            .unwrap();
        let sqlite_all = NotificationReadModel::list_sent_notifications(&adapter, &tenant, 0)
            .await
            .unwrap();
        assert_eq!(mem_all, sqlite_all);
        assert_eq!(mem_all.len(), 3);
        assert_eq!(mem_all[0].record_id, "rec_a");
        assert_eq!(
            mem_all[0].operator_id, "bob",
            "first-write-wins: replayed rec_a must not clobber operator_id"
        );
        assert_eq!(mem_all[1].record_id, "rec_z");
        assert_eq!(mem_all[2].record_id, "rec_m");

        let mem_failed = NotificationReadModel::list_failed_notifications(&mem, &tenant)
            .await
            .unwrap();
        let sqlite_failed = NotificationReadModel::list_failed_notifications(&adapter, &tenant)
            .await
            .unwrap();
        assert_eq!(mem_failed, sqlite_failed);
        assert_eq!(mem_failed.len(), 1);
        assert_eq!(mem_failed[0].record_id, "rec_a");
    }

    // ── RFC-025 Phase 2b.3 m5: checkpoint_strategies parity ────────────

    #[tokio::test]
    async fn checkpoint_strategy_projection_matches_across_backends() {
        use cairn_domain::CheckpointStrategySet;
        use cairn_store::projections::CheckpointStrategyReadModel;

        let mem = InMemoryStore::new();
        let adapter = SqliteAdapter::in_memory().await.unwrap();
        let sqlite_log = cairn_store::sqlite::SqliteEventLog::new(adapter.pool().clone());

        let run_a = cairn_domain::RunId::new("run_cs_a");
        let run_b = cairn_domain::RunId::new("run_cs_b");

        let events = vec![
            sys_env(RuntimeEvent::CheckpointStrategySet(CheckpointStrategySet {
                strategy_id: "s_a1".to_owned(),
                description: "".to_owned(),
                set_at_ms: 10,
                run_id: Some(run_a.clone()),
                interval_ms: 1000,
                max_checkpoints: 3,
                trigger_on_task_complete: true,
            })),
            sys_env(RuntimeEvent::CheckpointStrategySet(CheckpointStrategySet {
                strategy_id: "s_b".to_owned(),
                description: "".to_owned(),
                set_at_ms: 20,
                run_id: Some(run_b.clone()),
                interval_ms: 5_000,
                max_checkpoints: 0, // default-10 rehydration path
                trigger_on_task_complete: false,
            })),
            // Upsert run_a — last write wins.
            sys_env(RuntimeEvent::CheckpointStrategySet(CheckpointStrategySet {
                strategy_id: "s_a2".to_owned(),
                description: "".to_owned(),
                set_at_ms: 30,
                run_id: Some(run_a.clone()),
                interval_ms: 2000,
                max_checkpoints: 9,
                trigger_on_task_complete: false,
            })),
            // run_id = None — skipped on both appliers.
            sys_env(RuntimeEvent::CheckpointStrategySet(CheckpointStrategySet {
                strategy_id: "s_orphan".to_owned(),
                description: "".to_owned(),
                set_at_ms: 40,
                run_id: None,
                interval_ms: 0,
                max_checkpoints: 0,
                trigger_on_task_complete: false,
            })),
        ];
        append_both(&mem, &sqlite_log, &events).await;

        for run in [&run_a, &run_b] {
            let m = CheckpointStrategyReadModel::get_by_run(&mem, run)
                .await
                .unwrap()
                .unwrap();
            let s = CheckpointStrategyReadModel::get_by_run(&adapter, run)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(m, s, "get_by_run({}) byte-equal", run.as_str());
        }

        let a = CheckpointStrategyReadModel::get_by_run(&adapter, &run_a)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(a.strategy_id, "s_a2", "last write wins");
        assert_eq!(a.interval_ms, 2000);
        assert_eq!(a.max_checkpoints, 9);

        let b = CheckpointStrategyReadModel::get_by_run(&adapter, &run_b)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(b.max_checkpoints, 10, "0 rehydrates to default 10");
        assert!(!b.trigger_on_task_complete);
    }
}

// ── Postgres parity (nightly / labelled PRs) ───────────────────────────
//
// Gated behind `--features postgres` + the `TEST_DATABASE_URL` env var.
// The RFC-025 Phase 0 guidance runs this in nightly CI and on PRs
// labelled `projection-parity`; default PR lanes skip it so
// testcontainers cost stays off per-commit.
//
// The pg tests use `#[ignore]` so `cargo test` skips them by default —
// nightly CI runs `cargo test -- --ignored` after exporting
// `TEST_DATABASE_URL`. This is the pattern `event_log_batch_append.rs`
// uses for the same tradeoff; keeping both files consistent means
// operators already know the escape hatch.
#[cfg(feature = "postgres")]
mod in_memory_vs_pg {
    /// Nightly-only pg parity probe. Run with:
    ///
    /// ```text
    /// TEST_DATABASE_URL=postgres://… cargo test -p cairn-store \
    ///   --features postgres --test projection_parity -- --ignored
    /// ```
    ///
    /// Body intentionally minimal: Phase 0 asserts only that the pg
    /// path is *wired* and discoverable. Full per-variant coverage
    /// arrives in Phase 2a/2b, which will lift the `try_pg_adapter`
    /// helper out of `event_log_batch_append.rs` into a shared
    /// support module (~30 LOC of pool setup) before growing the pg
    /// parity surface.
    #[tokio::test]
    #[ignore = "requires TEST_DATABASE_URL; runs in nightly CI via `cargo test -- --ignored`"]
    async fn pg_parity_smoke_is_runnable_when_database_is_set() {
        let url = std::env::var("TEST_DATABASE_URL")
            .expect("TEST_DATABASE_URL must be set when running this ignored test");
        assert!(
            !url.is_empty(),
            "TEST_DATABASE_URL was set but empty — export a real postgres URL"
        );
    }
}
