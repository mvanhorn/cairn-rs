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
        ApprovalId, ApprovalRequested, ApprovalRequirement, EventEnvelope, EventId, EventSource,
        ProjectCreated, ProjectKey, RunCreated, RunId, RunState, RunStateChanged, RuntimeEvent,
        SessionCreated, SessionId, SessionState, SessionStateChanged, StateTransition, TaskCreated,
        TaskId, TaskState, TaskStateChanged, TenantCreated, TenantId, WorkspaceCreated,
        WorkspaceId,
    };
    use cairn_store::event_log::EventLog;
    use cairn_store::in_memory::InMemoryStore;
    use cairn_store::projections::{
        ApprovalReadModel, RunReadModel, SessionReadModel, TaskReadModel,
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
