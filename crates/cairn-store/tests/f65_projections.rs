//! F65 PR-2 integration tests for the orchestrator-session projections.
//!
//! PR-1 added the domain shapes and wired all projection sites as no-ops.
//! PR-2 replaces those no-ops with real writers across the three backends
//! (pg, sqlite, in-memory). These tests drive the event path end-to-end so
//! the promises in the arch doc (`docs/design/orchestrator-session-architecture.md`
//! §7 + §8) are real claims, not assertions on the scaffolding.
//!
//! Test strategy:
//!
//! 1. Append one (or a small chain of) `RuntimeEvent` variants through the
//!    real `EventLog::append` so the in-transaction sync projection hook
//!    runs. No mocks.
//! 2. Assert the projection state via the trait-level read APIs added in
//!    PR-2 (`SessionOutcomeReadModel`, `WorkspaceSnapshotReadModel`,
//!    `F65CheckpointReadModel`) and — where the trait doesn't cover
//!    a direct DB round-trip need — via the existing `SessionReadModel`
//!    so the extended columns round-trip through the DB-side SELECT.
//! 3. Exercise replay-idempotency, lineage walk, and the
//!    nullable `workspace_snapshot_id` / null `schema_version` back-compat
//!    paths from §6.3.
//!
//! SQLite is exercised directly in this file. Postgres parity is enforced
//! by the parser-level `schema_parity.rs` and `pg_migration_contract.rs`
//! tests plus the cross-backend projection contract per event. In-memory
//! coverage runs side-by-side on the multi-backend tests (session-extension
//! round-trip, checkpoint insert, workspace-snapshot list + reaped); the
//! remaining tests — termination_reason_json sidecar round-trip, outcome
//! upsert idempotency, schema-parity — are intentionally SQLite-only
//! because they assert SQL-layer behaviour (JSON column, ON CONFLICT,
//! DDL parity) that has no in-memory equivalent.

// The file only exists to validate F65 PR-2 projections. Builds with just
// `sqlite`, just `postgres`, or both — each inner module is gated on the
// feature(s) it needs. In-memory coverage runs unconditionally.
#![cfg(any(feature = "sqlite", feature = "postgres"))]

#[cfg(feature = "sqlite")]
mod f65_sqlite {
    use cairn_domain::{
        session_orchestration::{
            BreakerKind, CircuitBreakerTrip, SessionOutcome, TerminationReason,
        },
        CheckpointId, CheckpointPersisted, EventEnvelope, EventId, EventSource, ProjectKey, RunId,
        RuntimeEvent, SessionAttemptStarted, SessionCreated, SessionId, SessionOutcomeEmitted,
        WorkspaceId, WorkspaceSnapshotCreated, WorkspaceSnapshotId, WorkspaceSnapshotReaped,
    };
    use cairn_store::event_log::EventLog;
    use cairn_store::in_memory::InMemoryStore;
    use cairn_store::projections::{
        F65CheckpointReadModel, SessionOutcomeReadModel, SessionReadModel,
        WorkspaceSnapshotReadModel,
    };
    use cairn_store::sqlite::{SqliteAdapter, SqliteEventLog};
    use std::sync::atomic::{AtomicU64, Ordering};

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    fn project() -> ProjectKey {
        ProjectKey::new("tenant_f65", "ws_f65", "proj_f65")
    }

    fn env(event: RuntimeEvent) -> EventEnvelope<RuntimeEvent> {
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        EventEnvelope::for_runtime_event(
            EventId::new(format!("evt_f65_{n}")),
            EventSource::Runtime,
            event,
        )
    }

    /// Boot a fresh SQLite adapter + event log. Every test gets its own
    /// in-memory DB so tests don't need to reason about shared state.
    async fn fresh_sqlite() -> (SqliteAdapter, SqliteEventLog) {
        let adapter = SqliteAdapter::in_memory().await.expect("sqlite in_memory");
        let log = SqliteEventLog::new(adapter.pool().clone());
        (adapter, log)
    }

    /// Append a `SessionCreated` event through the log so the projection
    /// row exists — every F65 event that updates an existing row needs
    /// this scaffolding to avoid silently no-op'ing on missing rows
    /// (matches the `RunCompletionAnnotated` contract used elsewhere).
    async fn seed_session<L: EventLog + ?Sized>(log: &L, session_id: &str) {
        let evt = env(RuntimeEvent::SessionCreated(SessionCreated {
            project: project(),
            session_id: SessionId::new(session_id),
        }));
        log.append(std::slice::from_ref(&evt))
            .await
            .expect("seed session_created");
    }

    /// A minimal root-Run row — required so checkpoints.run_id FK holds.
    async fn seed_run<L: EventLog + ?Sized>(log: &L, session_id: &str, run_id: &str) {
        let evt = env(RuntimeEvent::RunCreated(cairn_domain::RunCreated {
            project: project(),
            session_id: SessionId::new(session_id),
            run_id: RunId::new(run_id),
            parent_run_id: None,
            prompt_release_id: None,
            agent_role_id: None,
        }));
        log.append(std::slice::from_ref(&evt))
            .await
            .expect("seed run_created");
    }

    // ── session-extension columns round-trip ────────────────────────────────

    #[tokio::test]
    async fn test_f65_session_extension_columns_round_trip() {
        let (adapter, log) = fresh_sqlite().await;
        let mem = InMemoryStore::new();
        let session_id = "sess_f65_round_trip";

        seed_session(&log, session_id).await;
        seed_session(&mem, session_id).await;

        // SessionAttemptStarted must be idempotent on replay but should
        // monotonically advance attempts_used. Send attempt 1, then replay
        // attempt 2 — record should converge at 2.
        let started_1 = env(RuntimeEvent::SessionAttemptStarted(SessionAttemptStarted {
            project: project(),
            session_id: SessionId::new(session_id),
            root_run_id: RunId::new("run_root_1"),
            attempt_number: 1,
            max_attempts: 5,
            at_ms: 10,
        }));
        let started_2 = env(RuntimeEvent::SessionAttemptStarted(SessionAttemptStarted {
            project: project(),
            session_id: SessionId::new(session_id),
            root_run_id: RunId::new("run_root_2"),
            attempt_number: 2,
            max_attempts: 5,
            at_ms: 20,
        }));
        log.append(std::slice::from_ref(&started_1)).await.unwrap();
        log.append(std::slice::from_ref(&started_2)).await.unwrap();
        // Replay attempt 1 against in-memory and sqlite — idempotency check.
        mem.append(std::slice::from_ref(&started_1)).await.unwrap();
        mem.append(std::slice::from_ref(&started_2)).await.unwrap();
        mem.append(std::slice::from_ref(&started_1)).await.unwrap();

        let sqlite_rec = SessionReadModel::get(&adapter, &SessionId::new(session_id))
            .await
            .unwrap()
            .expect("session row");
        let mem_rec = SessionReadModel::get(&mem, &SessionId::new(session_id))
            .await
            .unwrap()
            .expect("session row");

        assert_eq!(sqlite_rec.attempts_used, 2, "sqlite attempts_used");
        assert_eq!(mem_rec.attempts_used, 2, "in-memory attempts_used");
        assert_eq!(sqlite_rec.max_attempts, 5);
        assert_eq!(mem_rec.max_attempts, 5);
    }

    // ── checkpoint insert + fetch by session ────────────────────────────────

    #[tokio::test]
    async fn test_f65_checkpoint_insert_and_fetch_by_session() {
        let (adapter, log) = fresh_sqlite().await;
        let mem = InMemoryStore::new();
        let session_id = "sess_ckpt";
        let root_run_id = "run_ckpt_root";

        for backend in [&log as &dyn EventLog, &mem as &dyn EventLog] {
            seed_session(backend, session_id).await;
            seed_run(backend, session_id, root_run_id).await;
        }

        // Three checkpoints at iterations 0, 1, 2.
        for (i, ck) in ["ck_0", "ck_1", "ck_2"].iter().enumerate() {
            let evt = env(RuntimeEvent::CheckpointPersisted(CheckpointPersisted {
                project: project(),
                checkpoint_id: CheckpointId::new(*ck),
                session_id: SessionId::new(session_id),
                root_run_id: RunId::new(root_run_id),
                iteration: i as u32,
                at_ms: 100 + (i as u64) * 10,
            }));
            log.append(std::slice::from_ref(&evt)).await.unwrap();
            mem.append(std::slice::from_ref(&evt)).await.unwrap();
        }

        let sqlite_list = F65CheckpointReadModel::list_by_session(
            &adapter,
            &project(),
            &SessionId::new(session_id),
        )
        .await
        .unwrap();
        let mem_list =
            F65CheckpointReadModel::list_by_session(&mem, &project(), &SessionId::new(session_id))
                .await
                .unwrap();
        assert_eq!(sqlite_list.len(), 3, "sqlite checkpoint count");
        assert_eq!(mem_list.len(), 3, "in-memory checkpoint count");
        // Returned in iteration-ascending order.
        for (i, rec) in sqlite_list.iter().enumerate() {
            assert_eq!(rec.iteration, i as u32);
            assert_eq!(rec.session_id.as_str(), session_id);
            assert_eq!(rec.schema_version, 1);
        }
        for (i, rec) in mem_list.iter().enumerate() {
            assert_eq!(rec.iteration, i as u32);
        }

        // Single-id lookup.
        let sqlite_hit =
            F65CheckpointReadModel::get_f65(&adapter, &project(), &CheckpointId::new("ck_1"))
                .await
                .unwrap()
                .expect("ck_1 present");
        assert_eq!(sqlite_hit.iteration, 1);
        let mem_hit = F65CheckpointReadModel::get_f65(&mem, &project(), &CheckpointId::new("ck_1"))
            .await
            .unwrap()
            .expect("ck_1 present");
        assert_eq!(mem_hit.iteration, 1);
    }

    // ── snapshot + parent-snapshot lineage walk ────────────────────────────

    #[tokio::test]
    async fn test_f65_workspace_snapshot_with_parent_lineage() {
        let (adapter, log) = fresh_sqlite().await;
        let mem = InMemoryStore::new();
        let session_id = "sess_snap";

        seed_session(&log, session_id).await;
        seed_session(&mem, session_id).await;

        // Three-generation snapshot chain. #482 now carries
        // `parent_snapshot_id` on the `WorkspaceSnapshotCreated` event
        // itself, so the projection row is fully populated after the
        // event-log append — no post-insert UPDATEs needed.
        for (sid, parent) in [
            ("snap_root", None::<&str>),
            ("snap_mid", Some("snap_root")),
            ("snap_leaf", Some("snap_mid")),
        ] {
            let evt = env(RuntimeEvent::WorkspaceSnapshotCreated(
                WorkspaceSnapshotCreated {
                    project: project(),
                    snapshot_id: WorkspaceSnapshotId::new(sid),
                    workspace_id: WorkspaceId::new("ws_live"),
                    session_id: SessionId::new(session_id),
                    at_ms: 1_000,
                    bytes: 0,
                    reflink_used: false,
                    parent_snapshot_id: parent.map(WorkspaceSnapshotId::new),
                },
            ));
            log.append(std::slice::from_ref(&evt)).await.unwrap();
            mem.append(std::slice::from_ref(&evt)).await.unwrap();
        }

        // Lineage walk validation. The parent pointer now rides on the
        // event itself (#482), so both sqlite and in-memory backends
        // have the column populated at insert time — the walker runs
        // cross-backend.

        let sqlite_lineage = WorkspaceSnapshotReadModel::lineage(
            &adapter,
            &project(),
            &WorkspaceSnapshotId::new("snap_leaf"),
        )
        .await
        .unwrap();
        assert_eq!(
            sqlite_lineage
                .iter()
                .map(|r| r.snapshot_id.as_str().to_owned())
                .collect::<Vec<_>>(),
            vec!["snap_leaf", "snap_mid", "snap_root"],
        );

        // #482: the in-memory walker now sees the same parent chain —
        // before this landing the InMemoryStore row stayed at
        // `parent_snapshot_id: None` because the event didn't carry it.
        let mem_lineage = WorkspaceSnapshotReadModel::lineage(
            &mem,
            &project(),
            &WorkspaceSnapshotId::new("snap_leaf"),
        )
        .await
        .unwrap();
        assert_eq!(
            mem_lineage
                .iter()
                .map(|r| r.snapshot_id.as_str().to_owned())
                .collect::<Vec<_>>(),
            vec!["snap_leaf", "snap_mid", "snap_root"],
        );

        // List-by-session returns all three in created_at order.
        let sqlite_list = WorkspaceSnapshotReadModel::list_by_session(
            &adapter,
            &project(),
            &SessionId::new(session_id),
        )
        .await
        .unwrap();
        assert_eq!(sqlite_list.len(), 3);

        let mem_list = WorkspaceSnapshotReadModel::list_by_session(
            &mem,
            &project(),
            &SessionId::new(session_id),
        )
        .await
        .unwrap();
        assert_eq!(mem_list.len(), 3);

        // Single-id lookup works cross-backend.
        let sqlite_hit = WorkspaceSnapshotReadModel::get(
            &adapter,
            &project(),
            &WorkspaceSnapshotId::new("snap_root"),
        )
        .await
        .unwrap();
        let mem_hit = WorkspaceSnapshotReadModel::get(
            &mem,
            &project(),
            &WorkspaceSnapshotId::new("snap_root"),
        )
        .await
        .unwrap();
        assert!(sqlite_hit.is_some());
        assert!(mem_hit.is_some());
    }

    /// #482: WorkspaceSnapshotCreated must carry bytes / reflink_used /
    /// parent_snapshot_id so a replay from an empty projection store
    /// rebuilds the row with the SAME metadata the live writer produced.
    /// Pre-#482, the projection inserted these columns at 0 / FALSE /
    /// NULL and waited for an out-of-band stamp_metadata call that
    /// never fires on replay.
    #[tokio::test]
    async fn test_workspace_snapshot_created_carries_metadata_across_backends() {
        let (adapter, log) = fresh_sqlite().await;
        let mem = InMemoryStore::new();
        seed_session(&log, "sess_meta").await;
        seed_session(&mem, "sess_meta").await;

        let parent = WorkspaceSnapshotId::new("snap_parent");
        // Seed the parent first so the lineage pointer resolves.
        let parent_evt = env(RuntimeEvent::WorkspaceSnapshotCreated(
            WorkspaceSnapshotCreated {
                project: project(),
                snapshot_id: parent.clone(),
                workspace_id: WorkspaceId::new("ws_meta"),
                session_id: SessionId::new("sess_meta"),
                at_ms: 100,
                bytes: 0,
                reflink_used: false,
                parent_snapshot_id: None,
            },
        ));
        log.append(std::slice::from_ref(&parent_evt)).await.unwrap();
        mem.append(std::slice::from_ref(&parent_evt)).await.unwrap();

        // The payload carries concrete metadata — pre-#482, these would
        // have been dropped by the projection and zeros landed in the
        // row regardless of what the writer observed.
        let child_snap = WorkspaceSnapshotId::new("snap_child");
        let child_evt = env(RuntimeEvent::WorkspaceSnapshotCreated(
            WorkspaceSnapshotCreated {
                project: project(),
                snapshot_id: child_snap.clone(),
                workspace_id: WorkspaceId::new("ws_meta"),
                session_id: SessionId::new("sess_meta"),
                at_ms: 200,
                bytes: 42_000,
                reflink_used: true,
                parent_snapshot_id: Some(parent.clone()),
            },
        ));
        log.append(std::slice::from_ref(&child_evt)).await.unwrap();
        mem.append(std::slice::from_ref(&child_evt)).await.unwrap();

        for (label, rec) in [
            (
                "sqlite",
                WorkspaceSnapshotReadModel::get(&adapter, &project(), &child_snap)
                    .await
                    .unwrap()
                    .expect("sqlite row"),
            ),
            (
                "in-memory",
                WorkspaceSnapshotReadModel::get(&mem, &project(), &child_snap)
                    .await
                    .unwrap()
                    .expect("in-memory row"),
            ),
        ] {
            assert_eq!(rec.bytes, 42_000, "{label}: bytes must round-trip on event");
            assert!(
                rec.reflink_used,
                "{label}: reflink_used must round-trip on event"
            );
            assert_eq!(
                rec.parent_snapshot_id.as_ref().map(|p| p.as_str()),
                Some(parent.as_str()),
                "{label}: parent_snapshot_id must round-trip on event"
            );
        }
    }

    // ── snapshot reaping marks reaped_at ───────────────────────────────────

    #[tokio::test]
    async fn test_f65_workspace_snapshot_reaped_sets_reaped_at() {
        let (adapter, log) = fresh_sqlite().await;
        let mem = InMemoryStore::new();
        seed_session(&log, "sess_reap").await;
        seed_session(&mem, "sess_reap").await;

        let created = env(RuntimeEvent::WorkspaceSnapshotCreated(
            WorkspaceSnapshotCreated {
                project: project(),
                snapshot_id: WorkspaceSnapshotId::new("snap_to_reap"),
                workspace_id: WorkspaceId::new("ws_live"),
                session_id: SessionId::new("sess_reap"),
                at_ms: 500,
                bytes: 0,
                reflink_used: false,
                parent_snapshot_id: None,
            },
        ));
        log.append(std::slice::from_ref(&created)).await.unwrap();
        mem.append(std::slice::from_ref(&created)).await.unwrap();

        // Pre-reap: reaped_at is NULL.
        let before = WorkspaceSnapshotReadModel::get(
            &adapter,
            &project(),
            &WorkspaceSnapshotId::new("snap_to_reap"),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(before.reaped_at.is_none());

        let reaped = env(RuntimeEvent::WorkspaceSnapshotReaped(
            WorkspaceSnapshotReaped {
                project: project(),
                snapshot_id: WorkspaceSnapshotId::new("snap_to_reap"),
                at_ms: 9_999,
            },
        ));
        log.append(std::slice::from_ref(&reaped)).await.unwrap();
        mem.append(std::slice::from_ref(&reaped)).await.unwrap();

        let sqlite_after = WorkspaceSnapshotReadModel::get(
            &adapter,
            &project(),
            &WorkspaceSnapshotId::new("snap_to_reap"),
        )
        .await
        .unwrap()
        .unwrap();
        let mem_after = WorkspaceSnapshotReadModel::get(
            &mem,
            &project(),
            &WorkspaceSnapshotId::new("snap_to_reap"),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(sqlite_after.reaped_at, Some(9_999));
        assert_eq!(mem_after.reaped_at, Some(9_999));
    }

    // ── outcome links checkpoint + snapshot ────────────────────────────────

    #[tokio::test]
    async fn test_f65_session_outcome_links_checkpoint_and_snapshot() {
        let (adapter, log) = fresh_sqlite().await;
        let mem = InMemoryStore::new();
        let session_id = "sess_outcome";
        let root_run_id = "run_outcome_root";

        for backend in [&log as &dyn EventLog, &mem as &dyn EventLog] {
            seed_session(backend, session_id).await;
            seed_run(backend, session_id, root_run_id).await;
        }

        // Prerequisites: a checkpoint + a snapshot.
        let ck_evt = env(RuntimeEvent::CheckpointPersisted(CheckpointPersisted {
            project: project(),
            checkpoint_id: CheckpointId::new("ck_final"),
            session_id: SessionId::new(session_id),
            root_run_id: RunId::new(root_run_id),
            iteration: 7,
            at_ms: 700,
        }));
        let snap_evt = env(RuntimeEvent::WorkspaceSnapshotCreated(
            WorkspaceSnapshotCreated {
                project: project(),
                snapshot_id: WorkspaceSnapshotId::new("snap_final"),
                workspace_id: WorkspaceId::new("ws_final"),
                session_id: SessionId::new(session_id),
                at_ms: 750,
                bytes: 0,
                reflink_used: false,
                parent_snapshot_id: None,
            },
        ));
        for evt in [&ck_evt, &snap_evt] {
            log.append(std::slice::from_ref(evt)).await.unwrap();
            mem.append(std::slice::from_ref(evt)).await.unwrap();
        }

        let outcome = SessionOutcome {
            session_id: SessionId::new(session_id),
            root_run_id: RunId::new(root_run_id),
            project: project(),
            checkpoint_id: CheckpointId::new("ck_final"),
            workspace_snapshot_id: Some(WorkspaceSnapshotId::new("snap_final")),
            termination_reason: TerminationReason::CircuitBreakerTripped {
                trip: CircuitBreakerTrip {
                    which: BreakerKind::Tokens,
                    measured: 120_000,
                    limit: 100_000,
                    at_iteration: 7,
                },
            },
            compacted_summary: r#"{"headline":"trip"}"#.to_owned(),
            next_step_hint: Some("retry with smaller context".to_owned()),
            cost_micros: 123_456,
            emitted_at: 800,
        };
        let outcome_evt = env(RuntimeEvent::SessionOutcomeEmitted(SessionOutcomeEmitted {
            project: project(),
            session_id: SessionId::new(session_id),
            root_run_id: RunId::new(root_run_id),
            outcome: outcome.clone(),
            at_ms: 800,
        }));
        log.append(std::slice::from_ref(&outcome_evt))
            .await
            .unwrap();
        mem.append(std::slice::from_ref(&outcome_evt))
            .await
            .unwrap();

        let sqlite_hit = SessionOutcomeReadModel::get_by_root_run(
            &adapter,
            &project(),
            &RunId::new(root_run_id),
        )
        .await
        .unwrap()
        .unwrap();
        let mem_hit =
            SessionOutcomeReadModel::get_by_root_run(&mem, &project(), &RunId::new(root_run_id))
                .await
                .unwrap()
                .unwrap();

        for rec in [&sqlite_hit, &mem_hit] {
            assert_eq!(rec.session_id.as_str(), session_id);
            assert_eq!(rec.checkpoint_id.as_str(), "ck_final");
            assert_eq!(
                rec.workspace_snapshot_id.as_ref().map(|s| s.as_str()),
                Some("snap_final"),
            );
            assert_eq!(rec.compacted_summary, r#"{"headline":"trip"}"#);
            assert_eq!(rec.cost_micros, 123_456);
            assert_eq!(
                rec.next_step_hint.as_deref(),
                Some("retry with smaller context")
            );
            // Copilot #4 regression: verify the full `TerminationReason`
            // payload round-trips through the DB (not just the kind).
            // Previously the readers rehydrated the enum with zero-filled
            // payload fields, which silently misled callers. We now
            // persist `termination_reason_json` alongside the
            // discriminator and rehydrate from it.
            match &rec.termination_reason {
                TerminationReason::CircuitBreakerTripped { trip } => {
                    assert_eq!(trip.which, BreakerKind::Tokens);
                    assert_eq!(trip.measured, 120_000);
                    assert_eq!(trip.limit, 100_000);
                    assert_eq!(trip.at_iteration, 7);
                }
                other => panic!("expected CircuitBreakerTripped, got {other:?}"),
            }
        }

        let sqlite_by_session = SessionOutcomeReadModel::list_by_session(
            &adapter,
            &project(),
            &SessionId::new(session_id),
        )
        .await
        .unwrap();
        assert_eq!(sqlite_by_session.len(), 1);
    }

    // ── nullable snapshot for legacy outcomes ───────────────────────────────

    #[tokio::test]
    async fn test_f65_session_outcome_nullable_snapshot_for_legacy() {
        let (adapter, log) = fresh_sqlite().await;
        let mem = InMemoryStore::new();
        let session_id = "sess_legacy";
        let root_run_id = "run_legacy_root";

        for backend in [&log as &dyn EventLog, &mem as &dyn EventLog] {
            seed_session(backend, session_id).await;
            seed_run(backend, session_id, root_run_id).await;
        }

        // Legacy run: a checkpoint but no workspace snapshot (sandbox
        // didn't exist pre-F65). The outcome must persist with
        // workspace_snapshot_id = NULL — that is the arch-doc §6.3
        // back-compat contract.
        let ck_evt = env(RuntimeEvent::CheckpointPersisted(CheckpointPersisted {
            project: project(),
            checkpoint_id: CheckpointId::new("ck_legacy"),
            session_id: SessionId::new(session_id),
            root_run_id: RunId::new(root_run_id),
            iteration: 0,
            at_ms: 100,
        }));
        log.append(std::slice::from_ref(&ck_evt)).await.unwrap();
        mem.append(std::slice::from_ref(&ck_evt)).await.unwrap();

        let outcome = SessionOutcome {
            session_id: SessionId::new(session_id),
            root_run_id: RunId::new(root_run_id),
            project: project(),
            checkpoint_id: CheckpointId::new("ck_legacy"),
            workspace_snapshot_id: None,
            termination_reason: TerminationReason::CompleteRun,
            compacted_summary: String::new(),
            next_step_hint: None,
            cost_micros: 0,
            emitted_at: 200,
        };
        let outcome_evt = env(RuntimeEvent::SessionOutcomeEmitted(SessionOutcomeEmitted {
            project: project(),
            session_id: SessionId::new(session_id),
            root_run_id: RunId::new(root_run_id),
            outcome: outcome.clone(),
            at_ms: 200,
        }));
        log.append(std::slice::from_ref(&outcome_evt))
            .await
            .unwrap();
        mem.append(std::slice::from_ref(&outcome_evt))
            .await
            .unwrap();

        let sqlite_hit = SessionOutcomeReadModel::get_by_root_run(
            &adapter,
            &project(),
            &RunId::new(root_run_id),
        )
        .await
        .unwrap()
        .unwrap();
        let mem_hit =
            SessionOutcomeReadModel::get_by_root_run(&mem, &project(), &RunId::new(root_run_id))
                .await
                .unwrap()
                .unwrap();

        assert!(sqlite_hit.workspace_snapshot_id.is_none());
        assert!(mem_hit.workspace_snapshot_id.is_none());
        assert!(matches!(
            sqlite_hit.termination_reason,
            TerminationReason::CompleteRun
        ));
        assert!(matches!(
            mem_hit.termination_reason,
            TerminationReason::CompleteRun
        ));
    }

    // ── termination-reason payload round-trips via the json sidecar ────────

    /// The short `termination_reason` discriminator column keeps
    /// `idx_session_outcomes_termination` cheap. The `termination_reason_json`
    /// sidecar carries the full payload (provider error message, breaker
    /// trip fields, crash metadata) so readers get the real thing.
    #[tokio::test]
    async fn test_f65_termination_reason_payload_round_trip_via_json_sidecar() {
        let (adapter, log) = fresh_sqlite().await;
        let session_id = "sess_payload";
        let root_run_id = "run_payload_root";
        seed_session(&log, session_id).await;
        seed_run(&log, session_id, root_run_id).await;

        let ck_evt = env(RuntimeEvent::CheckpointPersisted(CheckpointPersisted {
            project: project(),
            checkpoint_id: CheckpointId::new("ck_payload"),
            session_id: SessionId::new(session_id),
            root_run_id: RunId::new(root_run_id),
            iteration: 0,
            at_ms: 10,
        }));
        log.append(std::slice::from_ref(&ck_evt)).await.unwrap();

        let original_trip = CircuitBreakerTrip {
            which: BreakerKind::Tokens,
            measured: 250_000,
            limit: 200_000,
            at_iteration: 42,
        };
        let outcome = SessionOutcome {
            session_id: SessionId::new(session_id),
            root_run_id: RunId::new(root_run_id),
            project: project(),
            checkpoint_id: CheckpointId::new("ck_payload"),
            workspace_snapshot_id: None,
            termination_reason: TerminationReason::CircuitBreakerTripped {
                trip: original_trip.clone(),
            },
            compacted_summary: String::new(),
            next_step_hint: None,
            cost_micros: 0,
            emitted_at: 20,
        };
        let outcome_evt = env(RuntimeEvent::SessionOutcomeEmitted(SessionOutcomeEmitted {
            project: project(),
            session_id: SessionId::new(session_id),
            root_run_id: RunId::new(root_run_id),
            outcome,
            at_ms: 20,
        }));
        log.append(std::slice::from_ref(&outcome_evt))
            .await
            .unwrap();

        let rec = SessionOutcomeReadModel::get_by_root_run(
            &adapter,
            &project(),
            &RunId::new(root_run_id),
        )
        .await
        .unwrap()
        .unwrap();
        match &rec.termination_reason {
            TerminationReason::CircuitBreakerTripped { trip } => {
                assert_eq!(trip, &original_trip, "trip payload must round-trip intact");
            }
            other => panic!("expected CircuitBreakerTripped, got {other:?}"),
        }

        // Issue #465 no-silent-fallback regression: a NULL json column
        // for a payload-bearing kind (`circuit_breaker_tripped`,
        // `provider_error`, `crashed`) must fail clearly rather than
        // return a fabricated zero-valued record. Previously the reader
        // would silently return `CircuitBreakerTripped { which: Round,
        // measured: 0, limit: 0, at_iteration: 0 }` — indistinguishable
        // from a real trip whose counters happened to be zero, which
        // undermines every operator dashboard that filters by breaker
        // kind. Simulate a NULL via a direct UPDATE and assert the read
        // surfaces `Err(Serialization)` with an actionable message.
        sqlx::query(
            "UPDATE session_outcomes SET termination_reason_json = NULL \
             WHERE root_run_id = ?",
        )
        .bind(root_run_id)
        .execute(adapter.pool())
        .await
        .unwrap();
        let err = SessionOutcomeReadModel::get_by_root_run(
            &adapter,
            &project(),
            &RunId::new(root_run_id),
        )
        .await
        .expect_err(
            "NULL termination_reason_json for kind=circuit_breaker_tripped \
                 must error, not silently fabricate a zero-valued trip",
        );
        let msg = err.to_string();
        assert!(
            msg.contains("circuit_breaker_tripped") && msg.contains("termination_reason_json"),
            "error must name the corrupt column and the affected kind; got: {msg}"
        );
        assert!(
            msg.contains("re-emit SessionOutcomeEmitted") || msg.contains("UPDATE"),
            "error must suggest a remediation; got: {msg}"
        );
    }

    // ── Issue #465: no-silent-fallback on NULL payload json ────────────────

    /// All three payload-bearing `TerminationReason` variants must error
    /// on NULL `termination_reason_json` rather than silently fabricate
    /// a zero-valued / empty-message record. Covers CircuitBreakerTripped,
    /// ProviderError, and Crashed end-to-end through the SQLite adapter.
    #[tokio::test]
    async fn test_issue_465_null_json_fails_closed_for_all_payload_variants() {
        let (adapter, log) = fresh_sqlite().await;
        let session_id = "sess_465_payload_variants";
        seed_session(&log, session_id).await;

        // Three rows, one per payload-bearing kind.
        let cases = [
            (
                "run_465_breaker",
                "ck_465_breaker",
                TerminationReason::CircuitBreakerTripped {
                    trip: CircuitBreakerTrip {
                        which: BreakerKind::WallClock,
                        measured: 900,
                        limit: 600,
                        at_iteration: 9,
                    },
                },
                "circuit_breaker_tripped",
            ),
            (
                "run_465_provider",
                "ck_465_provider",
                TerminationReason::ProviderError {
                    message: "upstream 502 from anthropic".to_owned(),
                },
                "provider_error",
            ),
            (
                "run_465_crashed",
                "ck_465_crashed",
                TerminationReason::Crashed {
                    message: "panic: index out of bounds".to_owned(),
                },
                "crashed",
            ),
        ];

        for (root_run_id, checkpoint_id, reason, discriminator) in cases {
            seed_run(&log, session_id, root_run_id).await;
            let ck = env(RuntimeEvent::CheckpointPersisted(CheckpointPersisted {
                project: project(),
                checkpoint_id: CheckpointId::new(checkpoint_id),
                session_id: SessionId::new(session_id),
                root_run_id: RunId::new(root_run_id),
                iteration: 0,
                at_ms: 100,
            }));
            log.append(std::slice::from_ref(&ck)).await.unwrap();

            let outcome_evt = env(RuntimeEvent::SessionOutcomeEmitted(SessionOutcomeEmitted {
                project: project(),
                session_id: SessionId::new(session_id),
                root_run_id: RunId::new(root_run_id),
                outcome: SessionOutcome {
                    session_id: SessionId::new(session_id),
                    root_run_id: RunId::new(root_run_id),
                    project: project(),
                    checkpoint_id: CheckpointId::new(checkpoint_id),
                    workspace_snapshot_id: None,
                    termination_reason: reason.clone(),
                    compacted_summary: String::new(),
                    next_step_hint: None,
                    cost_micros: 0,
                    emitted_at: 200,
                },
                at_ms: 200,
            }));
            log.append(std::slice::from_ref(&outcome_evt))
                .await
                .unwrap();

            // Baseline: full round-trip with json present works.
            let rec = SessionOutcomeReadModel::get_by_root_run(
                &adapter,
                &project(),
                &RunId::new(root_run_id),
            )
            .await
            .unwrap()
            .unwrap();
            assert_eq!(rec.termination_reason, reason);

            // Simulate data corruption / pre-column legacy row: NULL the
            // json column.
            sqlx::query(
                "UPDATE session_outcomes SET termination_reason_json = NULL \
                 WHERE root_run_id = ?",
            )
            .bind(root_run_id)
            .execute(adapter.pool())
            .await
            .unwrap();

            let err = SessionOutcomeReadModel::get_by_root_run(
                &adapter,
                &project(),
                &RunId::new(root_run_id),
            )
            .await
            .expect_err(&format!(
                "NULL termination_reason_json for kind={discriminator} must error"
            ));
            let msg = err.to_string();
            assert!(
                msg.contains(discriminator),
                "error must name the affected kind '{discriminator}'; got: {msg}"
            );
            assert!(
                msg.contains("termination_reason_json"),
                "error must name the corrupt column; got: {msg}"
            );
            assert!(
                msg.contains("NULL"),
                "error must state the failure mode (NULL); got: {msg}"
            );
            assert!(
                msg.contains("re-emit SessionOutcomeEmitted") || msg.contains("UPDATE"),
                "error must suggest a remediation (re-emit or manual UPDATE); got: {msg}"
            );
        }
    }

    /// The payload-less variants (`CompleteRun`, `LeaseLost`,
    /// `OperatorCancel`) have no data beyond their discriminator — NULL
    /// json is LEGITIMATE for them (legacy rows pre-date the column) and
    /// must still round-trip cleanly. This guards against over-fitting
    /// the #465 fix and breaking pre-column backcompat.
    #[tokio::test]
    async fn test_issue_465_null_json_is_fine_for_payload_less_variants() {
        let (adapter, log) = fresh_sqlite().await;
        let session_id = "sess_465_payload_less";
        seed_session(&log, session_id).await;

        let cases = [
            (
                "run_465_complete",
                "ck_465_complete",
                TerminationReason::CompleteRun,
            ),
            ("run_465_lost", "ck_465_lost", TerminationReason::LeaseLost),
            (
                "run_465_cancel",
                "ck_465_cancel",
                TerminationReason::OperatorCancel,
            ),
        ];

        for (root_run_id, checkpoint_id, reason) in cases {
            seed_run(&log, session_id, root_run_id).await;
            let ck = env(RuntimeEvent::CheckpointPersisted(CheckpointPersisted {
                project: project(),
                checkpoint_id: CheckpointId::new(checkpoint_id),
                session_id: SessionId::new(session_id),
                root_run_id: RunId::new(root_run_id),
                iteration: 0,
                at_ms: 100,
            }));
            log.append(std::slice::from_ref(&ck)).await.unwrap();

            let outcome_evt = env(RuntimeEvent::SessionOutcomeEmitted(SessionOutcomeEmitted {
                project: project(),
                session_id: SessionId::new(session_id),
                root_run_id: RunId::new(root_run_id),
                outcome: SessionOutcome {
                    session_id: SessionId::new(session_id),
                    root_run_id: RunId::new(root_run_id),
                    project: project(),
                    checkpoint_id: CheckpointId::new(checkpoint_id),
                    workspace_snapshot_id: None,
                    termination_reason: reason.clone(),
                    compacted_summary: String::new(),
                    next_step_hint: None,
                    cost_micros: 0,
                    emitted_at: 200,
                },
                at_ms: 200,
            }));
            log.append(std::slice::from_ref(&outcome_evt))
                .await
                .unwrap();

            sqlx::query(
                "UPDATE session_outcomes SET termination_reason_json = NULL \
                 WHERE root_run_id = ?",
            )
            .bind(root_run_id)
            .execute(adapter.pool())
            .await
            .unwrap();

            let rec = SessionOutcomeReadModel::get_by_root_run(
                &adapter,
                &project(),
                &RunId::new(root_run_id),
            )
            .await
            .expect("NULL json is fine for payload-less variants")
            .expect("row exists");
            assert_eq!(
                rec.termination_reason, reason,
                "payload-less variant must round-trip from discriminator alone"
            );
        }
    }

    /// Malformed (non-NULL but unparseable) json for a payload kind
    /// must also fail clearly. Covers the case where a bad writer or
    /// manual UPDATE corrupts the column with garbage text.
    #[tokio::test]
    async fn test_issue_465_malformed_json_fails_closed() {
        let (adapter, log) = fresh_sqlite().await;
        let session_id = "sess_465_garbage";
        let root_run_id = "run_465_garbage";
        seed_session(&log, session_id).await;
        seed_run(&log, session_id, root_run_id).await;

        let ck = env(RuntimeEvent::CheckpointPersisted(CheckpointPersisted {
            project: project(),
            checkpoint_id: CheckpointId::new("ck_465_garbage"),
            session_id: SessionId::new(session_id),
            root_run_id: RunId::new(root_run_id),
            iteration: 0,
            at_ms: 100,
        }));
        log.append(std::slice::from_ref(&ck)).await.unwrap();

        let outcome_evt = env(RuntimeEvent::SessionOutcomeEmitted(SessionOutcomeEmitted {
            project: project(),
            session_id: SessionId::new(session_id),
            root_run_id: RunId::new(root_run_id),
            outcome: SessionOutcome {
                session_id: SessionId::new(session_id),
                root_run_id: RunId::new(root_run_id),
                project: project(),
                checkpoint_id: CheckpointId::new("ck_465_garbage"),
                workspace_snapshot_id: None,
                termination_reason: TerminationReason::ProviderError {
                    message: "real message".to_owned(),
                },
                compacted_summary: String::new(),
                next_step_hint: None,
                cost_micros: 0,
                emitted_at: 200,
            },
            at_ms: 200,
        }));
        log.append(std::slice::from_ref(&outcome_evt))
            .await
            .unwrap();

        // Corrupt the json column with a non-NULL garbage value.
        sqlx::query(
            "UPDATE session_outcomes SET termination_reason_json = 'not-json-at-all' \
             WHERE root_run_id = ?",
        )
        .bind(root_run_id)
        .execute(adapter.pool())
        .await
        .unwrap();

        let err = SessionOutcomeReadModel::get_by_root_run(
            &adapter,
            &project(),
            &RunId::new(root_run_id),
        )
        .await
        .expect_err("malformed json must error");
        let msg = err.to_string();
        assert!(
            msg.contains("could not be parsed"),
            "error must name the parse failure; got: {msg}"
        );
        assert!(
            msg.contains("provider_error"),
            "error must name the affected kind; got: {msg}"
        );
    }

    /// Regression guard for the cursor-bugbot #544 finding: malformed
    /// JSON for a payload-LESS variant (`complete_run`, `lease_lost`,
    /// `operator_cancel`) must NOT fail — the discriminator column
    /// already carries the full meaning, so JSON corruption on a
    /// redundant column is not information loss. The function doc
    /// states this explicitly ("NULL / missing / malformed JSON is
    /// fine; the returned value carries no payload") so the behaviour
    /// must match the contract.
    #[tokio::test]
    async fn test_issue_465_malformed_json_is_fine_for_payload_less_variants() {
        let (adapter, log) = fresh_sqlite().await;
        let session_id = "sess_465_garbage_payloadless";
        seed_session(&log, session_id).await;

        let cases = [
            (
                "run_465_cr_garbage",
                "ck_465_cr_garbage",
                TerminationReason::CompleteRun,
            ),
            (
                "run_465_ll_garbage",
                "ck_465_ll_garbage",
                TerminationReason::LeaseLost,
            ),
            (
                "run_465_oc_garbage",
                "ck_465_oc_garbage",
                TerminationReason::OperatorCancel,
            ),
        ];

        for (root_run_id, checkpoint_id, reason) in cases {
            seed_run(&log, session_id, root_run_id).await;
            let ck = env(RuntimeEvent::CheckpointPersisted(CheckpointPersisted {
                project: project(),
                checkpoint_id: CheckpointId::new(checkpoint_id),
                session_id: SessionId::new(session_id),
                root_run_id: RunId::new(root_run_id),
                iteration: 0,
                at_ms: 100,
            }));
            log.append(std::slice::from_ref(&ck)).await.unwrap();

            let outcome_evt = env(RuntimeEvent::SessionOutcomeEmitted(SessionOutcomeEmitted {
                project: project(),
                session_id: SessionId::new(session_id),
                root_run_id: RunId::new(root_run_id),
                outcome: SessionOutcome {
                    session_id: SessionId::new(session_id),
                    root_run_id: RunId::new(root_run_id),
                    project: project(),
                    checkpoint_id: CheckpointId::new(checkpoint_id),
                    workspace_snapshot_id: None,
                    termination_reason: reason.clone(),
                    compacted_summary: String::new(),
                    next_step_hint: None,
                    cost_micros: 0,
                    emitted_at: 200,
                },
                at_ms: 200,
            }));
            log.append(std::slice::from_ref(&outcome_evt))
                .await
                .unwrap();

            // Corrupt the json column with garbage. For payload-less
            // variants this must NOT error — the discriminator column
            // already carries the full answer.
            sqlx::query(
                "UPDATE session_outcomes SET termination_reason_json = '{bogus-not-json' \
                 WHERE root_run_id = ?",
            )
            .bind(root_run_id)
            .execute(adapter.pool())
            .await
            .unwrap();

            let rec = SessionOutcomeReadModel::get_by_root_run(
                &adapter,
                &project(),
                &RunId::new(root_run_id),
            )
            .await
            .expect("malformed json is fine for payload-less variants")
            .expect("row exists");
            assert_eq!(
                rec.termination_reason, reason,
                "payload-less variant must round-trip from discriminator even when json column is garbage"
            );
        }
    }

    // ── outcome replay is idempotent (no row duplication) ──────────────────

    #[tokio::test]
    async fn test_f65_session_outcome_replay_is_idempotent() {
        let (adapter, log) = fresh_sqlite().await;
        let session_id = "sess_dup";
        let root_run_id = "run_dup_root";
        seed_session(&log, session_id).await;
        seed_run(&log, session_id, root_run_id).await;

        let ck_evt = env(RuntimeEvent::CheckpointPersisted(CheckpointPersisted {
            project: project(),
            checkpoint_id: CheckpointId::new("ck_dup"),
            session_id: SessionId::new(session_id),
            root_run_id: RunId::new(root_run_id),
            iteration: 0,
            at_ms: 10,
        }));
        log.append(std::slice::from_ref(&ck_evt)).await.unwrap();

        // First emission.
        let outcome = SessionOutcome {
            session_id: SessionId::new(session_id),
            root_run_id: RunId::new(root_run_id),
            project: project(),
            checkpoint_id: CheckpointId::new("ck_dup"),
            workspace_snapshot_id: None,
            termination_reason: TerminationReason::CompleteRun,
            compacted_summary: "v1".to_owned(),
            next_step_hint: None,
            cost_micros: 10,
            emitted_at: 20,
        };
        let first = env(RuntimeEvent::SessionOutcomeEmitted(SessionOutcomeEmitted {
            project: project(),
            session_id: SessionId::new(session_id),
            root_run_id: RunId::new(root_run_id),
            outcome: outcome.clone(),
            at_ms: 20,
        }));
        log.append(std::slice::from_ref(&first)).await.unwrap();

        // Second emission with an enriched compacted_summary — on-conflict
        // upsert should converge on the later value.
        let mut outcome_v2 = outcome.clone();
        outcome_v2.compacted_summary = "v2-enriched".to_owned();
        outcome_v2.cost_micros = 42;
        let second = env(RuntimeEvent::SessionOutcomeEmitted(SessionOutcomeEmitted {
            project: project(),
            session_id: SessionId::new(session_id),
            root_run_id: RunId::new(root_run_id),
            outcome: outcome_v2.clone(),
            at_ms: 30,
        }));
        log.append(std::slice::from_ref(&second)).await.unwrap();

        let rows: (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM session_outcomes WHERE root_run_id = ?")
                .bind(root_run_id)
                .fetch_one(adapter.pool())
                .await
                .unwrap();
        assert_eq!(rows.0, 1, "upsert must not duplicate row");

        let rec = SessionOutcomeReadModel::get_by_root_run(
            &adapter,
            &project(),
            &RunId::new(root_run_id),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(rec.compacted_summary, "v2-enriched");
        assert_eq!(rec.cost_micros, 42);
    }

    // ── pg ↔ sqlite parity on the new tables + session columns ──────────────

    /// Mechanical parser-level parity for the F65 additions.
    ///
    /// The generic `schema_parity.rs` test asserts identical **table sets**
    /// across backends. This test narrows the scope to the specific F65
    /// table names + session column-set assertions — gives a faster signal
    /// when only the F65 migration drifts.
    #[test]
    fn test_f65_pg_sqlite_schema_parity() {
        use std::fs;
        use std::path::PathBuf;

        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let sqlite_schema = fs::read_to_string(root.join("src/sqlite/schema.rs")).unwrap();
        let pg_v031 =
            fs::read_to_string(root.join("src/pg/migrations/V031__f65_session_extensions.sql"))
                .unwrap();
        let pg_v032 = fs::read_to_string(
            root.join("src/pg/migrations/V032__f65_orchestrator_projections.sql"),
        )
        .unwrap();

        // Session extension columns — each must appear in both the pg
        // migration and the sqlite schema. The column-kind mismatch
        // (BIGINT vs INTEGER, DOUBLE PRECISION vs REAL) is deliberate and
        // documented — we check names only.
        for col in [
            "goal_title",
            "max_attempts",
            "attempts_used",
            "wall_clock_ms_cap",
            "wall_clock_ms_used",
            "token_cap",
            "tokens_used",
            "cost_usd_cap",
            "cost_usd_used",
        ] {
            assert!(pg_v031.contains(col), "pg V031 missing sessions.{col}");
            assert!(
                sqlite_schema.contains(col),
                "sqlite SCHEMA_SQL missing sessions.{col}"
            );
        }

        // F65 tables — each must CREATE TABLE in both backends.
        for tbl in [
            "workspace_registry",
            "workspace_snapshots",
            "session_outcomes",
        ] {
            assert!(pg_v032.contains(tbl), "pg V032 missing CREATE TABLE {tbl}");
            assert!(
                sqlite_schema.contains(tbl),
                "sqlite SCHEMA_SQL missing CREATE TABLE {tbl}"
            );
        }

        // session_outcomes payload columns — both discriminators must be
        // present on both backends so rehydration works cross-backend.
        for col in ["termination_reason", "termination_reason_json"] {
            assert!(
                pg_v032.contains(col),
                "pg V032 missing session_outcomes.{col}"
            );
            assert!(
                sqlite_schema.contains(col),
                "sqlite SCHEMA_SQL missing session_outcomes.{col}"
            );
        }

        // Checkpoint F65 columns — nullable ALTER TABLE cols on the
        // existing table, present in both backends.
        for col in [
            "session_id",
            "schema_version",
            "body",
            "body_size_bytes",
            "iteration",
        ] {
            assert!(pg_v032.contains(col), "pg V032 missing checkpoints.{col}");
            assert!(
                sqlite_schema.contains(col),
                "sqlite SCHEMA_SQL missing checkpoints.{col}"
            );
        }

        // Portability guards — no JSONB, no arrays, no advisory locks,
        // no LISTEN/NOTIFY. A failure here means someone reached for a
        // Postgres-specific feature in the F65 migration (project memory
        // `feedback_no_db_specific_features`).
        //
        // Strip `-- …` line comments before scanning so the docstring
        // inside the migration (which intentionally mentions "no JSONB
        // / arrays / ...") does not false-positive. Block comments
        // (`/* … */`) aren't used in these migrations.
        fn strip_line_comments(sql: &str) -> String {
            sql.lines()
                .map(|line| match line.find("--") {
                    Some(i) => &line[..i],
                    None => line,
                })
                .collect::<Vec<_>>()
                .join("\n")
        }
        let pg_v031_exec = strip_line_comments(&pg_v031);
        let pg_v032_exec = strip_line_comments(&pg_v032);
        for banned in ["JSONB", "SERIAL", "pg_advisory", "LISTEN ", "NOTIFY "] {
            assert!(
                !pg_v031_exec.to_uppercase().contains(banned),
                "V031 uses Postgres-specific feature: {banned}"
            );
            assert!(
                !pg_v032_exec.to_uppercase().contains(banned),
                "V032 uses Postgres-specific feature: {banned}"
            );
        }
        // Array column syntax — specifically `text[]` / `int[]` etc. —
        // matched case-insensitively. Applied to the comment-stripped
        // SQL so docstrings that name array types as prohibited are
        // not themselves flagged.
        for line in pg_v031_exec.lines().chain(pg_v032_exec.lines()) {
            let up = line.to_uppercase();
            assert!(
                !up.contains("TEXT[]")
                    && !up.contains("INT[]")
                    && !up.contains("BIGINT[]")
                    && !up.contains("DOUBLE PRECISION[]"),
                "F65 migration uses a Postgres array column: {line}"
            );
        }
    }
}
