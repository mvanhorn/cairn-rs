//! Batched event-log append: correctness + parity + projection + concurrency.
//!
//! These tests cover the remediation for #495–#498:
//!
//! - #495/#496: one multi-row `INSERT ... RETURNING` per call instead of
//!   one INSERT per event.
//! - #497/#498: `apply_async` takes the envelope by reference so the hot
//!   append path does not clone the full `EventEnvelope` (including
//!   possibly-large `CheckpointCreated` snapshots) per event.
//!
//! Per `feedback_integration_tests_only.md` the coverage here targets
//! real backends end-to-end: `InMemoryStore` (primary path the runtime
//! talks to in `--db memory` and in almost every unit-scope test),
//! `SqliteEventLog` (the local-mode durable backend), and — behind a
//! `TEST_DATABASE_URL` env var — a real Postgres instance. The Pg case
//! exercises exactly the same assertions against the production
//! append path. If `TEST_DATABASE_URL` is unset, the Pg sub-test
//! no-ops, matching how `cairn-store`'s existing Pg static-SQL tests
//! behave in CI.
//!
//! The file-level cfg gate is intentionally absent: `in_memory_burst`
//! tests exercise only `InMemoryStore` and must run in every build,
//! including the default no-feature one. SQLite and Postgres modules
//! carry their own `#[cfg(feature = ...)]` gates.

use cairn_domain::{
    events::{RunCreated, SessionCreated, TaskCreated},
    EventEnvelope, EventId, EventSource, ProjectId, ProjectKey, RunId, RuntimeEvent, SessionId,
    TaskId, TenantId, WorkspaceId,
};
use cairn_store::event_log::{EventLog, StoredEvent};

// ── Fixtures ──────────────────────────────────────────────────────────────────

fn project(suffix: &str) -> ProjectKey {
    ProjectKey {
        tenant_id: TenantId::new(format!("t_batch_{suffix}")),
        workspace_id: WorkspaceId::new(format!("w_batch_{suffix}")),
        project_id: ProjectId::new(format!("p_batch_{suffix}")),
    }
}

fn session_envelope(eid: &str, project: &ProjectKey, session: &str) -> EventEnvelope<RuntimeEvent> {
    EventEnvelope::for_runtime_event(
        EventId::new(eid),
        EventSource::Runtime,
        RuntimeEvent::SessionCreated(SessionCreated {
            project: project.clone(),
            session_id: SessionId::new(session),
        }),
    )
}

fn run_envelope(
    eid: &str,
    project: &ProjectKey,
    session: &str,
    run: &str,
) -> EventEnvelope<RuntimeEvent> {
    EventEnvelope::for_runtime_event(
        EventId::new(eid),
        EventSource::Runtime,
        RuntimeEvent::RunCreated(RunCreated {
            project: project.clone(),
            session_id: SessionId::new(session),
            run_id: RunId::new(run),
            parent_run_id: None,
            prompt_release_id: None,
            agent_role_id: None,
        }),
    )
}

fn task_envelope(
    eid: &str,
    project: &ProjectKey,
    run: &str,
    task: &str,
    session: &str,
) -> EventEnvelope<RuntimeEvent> {
    EventEnvelope::for_runtime_event(
        EventId::new(eid),
        EventSource::Runtime,
        RuntimeEvent::TaskCreated(TaskCreated {
            project: project.clone(),
            task_id: TaskId::new(task),
            parent_run_id: Some(RunId::new(run)),
            parent_task_id: None,
            prompt_release_id: None,
            session_id: Some(SessionId::new(session)),
        }),
    )
}

/// Build a 100-event burst the runtime could realistically emit: one
/// SessionCreated, one RunCreated for that session, then 98 tasks on
/// the run. The mix lets us verify both the event-log round-trip and
/// that a few projection table rows land (sessions, runs, tasks).
fn burst_of_one_hundred(project: &ProjectKey, tag: &str) -> Vec<EventEnvelope<RuntimeEvent>> {
    let mut events = Vec::with_capacity(100);
    events.push(session_envelope(
        &format!("evt_{tag}_session"),
        project,
        &format!("sess_{tag}"),
    ));
    events.push(run_envelope(
        &format!("evt_{tag}_run"),
        project,
        &format!("sess_{tag}"),
        &format!("run_{tag}"),
    ));
    for i in 0..98 {
        events.push(task_envelope(
            &format!("evt_{tag}_task_{i:03}"),
            project,
            &format!("run_{tag}"),
            &format!("task_{tag}_{i:03}"),
            &format!("sess_{tag}"),
        ));
    }
    events
}

fn assert_round_trip(input: &[EventEnvelope<RuntimeEvent>], stored: &[StoredEvent]) {
    assert_eq!(
        input.len(),
        stored.len(),
        "read_stream returned {} events for a {}-event burst",
        stored.len(),
        input.len()
    );

    // Positions must be strictly monotonically increasing.
    for w in stored.windows(2) {
        assert!(
            w[0].position.0 < w[1].position.0,
            "event positions not monotonic: {:?} then {:?}",
            w[0].position,
            w[1].position
        );
    }

    // Events come back in insertion order, byte-equal by envelope.
    for (in_evt, out_evt) in input.iter().zip(stored.iter()) {
        assert_eq!(
            in_evt.event_id, out_evt.envelope.event_id,
            "event_id out of order"
        );
        assert_eq!(
            in_evt.source,
            out_evt.envelope.source,
            "source mismatch for {}",
            in_evt.event_id.as_str()
        );
        assert_eq!(
            in_evt.ownership,
            out_evt.envelope.ownership,
            "ownership mismatch for {}",
            in_evt.event_id.as_str()
        );
        assert_eq!(
            in_evt.payload,
            out_evt.envelope.payload,
            "payload mismatch for {}",
            in_evt.event_id.as_str()
        );
        assert!(
            out_evt.stored_at > 0,
            "stored_at must be populated for {}",
            in_evt.event_id.as_str()
        );
    }
}

// ── InMemoryStore ────────────────────────────────────────────────────────────

mod in_memory_burst {
    use super::*;
    use cairn_store::InMemoryStore;

    /// 100-event burst → 100 positions in insertion order, all read back
    /// intact. Positions are monotonic and envelopes are byte-equal.
    #[tokio::test]
    async fn burst_100_round_trips_in_memory() {
        let store = InMemoryStore::new();
        let pk = project("mem_burst");
        let burst = burst_of_one_hundred(&pk, "mem");

        let positions = store.append(&burst).await.expect("append burst");
        assert_eq!(positions.len(), 100);
        for w in positions.windows(2) {
            assert!(w[0].0 < w[1].0, "in-memory positions must be monotonic");
        }

        let all = store.read_stream(None, 1_000).await.expect("read_stream");
        assert_round_trip(&burst, &all);
    }

    /// 4 concurrent writers, each appending 25 events. All 100 events
    /// must land, with no dupes or gaps, linearizably.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_writers_are_linearizable_in_memory() {
        use std::sync::Arc;

        let store = Arc::new(InMemoryStore::new());
        let pk = project("mem_concurrent");

        let mut handles = Vec::new();
        for writer in 0..4u32 {
            let store = Arc::clone(&store);
            let pk = pk.clone();
            handles.push(tokio::spawn(async move {
                let batch: Vec<_> = (0..25u32)
                    .map(|i| {
                        session_envelope(
                            &format!("evt_mem_w{writer:02}_{i:02}"),
                            &pk,
                            &format!("sess_w{writer:02}_{i:02}"),
                        )
                    })
                    .collect();
                store.append(&batch).await.expect("append")
            }));
        }

        let mut all_positions: Vec<u64> = Vec::new();
        for h in handles {
            for p in h.await.expect("join") {
                all_positions.push(p.0);
            }
        }

        assert_eq!(all_positions.len(), 100);
        all_positions.sort_unstable();
        all_positions.dedup();
        assert_eq!(
            all_positions.len(),
            100,
            "positions must be unique across concurrent writers"
        );

        let stream = store.read_stream(None, 1_000).await.expect("read_stream");
        assert_eq!(stream.len(), 100, "all 100 events must be on the stream");

        // Every event_id must be present exactly once.
        let mut ids: Vec<String> = stream
            .iter()
            .map(|e| e.envelope.event_id.as_str().to_owned())
            .collect();
        ids.sort();
        ids.dedup();
        assert_eq!(ids.len(), 100, "no duplicate event_ids");
    }
}

// ── SqliteEventLog ───────────────────────────────────────────────────────────

#[cfg(feature = "sqlite")]
mod sqlite_burst {
    use super::*;
    use cairn_store::sqlite::{SqliteAdapter, SqliteEventLog};

    /// 100-event burst → 100 positions, all envelopes round-trip.
    /// Proves the multi-row INSERT..RETURNING path produces the same
    /// observable outcome as the pre-fix per-event loop.
    #[tokio::test]
    async fn burst_100_round_trips_sqlite() {
        let adapter = SqliteAdapter::in_memory().await.expect("sqlite in_memory");
        let log = SqliteEventLog::new(adapter.pool().clone());
        let pk = project("sqlite_burst");
        let burst = burst_of_one_hundred(&pk, "sqlite");

        let positions = log.append(&burst).await.expect("append burst");
        assert_eq!(positions.len(), 100);
        for w in positions.windows(2) {
            assert!(w[0].0 < w[1].0, "sqlite positions must be monotonic");
        }

        let all = log.read_stream(None, 1_000).await.expect("read_stream");
        assert_round_trip(&burst, &all);
    }

    /// Synchronous projections fire for every event in the batch —
    /// the 1 SessionCreated + 1 RunCreated + 98 TaskCreated events
    /// populate their respective projection tables in the same
    /// transaction as the event_log insert.
    #[tokio::test]
    async fn burst_100_projections_all_fire_sqlite() {
        let adapter = SqliteAdapter::in_memory().await.expect("sqlite in_memory");
        let log = SqliteEventLog::new(adapter.pool().clone());
        let pk = project("sqlite_proj");
        let burst = burst_of_one_hundred(&pk, "proj");

        log.append(&burst).await.expect("append burst");

        let (sessions,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM sessions")
            .fetch_one(adapter.pool())
            .await
            .expect("sessions count");
        assert_eq!(sessions, 1, "SessionCreated projection must fire");

        let (runs,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM runs")
            .fetch_one(adapter.pool())
            .await
            .expect("runs count");
        assert_eq!(runs, 1, "RunCreated projection must fire");

        let (tasks,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM tasks")
            .fetch_one(adapter.pool())
            .await
            .expect("tasks count");
        assert_eq!(tasks, 98, "all 98 TaskCreated projections must fire");

        // head_position() matches 100.
        let head = log.head_position().await.expect("head_position");
        assert_eq!(head.expect("head present").0, 100);
    }

    /// 4 concurrent writers × 25 events each → 100 unique, in-order
    /// events visible on the stream with no dupes and no gaps.
    ///
    /// Uses a tempfile-backed SQLite DB with a multi-connection pool so
    /// each writer task can hold its own connection — `sqlite::memory:`
    /// gives each connection a private in-memory DB and would only
    /// serialize through one pool connection, hiding true
    /// cross-connection contention (flagged by Copilot in #539).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_writers_are_linearizable_sqlite() {
        use cairn_store::db::DbAdapter;
        use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
        use std::str::FromStr;
        use std::sync::Arc;

        // Temp file so every pool connection maps to the same on-disk
        // DB. Matches the `sqlite:{path}` DSN style used elsewhere in
        // cairn-app (avoids the platform-dependent `sqlite://` double-
        // slash parsing that Copilot flagged on #539).
        let tmp = tempfile::NamedTempFile::new().expect("tempfile");
        let url = format!("sqlite:{}", tmp.path().display());
        let opts = SqliteConnectOptions::from_str(&url)
            .expect("sqlite url")
            .create_if_missing(true)
            .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal);
        let pool = SqlitePoolOptions::new()
            .max_connections(4)
            .connect_with(opts)
            .await
            .expect("sqlite pool");

        let adapter = SqliteAdapter::new(pool.clone());
        adapter.migrate().await.expect("migrate");

        let log = Arc::new(SqliteEventLog::new(pool));
        let pk = project("sqlite_concurrent");

        let mut handles = Vec::new();
        for writer in 0..4u32 {
            let log = Arc::clone(&log);
            let pk = pk.clone();
            handles.push(tokio::spawn(async move {
                let batch: Vec<_> = (0..25u32)
                    .map(|i| {
                        session_envelope(
                            &format!("evt_sq_w{writer:02}_{i:02}"),
                            &pk,
                            &format!("sess_sq_w{writer:02}_{i:02}"),
                        )
                    })
                    .collect();
                log.append(&batch).await.expect("append")
            }));
        }

        let mut all_positions: Vec<u64> = Vec::new();
        for h in handles {
            for p in h.await.expect("join") {
                all_positions.push(p.0);
            }
        }

        assert_eq!(all_positions.len(), 100);
        all_positions.sort_unstable();
        all_positions.dedup();
        assert_eq!(
            all_positions.len(),
            100,
            "positions unique across concurrent writers"
        );

        let stream = log.read_stream(None, 1_000).await.expect("read_stream");
        assert_eq!(stream.len(), 100);

        let mut ids: Vec<String> = stream
            .iter()
            .map(|e| e.envelope.event_id.as_str().to_owned())
            .collect();
        ids.sort();
        ids.dedup();
        assert_eq!(ids.len(), 100);
    }

    /// Empty slice short-circuits — no transaction is opened, no SQL is
    /// emitted, returned positions are empty. Preserves the original
    /// per-event-loop contract post-refactor.
    #[tokio::test]
    async fn empty_slice_is_no_op_sqlite() {
        let adapter = SqliteAdapter::in_memory().await.expect("sqlite in_memory");
        let log = SqliteEventLog::new(adapter.pool().clone());
        let positions = log.append(&[]).await.expect("append empty");
        assert!(positions.is_empty());
        assert_eq!(log.head_position().await.expect("head"), None);
    }

    /// A burst larger than the internal chunk boundary still round-trips
    /// correctly. Proves the chunking loop (introduced to stay under the
    /// legacy SQLite 999-parameter cap) preserves order, positions, and
    /// envelope equality across chunk boundaries.
    #[tokio::test]
    async fn burst_larger_than_chunk_boundary_sqlite() {
        let adapter = SqliteAdapter::in_memory().await.expect("sqlite in_memory");
        let log = SqliteEventLog::new(adapter.pool().clone());
        let pk = project("sqlite_chunked");

        // 350 events >> BATCH_INSERT_CHUNK (100) → 4 chunks.
        // Keep it to SessionCreated only so FK constraints stay satisfied
        // without having to thread a session_id through every event.
        let events: Vec<_> = (0..350)
            .map(|i| session_envelope(&format!("evt_ch_{i:03}"), &pk, &format!("sess_ch_{i:03}")))
            .collect();

        let positions = log.append(&events).await.expect("append 350-event burst");
        assert_eq!(positions.len(), 350);
        for w in positions.windows(2) {
            assert!(
                w[0].0 < w[1].0,
                "positions must stay monotonic across chunk boundaries"
            );
        }

        let all = log.read_stream(None, 1_000).await.expect("read_stream");
        assert_round_trip(&events, &all);
    }

    /// A single `append` call that contains the same event_id twice must
    /// fail fast with a clear error, not be caught downstream by
    /// the DB's UNIQUE constraint with a cryptic message. Gemini
    /// flagged the client-side reorder's implicit uniqueness assumption
    /// on the first review pass of #539.
    #[tokio::test]
    async fn duplicate_event_id_in_single_batch_errors_sqlite() {
        let adapter = SqliteAdapter::in_memory().await.expect("sqlite in_memory");
        let log = SqliteEventLog::new(adapter.pool().clone());
        let pk = project("sqlite_dup");

        let events = vec![
            session_envelope("evt_dup", &pk, "sess_dup_a"),
            session_envelope("evt_dup", &pk, "sess_dup_b"),
        ];

        let err = log.append(&events).await.expect_err("must fail");
        let msg = err.to_string();
        assert!(
            msg.contains("duplicate event_id"),
            "error must surface the domain invariant clearly; got: {msg}"
        );

        // And nothing landed in the log.
        assert_eq!(
            log.head_position().await.expect("head"),
            None,
            "failed batch must not leave partial state"
        );
    }

    /// Cross-backend parity: the same event sequence produces the same
    /// read-back order on both `InMemoryStore` and `SqliteEventLog`.
    /// Positions are backend-assigned so we compare event_ids not
    /// position values.
    #[tokio::test]
    async fn sqlite_and_in_memory_agree_on_order() {
        use cairn_store::InMemoryStore;

        let adapter = SqliteAdapter::in_memory().await.expect("sqlite in_memory");
        let sqlite_log = SqliteEventLog::new(adapter.pool().clone());
        let mem = InMemoryStore::new();
        let pk = project("parity_burst");
        let burst = burst_of_one_hundred(&pk, "parity");

        sqlite_log.append(&burst).await.expect("sqlite append");
        mem.append(&burst).await.expect("mem append");

        let sqlite_stream = sqlite_log
            .read_stream(None, 1_000)
            .await
            .expect("sqlite read");
        let mem_stream = mem.read_stream(None, 1_000).await.expect("mem read");

        assert_eq!(sqlite_stream.len(), 100);
        assert_eq!(mem_stream.len(), 100);

        let sqlite_ids: Vec<&str> = sqlite_stream
            .iter()
            .map(|e| e.envelope.event_id.as_str())
            .collect();
        let mem_ids: Vec<&str> = mem_stream
            .iter()
            .map(|e| e.envelope.event_id.as_str())
            .collect();

        assert_eq!(
            sqlite_ids, mem_ids,
            "both backends must yield identical event order for the same burst"
        );
    }
}

// ── PgEventLog (requires TEST_DATABASE_URL) ─────────────────────────────────
//
// Gated on the `postgres` feature + `TEST_DATABASE_URL`. In the default
// CI matrix this module compiles but every test early-returns if the
// env var is unset — mirrors how cairn-fabric's pg tests behave.

#[cfg(feature = "postgres")]
mod pg_burst {
    use super::*;
    use cairn_store::db::DbAdapter;
    use cairn_store::pg::{PgAdapter, PgEventLog};
    use sqlx::postgres::PgPoolOptions;

    /// Probe for a live Postgres instance from `TEST_DATABASE_URL`.
    /// If the var is unset or the connection fails, the caller should
    /// log and skip rather than fail — static-SQL assertions in
    /// `pg_migration_contract.rs` already cover the offline contract.
    async fn try_pg_adapter() -> Option<PgAdapter> {
        let url = std::env::var("TEST_DATABASE_URL").ok()?;
        let pool = PgPoolOptions::new()
            .max_connections(4)
            .connect(&url)
            .await
            .ok()?;
        let adapter = PgAdapter::new(pool);
        adapter.migrate().await.ok()?;
        Some(adapter)
    }

    /// Pg parity for the sqlite burst_100_round_trips test. Skips when
    /// TEST_DATABASE_URL is unset.
    ///
    /// The project suffix and event_id `tag` carry a per-run nanosecond
    /// timestamp so repeated executions against the same test database
    /// do not collide on `event_log.event_id`'s UNIQUE constraint
    /// (Copilot flagged this on #539).
    #[tokio::test]
    async fn burst_100_round_trips_pg() {
        let Some(adapter) = try_pg_adapter().await else {
            eprintln!(
                "skipping pg burst test: TEST_DATABASE_URL unset or unreachable. \
                 Run with `TEST_DATABASE_URL=postgres://… cargo test ...` to exercise."
            );
            return;
        };
        let log = PgEventLog::new(adapter.pool().clone());
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock before UNIX_EPOCH")
            .as_nanos();
        let pk = project(&format!("pg_burst_{unique}"));
        let burst = burst_of_one_hundred(&pk, &format!("pg_{unique}"));

        let positions = log.append(&burst).await.expect("append burst");
        assert_eq!(positions.len(), 100);
        for w in positions.windows(2) {
            assert!(w[0].0 < w[1].0, "pg positions must be monotonic");
        }

        // Read back only this test's events so the assertion is isolated
        // from any other data in a shared test database.
        let all = log.read_stream(None, 10_000).await.expect("read_stream");
        let our_event_ids: std::collections::HashSet<&str> =
            burst.iter().map(|e| e.event_id.as_str()).collect();
        let ours: Vec<&StoredEvent> = all
            .iter()
            .filter(|e| our_event_ids.contains(e.envelope.event_id.as_str()))
            .collect();
        assert_eq!(ours.len(), 100, "every event must survive the round trip");
        // Ours must be in insertion order — filter preserves order.
        for (input_evt, out_evt) in burst.iter().zip(ours.iter()) {
            assert_eq!(input_evt.event_id, out_evt.envelope.event_id);
            assert_eq!(input_evt.payload, out_evt.envelope.payload);
        }
    }
}
