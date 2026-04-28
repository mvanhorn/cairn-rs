//! `#[ignore]`-by-default latency bench for the batched event-log INSERT
//! path (#495 / #496).
//!
//! The bench measures wall-clock time for an N-event burst through two
//! implementations that differ only in the INSERT shape:
//!
//! 1. **per-event** — one `INSERT ... RETURNING position` per event
//!    inside a single transaction (the pre-#495 / pre-#496 code path).
//! 2. **batched** — one multi-row `INSERT ... VALUES (…),(…),…
//!    RETURNING position, event_id` built via sqlx QueryBuilder (the
//!    post-fix path).
//!
//! Projection dispatch is elided from both sides so the comparison
//! measures the INSERT shape in isolation. Projection runs N queries
//! per call in both the old and new code — it would add the same
//! constant to both means and dilute the delta we're actually proving.
//! End-to-end projection coverage is in `event_log_batch_append.rs`.
//!
//! Run with:
//!
//! ```bash
//! cargo test -p cairn-store --features sqlite --test event_log_batch_bench \
//!     --release -- --ignored --nocapture
//! ```
//!
//! Numbers on SQLite `:memory:` (no RTT, no fsync) are modest — the
//! real win shows up on RTT-dominated backends (Postgres over TCP).
//! The PR body contains before/after numbers for the Pg path.

#![cfg(feature = "sqlite")]

use cairn_domain::{
    events::SessionCreated, EventEnvelope, EventId, EventSource, ProjectId, ProjectKey,
    RuntimeEvent, SessionId, TenantId, WorkspaceId,
};
use cairn_store::sqlite::SqliteAdapter;
use sqlx::{QueryBuilder, SqlitePool};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

fn project(suffix: &str) -> ProjectKey {
    ProjectKey {
        tenant_id: TenantId::new(format!("t_bench_{suffix}")),
        workspace_id: WorkspaceId::new(format!("w_bench_{suffix}")),
        project_id: ProjectId::new(format!("p_bench_{suffix}")),
    }
}

fn burst(n: usize, tag: &str, project: &ProjectKey) -> Vec<EventEnvelope<RuntimeEvent>> {
    (0..n)
        .map(|i| {
            EventEnvelope::for_runtime_event(
                EventId::new(format!("evt_{tag}_{i:05}")),
                EventSource::Runtime,
                RuntimeEvent::SessionCreated(SessionCreated {
                    project: project.clone(),
                    session_id: SessionId::new(format!("sess_{tag}_{i:05}")),
                }),
            )
        })
        .collect()
}

fn source_type_str(source: &cairn_domain::EventSource) -> &'static str {
    match source {
        cairn_domain::EventSource::Operator { .. } => "operator",
        cairn_domain::EventSource::Runtime => "runtime",
        cairn_domain::EventSource::Scheduler => "scheduler",
        cairn_domain::EventSource::ExternalWorker { .. } => "external_worker",
        cairn_domain::EventSource::System => "system",
    }
}

/// Per-event INSERT path (pre-#495 / pre-#496 shape). One INSERT
/// RETURNING per event, all inside one transaction.
async fn per_event_insert(
    pool: &SqlitePool,
    events: &[EventEnvelope<RuntimeEvent>],
) -> Result<(), Box<dyn std::error::Error>> {
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as i64;

    let mut tx = pool.begin().await?;

    for event in events {
        let source_meta = serde_json::to_string(&event.source)?;
        let ownership = serde_json::to_string(&event.ownership)?;
        let payload = serde_json::to_string(&event.payload)?;

        let _row: (i64,) = sqlx::query_as(
            "INSERT INTO event_log (event_id, source_type, source_meta, ownership, causation_id, correlation_id, payload, stored_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
             RETURNING position",
        )
        .bind(event.event_id.as_str())
        .bind(source_type_str(&event.source))
        .bind(&source_meta)
        .bind(&ownership)
        .bind(event.causation_id.as_ref().map(|id| id.as_str()))
        .bind(event.correlation_id.as_deref())
        .bind(&payload)
        .bind(now)
        .fetch_one(&mut *tx)
        .await?;
    }

    tx.commit().await?;
    Ok(())
}

/// Batched multi-row INSERT path (post-fix shape). Same final commit
/// boundary, but one round-trip of SQL per call.
async fn batched_insert(
    pool: &SqlitePool,
    events: &[EventEnvelope<RuntimeEvent>],
) -> Result<(), Box<dyn std::error::Error>> {
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as i64;

    struct Row<'a> {
        event_id: &'a str,
        source_type: &'static str,
        source_meta: String,
        ownership: String,
        causation_id: Option<&'a str>,
        correlation_id: Option<&'a str>,
        payload: String,
    }

    let mut rows: Vec<Row<'_>> = Vec::with_capacity(events.len());
    for event in events {
        rows.push(Row {
            event_id: event.event_id.as_str(),
            source_type: source_type_str(&event.source),
            source_meta: serde_json::to_string(&event.source)?,
            ownership: serde_json::to_string(&event.ownership)?,
            causation_id: event.causation_id.as_ref().map(|id| id.as_str()),
            correlation_id: event.correlation_id.as_deref(),
            payload: serde_json::to_string(&event.payload)?,
        });
    }

    let mut tx = pool.begin().await?;

    let mut builder: QueryBuilder<'_, sqlx::Sqlite> = QueryBuilder::new(
        "INSERT INTO event_log (event_id, source_type, source_meta, ownership, causation_id, correlation_id, payload, stored_at) ",
    );
    builder.push_values(rows.iter(), |mut b, row| {
        b.push_bind(row.event_id)
            .push_bind(row.source_type)
            .push_bind(&row.source_meta)
            .push_bind(&row.ownership)
            .push_bind(row.causation_id)
            .push_bind(row.correlation_id)
            .push_bind(&row.payload)
            .push_bind(now);
    });
    builder.push(" RETURNING position, event_id");

    let _returned: Vec<(i64, String)> = builder.build_query_as().fetch_all(&mut *tx).await?;

    tx.commit().await?;
    Ok(())
}

/// Measure both INSERT paths across a range of burst sizes so the win
/// vs. per-event RTT count is visible. Prints a small table to stdout
/// ready for the PR body.
#[tokio::test]
#[ignore = "perf bench — run explicitly with --ignored"]
async fn bench_burst_append_sqlite() {
    let iterations = 30;
    let burst_sizes = [10usize, 50, 100, 250];

    println!("\n== event_log INSERT shape bench (SQLite :memory:, {iterations} iterations) ==");
    println!(
        "{:>6} {:>14} {:>14} {:>10} {:>14} {:>14} {:>10}",
        "N", "per-event p50", "batched  p50", "speedup", "per-event p95", "batched  p95", "speedup"
    );

    for &n in &burst_sizes {
        let mut per_event_us = Vec::with_capacity(iterations);
        let mut batched_us = Vec::with_capacity(iterations);

        for i in 0..iterations {
            let pk = project(&format!("bench_{n}_{i}"));
            let events = burst(n, &format!("n{n}i{i}"), &pk);

            // Fresh DB per iteration — keeps the cold-path cost honest
            // and avoids cross-iteration cache effects.
            let adapter = SqliteAdapter::in_memory().await.expect("sqlite in_memory");
            let t = Instant::now();
            per_event_insert(adapter.pool(), &events)
                .await
                .expect("per-event");
            per_event_us.push(t.elapsed().as_micros());

            let adapter = SqliteAdapter::in_memory().await.expect("sqlite in_memory");
            let t = Instant::now();
            batched_insert(adapter.pool(), &events)
                .await
                .expect("batched");
            batched_us.push(t.elapsed().as_micros());
        }

        per_event_us.sort();
        batched_us.sort();

        let p50 = |v: &[u128]| v[v.len() / 2];
        let p95 = |v: &[u128]| v[(v.len() * 95) / 100];

        let pe_p50 = p50(&per_event_us);
        let b_p50 = p50(&batched_us);
        let pe_p95 = p95(&per_event_us);
        let b_p95 = p95(&batched_us);
        let speedup_p50 = pe_p50 as f64 / b_p50.max(1) as f64;
        let speedup_p95 = pe_p95 as f64 / b_p95.max(1) as f64;

        println!(
            "{:>6} {:>11} µs {:>11} µs {:>9.2}× {:>11} µs {:>11} µs {:>9.2}×",
            n, pe_p50, b_p50, speedup_p50, pe_p95, b_p95, speedup_p95
        );
    }

    println!();
    println!(
        "Note: SQLite :memory: has no RTT and no fsync, so the measured win is\n\
         conservative. The real gain is on Postgres over a TCP socket: every\n\
         RETURNING round trip eliminated saves ~0.2–1 ms of network + parse.\n\
         For a 100-event burst that's 99 fewer round trips and typically 5–20×\n\
         lower tx-hold time under pg_pool contention."
    );
}
