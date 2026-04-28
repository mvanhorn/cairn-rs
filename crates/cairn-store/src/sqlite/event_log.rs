use async_trait::async_trait;
use sqlx::{QueryBuilder, SqlitePool};
use std::time::{SystemTime, UNIX_EPOCH};

use cairn_domain::{EventEnvelope, RuntimeEvent};

use super::projections::SqliteSyncProjection;
use crate::error::StoreError;
use crate::event_log::{EntityRef, EventLog, EventPosition, StoredEvent};

/// Maximum events per multi-row `INSERT`. SQLite caps host parameters
/// at 32766 on modern builds (≥ 3.32.0) but historically at 999. With
/// 8 columns per event, 100 events × 8 = 800 parameters — safely under
/// the legacy limit and trivially under the modern one, so the same
/// number ports across every SQLite build cairn-rs might run on.
///
/// For typical bursts (≤ 100 events — a checkpoint flush, a tool-result
/// fanout) this is still one statement per call. Larger bursts loop
/// the statement inside the existing transaction; durability and
/// projection atomicity are unchanged.
const BATCH_INSERT_CHUNK: usize = 100;

/// SQLite-backed append-only event log for local-mode.
///
/// Appends events and updates synchronous projections within a single
/// transaction so reads can never observe an event position that hasn't
/// been projected yet. Mirrors the Postgres backend contract.
pub struct SqliteEventLog {
    pool: SqlitePool,
}

impl SqliteEventLog {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl EventLog for SqliteEventLog {
    async fn append(
        &self,
        events: &[EventEnvelope<RuntimeEvent>],
    ) -> Result<Vec<EventPosition>, StoreError> {
        if events.is_empty() {
            return Ok(vec![]);
        }

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;

        // Pre-serialize JSON columns outside the transaction. SQLite stores
        // these as TEXT, not JSONB, so the sqlx bind is just a string.
        struct Row<'a> {
            event_id: &'a str,
            source_type: &'static str,
            source_meta: String,
            ownership: String,
            causation_id: Option<&'a str>,
            correlation_id: Option<&'a str>,
            payload: String,
        }

        // Reject duplicate event_ids *before* touching the database so the
        // caller gets a clear domain error instead of a cryptic
        // `UNIQUE constraint failed` from SQLite. The schema's
        // `event_id TEXT NOT NULL UNIQUE` (sqlite::schema) is the
        // ultimate guard — this check is a defense in depth that also
        // prevents the client-side reorder HashMap (below) from aliasing
        // two input slots to the same position when an upstream bug
        // somehow produces a duplicate.
        let mut seen: std::collections::HashSet<&str> =
            std::collections::HashSet::with_capacity(events.len());
        for event in events {
            if !seen.insert(event.event_id.as_str()) {
                return Err(StoreError::Internal(format!(
                    "duplicate event_id {} in single append batch",
                    event.event_id.as_str()
                )));
            }
        }

        let mut rows: Vec<Row<'_>> = Vec::with_capacity(events.len());
        for event in events {
            rows.push(Row {
                event_id: event.event_id.as_str(),
                source_type: source_type_str(&event.source),
                source_meta: serde_json::to_string(&event.source)
                    .map_err(|e| StoreError::Serialization(e.to_string()))?,
                ownership: serde_json::to_string(&event.ownership)
                    .map_err(|e| StoreError::Serialization(e.to_string()))?,
                causation_id: event.causation_id.as_ref().map(|id| id.as_str()),
                correlation_id: event.correlation_id.as_deref(),
                payload: serde_json::to_string(&event.payload)
                    .map_err(|e| StoreError::Serialization(e.to_string()))?,
            });
        }

        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| StoreError::Connection(e.to_string()))?;

        // ── Batched multi-row INSERT ──────────────────────────────────────
        //
        // Invariants enforced by this block (mirrors the Pg backend):
        //
        // 1. **One transaction per call.** All chunks commit together;
        //    partial batches must never be observable.
        // 2. **Portable SQL shape.** `INSERT ... VALUES (…),(…),… RETURNING
        //    position, event_id` (SQLite ≥ 3.35). The same shape works
        //    on Postgres — required by the "no DB-specific features"
        //    rule.
        // 3. **Client-side reorder via `event_id`.** The SQL spec does
        //    not guarantee RETURNING row order; we re-map against each
        //    input event's event_id to stay correct even if a future
        //    engine returns rows out of insertion order.
        // 4. **Chunk size ≤ `BATCH_INSERT_CHUNK`.** Keeps the host-
        //    parameter count below the legacy SQLite 999 cap (and
        //    trivially below Postgres 65535). All chunks share the
        //    enclosing transaction; projection atomicity is unchanged.
        // 5. **WAL-friendly.** One batched INSERT per call replaces N
        //    per-event statements, collapsing N fsync points into one
        //    on `synchronous=FULL` and reducing page-lock contention
        //    under `synchronous=NORMAL`.
        let mut all_returned: Vec<(i64, String)> = Vec::with_capacity(events.len());

        for chunk in rows.chunks(BATCH_INSERT_CHUNK) {
            let mut builder: QueryBuilder<'_, sqlx::Sqlite> = QueryBuilder::new(
                "INSERT INTO event_log (event_id, source_type, source_meta, ownership, causation_id, correlation_id, payload, stored_at) ",
            );
            builder.push_values(chunk.iter(), |mut b, row| {
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

            let returned: Vec<(i64, String)> = builder
                .build_query_as()
                .fetch_all(&mut *tx)
                .await
                .map_err(|e| StoreError::Internal(e.to_string()))?;

            if returned.len() != chunk.len() {
                return Err(StoreError::Internal(format!(
                    "event_log batch INSERT returned {} rows for {} events in chunk",
                    returned.len(),
                    chunk.len()
                )));
            }

            all_returned.extend(returned);
        }

        if all_returned.len() != events.len() {
            return Err(StoreError::Internal(format!(
                "event_log batch INSERT returned {} rows for {} events",
                all_returned.len(),
                events.len()
            )));
        }

        // Re-order returned (position, event_id) to match the input event
        // order. SQLite's RETURNING clause currently returns rows in the
        // order rows were inserted (same as PG), but the SQL spec does not
        // mandate it. Pay O(N) hashmap once rather than ever debug a
        // flaky projection order. Input uniqueness is verified above, so
        // every input event_id maps to exactly one entry here.
        let mut by_event_id: std::collections::HashMap<&str, i64> =
            std::collections::HashMap::with_capacity(all_returned.len());
        for (pos, eid) in &all_returned {
            by_event_id.insert(eid.as_str(), *pos);
        }

        let mut positions = Vec::with_capacity(events.len());
        for event in events {
            let pos = by_event_id.get(event.event_id.as_str()).ok_or_else(|| {
                StoreError::Internal(format!(
                    "event_log batch INSERT did not return event_id {}",
                    event.event_id.as_str()
                ))
            })?;
            positions.push(EventPosition(*pos as u64));
        }

        // Apply synchronous projections within the same transaction so
        // current-state tables stay consistent with the event log —
        // reads can never observe a position that has not been
        // projected. `apply_async` takes `&EventEnvelope` so no clone
        // of the potentially large payload is needed on the hot path.
        //
        // (Pre-T2-C1 this call was missing and every SQLite-backed
        // projection table stayed empty in production — retained as a
        // regression-origin pointer to .claude/audit-state/review-queue.md
        // §T2-C1 per project convention.)
        for event in events {
            SqliteSyncProjection::apply_async(&mut tx, event).await?;
        }

        tx.commit()
            .await
            .map_err(|e| StoreError::Connection(e.to_string()))?;

        Ok(positions)
    }

    async fn read_by_entity(
        &self,
        entity: &EntityRef,
        after: Option<EventPosition>,
        limit: usize,
    ) -> Result<Vec<StoredEvent>, StoreError> {
        let after_pos = after.map(|p| p.0 as i64).unwrap_or(0);
        let (id_field, id_value) = entity_ref_filter(entity);

        let sql = format!(
            "SELECT position, event_id, source_meta, ownership, causation_id, correlation_id, payload, stored_at
             FROM event_log
             WHERE position > $1
               AND json_extract(payload, '$.{id_field}') = $2
             ORDER BY position ASC
             LIMIT $3"
        );

        let rows = sqlx::query_as::<_, EventRow>(&sql)
            .bind(after_pos)
            .bind(&id_value)
            .bind(limit as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;

        rows.into_iter().map(|r| r.into_stored_event()).collect()
    }

    async fn read_stream(
        &self,
        after: Option<EventPosition>,
        limit: usize,
    ) -> Result<Vec<StoredEvent>, StoreError> {
        let after_pos = after.map(|p| p.0 as i64).unwrap_or(0);

        let rows = sqlx::query_as::<_, EventRow>(
            "SELECT position, event_id, source_meta, ownership, causation_id, correlation_id, payload, stored_at
             FROM event_log
             WHERE position > $1
             ORDER BY position ASC
             LIMIT $2",
        )
        .bind(after_pos)
        .bind(limit as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StoreError::Internal(e.to_string()))?;

        rows.into_iter().map(|r| r.into_stored_event()).collect()
    }

    async fn head_position(&self) -> Result<Option<EventPosition>, StoreError> {
        // `MAX(position)` on empty table yields NULL (decoded as
        // `Some((None,))` by sqlx-SQLite). Decode into `Option<i64>` and
        // filter on the inner option; decoding into plain `i64` would
        // error on NULL.
        let row: Option<(Option<i64>,)> = sqlx::query_as("SELECT MAX(position) FROM event_log")
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;

        Ok(row.and_then(|(pos,)| pos.map(|p| EventPosition(p as u64))))
    }

    async fn find_by_causation_id(
        &self,
        causation_id: &str,
    ) -> Result<Option<EventPosition>, StoreError> {
        // `MIN(position)` on an empty match always returns one row with a
        // NULL value — `fetch_optional` reports `Some(...)` for the row and
        // sqlx decodes the NULL into `Option<i64>`. Filter on the inner
        // `Option` rather than checking for a sentinel. Pre-T2-M7 a
        // `pos > 0` guard tried to approximate this but discarded the
        // legitimate position-0 edge, diverging from the PG backend.
        let row: Option<(Option<i64>,)> =
            sqlx::query_as("SELECT MIN(position) FROM event_log WHERE causation_id = ?")
                .bind(causation_id)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| StoreError::Internal(e.to_string()))?;

        Ok(row.and_then(|(pos,)| pos.map(|p| EventPosition(p as u64))))
    }
}

#[derive(sqlx::FromRow)]
struct EventRow {
    position: i64,
    event_id: String,
    source_meta: String,
    ownership: String,
    causation_id: Option<String>,
    correlation_id: Option<String>,
    payload: String,
    stored_at: i64,
}

impl EventRow {
    fn into_stored_event(self) -> Result<StoredEvent, StoreError> {
        let source = serde_json::from_str(&self.source_meta)
            .map_err(|e| StoreError::Serialization(e.to_string()))?;
        let ownership: cairn_domain::OwnershipKey = serde_json::from_str(&self.ownership)
            .map_err(|e| StoreError::Serialization(e.to_string()))?;
        let payload: RuntimeEvent = serde_json::from_str(&self.payload)
            .map_err(|e| StoreError::Serialization(e.to_string()))?;

        // Row rehydration stays explicit here: source/ownership/ids are
        // persisted separately in the event log and need to be reconstructed
        // from stored columns rather than re-derived only from the payload.
        let mut envelope = EventEnvelope::new(
            cairn_domain::EventId::new(self.event_id),
            source,
            ownership,
            payload,
        );

        if let Some(causation_id) = self.causation_id {
            envelope = envelope.with_causation_id(cairn_domain::CommandId::new(causation_id));
        }

        if let Some(correlation_id) = self.correlation_id {
            envelope = envelope.with_correlation_id(correlation_id);
        }

        Ok(StoredEvent {
            position: EventPosition(self.position as u64),
            envelope,
            stored_at: self.stored_at as u64,
        })
    }
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

fn entity_ref_filter(entity: &EntityRef) -> (&'static str, String) {
    match entity {
        EntityRef::Session(id) => ("session_id", id.to_string()),
        EntityRef::Run(id) => ("run_id", id.to_string()),
        EntityRef::Task(id) => ("task_id", id.to_string()),
        EntityRef::Approval(id) => ("approval_id", id.to_string()),
        EntityRef::Checkpoint(id) => ("checkpoint_id", id.to_string()),
        EntityRef::Mailbox(id) => ("message_id", id.to_string()),
        EntityRef::ToolInvocation(id) => ("invocation_id", id.to_string()),
        EntityRef::Signal(id) => ("signal_id", id.to_string()),
        EntityRef::IngestJob(id) => ("job_id", id.to_string()),
        EntityRef::EvalRun(id) => ("eval_run_id", id.to_string()),
        EntityRef::PromptAsset(id) => ("prompt_asset_id", id.to_string()),
        EntityRef::PromptVersion(id) => ("prompt_version_id", id.to_string()),
        EntityRef::PromptRelease(id) => ("prompt_release_id", id.to_string()),
    }
}
