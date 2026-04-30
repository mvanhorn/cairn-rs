use async_trait::async_trait;
use sqlx::{PgPool, QueryBuilder};
use std::time::{SystemTime, UNIX_EPOCH};

use cairn_domain::{EventEnvelope, RuntimeEvent};

use super::projections::PgSyncProjection;
use crate::error::StoreError;
use crate::event_log::{EntityRef, EventLog, EventPosition, StoredEvent};

/// Maximum events per multi-row `INSERT`. Keeps the total host-parameter
/// count conservatively below backend limits:
///
/// - Postgres caps query parameters at 65535 (i16); 100 events × 8 columns =
///   800, giving ~80× headroom.
/// - SQLite defaults to 32766 on modern builds (≥ 3.32) but historically
///   999. 100 × 8 = 800 stays under the legacy 999 limit too so the same
///   chunk size works identically on both backends — important because
///   the SQL shape has to stay portable per the "no DB-specific
///   features" rule.
///
/// For bursts of ≤ 100 events (almost all production traffic — a
/// checkpoint flush, a tool-result fanout, a recovery replay batch)
/// this is still a single round trip. For mega-bursts the insert
/// stays in one transaction; we just loop the INSERT statement inside
/// the existing tx.
const BATCH_INSERT_CHUNK: usize = 100;

/// Postgres-backed append-only event log.
///
/// Appends events to the `event_log` table and updates synchronous
/// projections within the same transaction.
pub struct PgEventLog {
    pool: PgPool,
}

impl PgEventLog {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl EventLog for PgEventLog {
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

        // Pre-serialize JSON columns so the transaction-holding code path
        // touches zero allocation hot spots after we start the tx. Any
        // serialization failure is a caller bug (event payloads are always
        // serializable) but we still surface it cleanly.
        struct Row<'a> {
            event_id: &'a str,
            source_type: &'static str,
            source_meta: serde_json::Value,
            ownership: serde_json::Value,
            causation_id: Option<&'a str>,
            correlation_id: Option<&'a str>,
            payload: serde_json::Value,
        }

        // Reject duplicate event_ids *before* touching the database so the
        // caller gets a clear domain error instead of a cryptic
        // `duplicate key value violates unique constraint` from Postgres.
        // The schema's `event_id TEXT NOT NULL UNIQUE` (V001) is the
        // ultimate guard — this check is a defense in depth that also
        // prevents the client-side reorder HashMap (below) from aliasing
        // two input slots to the same position when an upstream bug
        // somehow produces a duplicate. Gemini flagged the HashMap's
        // unique-key assumption on the first review pass (#539); this
        // is the explicit, auditable version of that invariant.
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
                source_meta: serde_json::to_value(&event.source)
                    .map_err(|e| StoreError::Serialization(e.to_string()))?,
                ownership: serde_json::to_value(&event.ownership)
                    .map_err(|e| StoreError::Serialization(e.to_string()))?,
                causation_id: event.causation_id.as_ref().map(|id| id.as_str()),
                correlation_id: event.correlation_id.as_deref(),
                payload: serde_json::to_value(&event.payload)
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
        // Invariants enforced by this block:
        //
        // 1. **One transaction per call.** All chunks commit together;
        //    partial batches must never be observable.
        // 2. **Portable SQL shape.** `INSERT ... VALUES (…),(…),… RETURNING
        //    position, event_id` works identically on Postgres and SQLite;
        //    no JSONB / array / backend-specific DML.
        // 3. **Client-side reorder via `event_id`.** The SQL spec does
        //    not guarantee RETURNING row order, so we match each input
        //    event's event_id against the returned set. Both pg and
        //    sqlite currently preserve insertion order but tests must
        //    not rely on that.
        // 4. **Chunk size ≤ `BATCH_INSERT_CHUNK`.** Keeps the host-
        //    parameter count below every supported backend's cap
        //    (Postgres 65535, legacy SQLite 999). All chunks share the
        //    enclosing transaction; projection atomicity is unchanged.
        let mut all_returned: Vec<(i64, String)> = Vec::with_capacity(events.len());

        for chunk in rows.chunks(BATCH_INSERT_CHUNK) {
            let mut builder: QueryBuilder<'_, sqlx::Postgres> = QueryBuilder::new(
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
        // order. Input uniqueness has been verified above, so every input
        // event_id has exactly one entry in the returned set.
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

        // Apply synchronous projections within the same transaction.
        // This guarantees current-state tables (sessions, runs, tasks,
        // …) are always consistent with the event log — reads can never
        // observe a position that has not been projected.
        //
        // Iteration is over the borrowed input slice; `apply_async`
        // takes `&EventEnvelope` so no clone of the potentially large
        // payload (e.g. CheckpointCreated snapshots) is needed on the
        // hot append path.
        //
        // `event_time_ms` is the same `now` we bound to the `stored_at`
        // column above — projection arms that key row data off event
        // time (e.g. `pause_schedules.resume_at_ms`) must agree with
        // the event-log column to be rebuild-safe. Copilot #595.
        let event_time_ms = u64::try_from(now).unwrap_or(0);
        for event in events {
            PgSyncProjection::apply_async(&mut tx, event, event_time_ms).await?;
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

        // Filter events by payload JSON field matching the entity ID.
        let sql = format!(
            "SELECT position, event_id, source_type, source_meta, ownership, causation_id, correlation_id, payload, stored_at
             FROM event_log
             WHERE position > $1
               AND payload->>'{id_field}' = $2
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
            "SELECT position, event_id, source_type, source_meta, ownership, causation_id, correlation_id, payload, stored_at
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
        let row: Option<(i64,)> = sqlx::query_as("SELECT MAX(position) FROM event_log")
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;

        Ok(row.and_then(|(pos,)| {
            if pos > 0 {
                Some(EventPosition(pos as u64))
            } else {
                None
            }
        }))
    }

    async fn find_by_causation_id(
        &self,
        causation_id: &str,
    ) -> Result<Option<EventPosition>, StoreError> {
        let row: Option<(i64,)> =
            sqlx::query_as("SELECT position FROM event_log WHERE causation_id = $1 LIMIT 1")
                .bind(causation_id)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(row.map(|(pos,)| EventPosition(pos as u64)))
    }
}

/// Raw row from the event_log table.
///
/// The shape must stay byte-for-byte aligned with the SELECT lists in
/// `read_entity_stream` / `read_stream` because sqlx::FromRow matches
/// by column order. `source_type` is denormalised from `source_meta`
/// for index-backed filtering in future debug queries — it is not
/// consumed by `into_stored_event` yet, hence the single-field
/// `#[allow(dead_code)]` (#480). `event_id` was flagged as dead by an
/// earlier sweep, but it IS consumed below (line 327) — the annotation
/// is stale and removed.
#[derive(sqlx::FromRow)]
struct EventRow {
    position: i64,
    event_id: String,
    /// Denormalised from `source_meta` for future index-backed debug
    /// queries (e.g. "all events produced by the Runtime source").
    /// Present in the SELECT list so adding a future reader doesn't
    /// have to re-audit every query — but not read by the current
    /// rehydrator, which reconstructs the full `EventSource` from
    /// `source_meta`.
    #[allow(dead_code)]
    source_type: String,
    source_meta: serde_json::Value,
    ownership: serde_json::Value,
    causation_id: Option<String>,
    correlation_id: Option<String>,
    payload: serde_json::Value,
    stored_at: i64,
}

impl EventRow {
    fn into_stored_event(self) -> Result<StoredEvent, StoreError> {
        let source = serde_json::from_value(self.source_meta)
            .map_err(|e| StoreError::Serialization(e.to_string()))?;
        let ownership: cairn_domain::OwnershipKey = serde_json::from_value(self.ownership)
            .map_err(|e| StoreError::Serialization(e.to_string()))?;
        let payload: RuntimeEvent = serde_json::from_value(self.payload)
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
