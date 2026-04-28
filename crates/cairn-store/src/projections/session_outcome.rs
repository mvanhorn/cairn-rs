//! F65 PR-2: projections for the orchestrator-session redesign.
//!
//! This module owns the read-model shapes and traits for three new
//! projection tables plus one extension of the existing `checkpoints`
//! table introduced by F65 PR-2:
//!
//! - `workspace_registry`  — live workspace id → host path mapping (new table)
//! - `workspace_snapshots` — immutable reflinked workspace trees (new table)
//! - `session_outcomes`    — rich terminal outcome per session (new table)
//! - `checkpoints`         — extended with F65 columns (session_id,
//!   schema_version, body, body_size_bytes, iteration); the RFC 005 shape
//!   lives on alongside the F65 columns. No separate `f65_checkpoints`
//!   table — the identity namespace is shared and the columns are
//!   nullable, so legacy rows continue to read cleanly via
//!   [`crate::projections::checkpoint::CheckpointReadModel`].
//!
//! The domain event shapes live in [`cairn_domain::events`]; the long-lived
//! domain records live in [`cairn_domain::session_orchestration`]. These
//! projection records are storage-layer views with the additional bookkeeping
//! columns (version, created_at/updated_at) that every cairn-store table
//! carries.
//!
//! PR-2 is projection-only: no write HTTP endpoints land here; the real
//! orchestrator surfaces come in PR-3..PR-7. Integration tests in
//! `crates/cairn-store/tests/f65_*` drive the event path end-to-end.
//!
//! Portability: all persisted columns use the pg/sqlite common subset — `TEXT`
//! for strings, `BIGINT`/`INTEGER` for counters, `DOUBLE PRECISION`/`REAL`
//! for the cost totals. JSON payloads (checkpoint body, compacted_summary)
//! are stored as TEXT — no JSONB, no arrays, no dialect-specific features.

use async_trait::async_trait;
use cairn_domain::{
    CheckpointId, ProjectKey, RunId, SessionId, TerminationReason, WorkspaceId, WorkspaceSnapshotId,
};
use serde::{Deserialize, Serialize};

use crate::error::StoreError;

/// Lifecycle state of a `workspace_registry` row.
///
/// Matches the overlayfs lifecycle described in the arch doc §4.3:
/// - `Active` — the overlay is mounted and the workspace is in use.
/// - `Snapshotted` — the workspace has been umounted and reflinked into a
///   durable `workspace_snapshots` row.
/// - `Reaped` — both the overlay and any dependent snapshot lineage have
///   been garbage-collected.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkspaceRegistryStatus {
    Active,
    Snapshotted,
    Reaped,
}

impl WorkspaceRegistryStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Snapshotted => "snapshotted",
            Self::Reaped => "reaped",
        }
    }
}

impl std::str::FromStr for WorkspaceRegistryStatus {
    type Err = StoreError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "active" => Ok(Self::Active),
            "snapshotted" => Ok(Self::Snapshotted),
            "reaped" => Ok(Self::Reaped),
            other => Err(StoreError::Serialization(format!(
                "unknown workspace_registry.status '{other}'"
            ))),
        }
    }
}

/// A row in the `workspace_registry` projection.
///
/// Identifies a live overlayfs mount on the host. Created when a root-Run
/// provisions its sandbox; transitions to `Snapshotted` when the umount +
/// reflink path runs at termination; transitions to `Reaped` when the GC
/// sweep removes the snapshot lineage.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceRegistryRecord {
    pub workspace_id: WorkspaceId,
    pub project: ProjectKey,
    pub root_run_id: RunId,
    /// Resolved host path. Not exposed to LLMs — the WorkspaceResolver
    /// (PR-7) is the only consumer with access.
    pub fs_root: String,
    pub status: WorkspaceRegistryStatus,
    pub created_at: u64,
    pub reaped_at: Option<u64>,
}

/// A row in the `workspace_snapshots` projection.
///
/// Immutable. One per `WorkspaceSnapshotCreated` event; `reaped_at` is set
/// (update-in-place) when the GC sweep clears the snapshot. `parent_snapshot_id`
/// encodes the reflink lineage so the reaper can walk the chain.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceSnapshotRecord {
    pub snapshot_id: WorkspaceSnapshotId,
    pub project: ProjectKey,
    pub session_id: SessionId,
    pub workspace_id: WorkspaceId,
    pub parent_snapshot_id: Option<WorkspaceSnapshotId>,
    /// Relative-to-configured-root path (e.g. `snapshots/<uuid>/`). The
    /// workspace backend resolves the absolute path at access time.
    pub snapshot_path: String,
    pub bytes: u64,
    pub reflink_used: bool,
    pub created_at: u64,
    pub reaped_at: Option<u64>,
}

/// A row in the `session_outcomes` projection.
///
/// One per session terminal outcome (primary key = `root_run_id`).
/// `workspace_snapshot_id` is **nullable** by design — legacy runs that
/// predate the sandbox never produced a snapshot; the arch doc's back-compat
/// section (§6.3) requires the projection to tolerate this.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionOutcomeRecord {
    pub root_run_id: RunId,
    pub project: ProjectKey,
    pub session_id: SessionId,
    pub checkpoint_id: CheckpointId,
    pub workspace_snapshot_id: Option<WorkspaceSnapshotId>,
    pub termination_reason: TerminationReason,
    /// JSON-encoded compacted summary (populated by PR-6). Stored as TEXT
    /// so pg and sqlite share the same column type.
    pub compacted_summary: String,
    pub next_step_hint: Option<String>,
    /// Cost in USD micros (1 USD = 1_000_000). Integer storage matches
    /// `SessionCostUpdated.delta_cost_micros` and keeps the outcome
    /// equatable under serde replay.
    pub cost_micros: u64,
    pub created_at: u64,
}

/// F65 checkpoint row.
///
/// Distinct from the RFC 005 `CheckpointRecord` on purpose:
/// - RFC 005 checkpoints are indexed by `(run_id, disposition)` and model
///   per-run recovery state. The body lives in a separate JSON column added
///   by V020.
/// - F65 checkpoints are the orchestrator's LLM-resumable blob, keyed by
///   `(session_id, root_run_id, iteration)`. Body is canonical-serialized
///   JSON stored as TEXT — size budget 50KB–2MB per blob.
///
/// We implement this by ALTERing the existing `checkpoints` table to add the
/// F65 columns (nullable — legacy rows never fill them), rather than shipping
/// a second `checkpoints` table. The existing `CheckpointRecord` projection
/// continues to work unchanged; F65 readers use the new shape below.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct F65CheckpointRecord {
    pub checkpoint_id: CheckpointId,
    pub project: ProjectKey,
    pub session_id: SessionId,
    pub root_run_id: RunId,
    pub schema_version: u32,
    /// Canonical-JSON-serialized checkpoint body. Opaque to the projection
    /// layer; PR-6 summarizer consumes the fields.
    pub body: String,
    pub body_size_bytes: u64,
    pub iteration: u32,
    pub created_at: u64,
}

// ── Read models ──────────────────────────────────────────────────────────────

/// Reader for the `session_outcomes` projection.
///
/// One outcome per terminal attempt. Tests + UI + orchestrator all use this
/// to enumerate a session's outcomes without walking the event log.
#[async_trait]
pub trait SessionOutcomeReadModel: Send + Sync {
    /// Fetch the outcome for a specific root-Run (primary key).
    async fn get_by_root_run(
        &self,
        root_run_id: &RunId,
    ) -> Result<Option<SessionOutcomeRecord>, StoreError>;

    /// List all outcomes for a session in chronological order (oldest first).
    async fn list_by_session(
        &self,
        session_id: &SessionId,
    ) -> Result<Vec<SessionOutcomeRecord>, StoreError>;
}

/// F65 PR-5: back-fill the `bytes`, `reflink_used`, `snapshot_path`, and
/// `parent_snapshot_id` columns on an existing `workspace_snapshots` row
/// after the overlay-to-reflink copy runs.
///
/// PR-2 shipped the insert path that zero-fills those columns (the domain
/// event [`cairn_domain::WorkspaceSnapshotCreated`] intentionally carries
/// only identity, so it's portable across log replay). PR-5 stamps the
/// filesystem-observed metadata via this writer — called by
/// [`cairn_workspace::SandboxService::terminate_for_session`] **before** it
/// emits `WorkspaceSnapshotCreated` so readers who pick up the event see
/// the fully-populated row.
///
/// Idempotent: the UPDATE overwrites the columns unconditionally, so a
/// replay or a stamper retry leaves the row consistent. `reap_for_session`
/// marks every still-live snapshot row for a session as reaped — the
/// admin endpoint in cairn-app calls this after walking the read model
/// and emitting `WorkspaceSnapshotReaped` per row.
#[async_trait]
pub trait WorkspaceSnapshotWriter: Send + Sync {
    /// Stamp filesystem metadata onto an existing `workspace_snapshots`
    /// row. No-op if the row doesn't exist (the caller is responsible
    /// for emitting `WorkspaceSnapshotCreated` first).
    async fn stamp_metadata(
        &self,
        snapshot_id: &WorkspaceSnapshotId,
        snapshot_path: &str,
        bytes: u64,
        reflink_used: bool,
        parent_snapshot_id: Option<&WorkspaceSnapshotId>,
    ) -> Result<(), StoreError>;
}

/// Reader for the `workspace_snapshots` projection.
#[async_trait]
pub trait WorkspaceSnapshotReadModel: Send + Sync {
    async fn get(
        &self,
        snapshot_id: &WorkspaceSnapshotId,
    ) -> Result<Option<WorkspaceSnapshotRecord>, StoreError>;

    /// List all snapshots for a session in chronological order (oldest first).
    async fn list_by_session(
        &self,
        session_id: &SessionId,
    ) -> Result<Vec<WorkspaceSnapshotRecord>, StoreError>;

    /// Walk the parent-snapshot chain for a snapshot, newest first, stopping
    /// at the root. The returned vector starts with `start` itself and ends
    /// at the lineage root. Returns an empty vector if `start` doesn't exist.
    async fn lineage(
        &self,
        start: &WorkspaceSnapshotId,
    ) -> Result<Vec<WorkspaceSnapshotRecord>, StoreError>;
}

/// Reader for the `workspace_registry` projection.
#[async_trait]
pub trait WorkspaceRegistryReadModel: Send + Sync {
    async fn get(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<Option<WorkspaceRegistryRecord>, StoreError>;

    /// Look up the live workspace row belonging to a root-Run, if any.
    async fn get_by_root_run(
        &self,
        root_run_id: &RunId,
    ) -> Result<Option<WorkspaceRegistryRecord>, StoreError>;
}

/// F65 PR-2: short-form termination discriminator for
/// `session_outcomes.termination_reason`.
///
/// Matches the serde `#[serde(rename_all = "snake_case")]` discriminator on
/// [`cairn_domain::TerminationReason`] so the event log and the projection
/// use the same string vocabulary — operator filters don't need a second
/// mapping. The payload variants (`ProviderError`, `CircuitBreakerTripped`,
/// `Crashed`) are represented only by their kind tag here; the full
/// payload lives on `session_outcomes.termination_reason_json` (a TEXT
/// JSON sidecar) so readers can rehydrate the complete
/// [`cairn_domain::TerminationReason`] when needed.
pub fn termination_reason_kind(r: &cairn_domain::TerminationReason) -> &'static str {
    match r {
        cairn_domain::TerminationReason::CompleteRun => "complete_run",
        cairn_domain::TerminationReason::CircuitBreakerTripped { .. } => "circuit_breaker_tripped",
        cairn_domain::TerminationReason::LeaseLost => "lease_lost",
        cairn_domain::TerminationReason::ProviderError { .. } => "provider_error",
        cairn_domain::TerminationReason::OperatorCancel => "operator_cancel",
        cairn_domain::TerminationReason::Crashed { .. } => "crashed",
    }
}

/// F65 PR-2: rehydrate a full [`cairn_domain::TerminationReason`] from the
/// two-column storage shape (`termination_reason` discriminator +
/// `termination_reason_json` payload).
///
/// The writer (`pg/projections.rs` + `sqlite/projections.rs`) always
/// populates both columns. Readers prefer the JSON column so payload
/// variants (`ProviderError.message`, `CircuitBreakerTripped.trip`,
/// `Crashed.message`) carry their real fields instead of empty/zeroed
/// placeholders that would mislead callers.
///
/// # Failure modes (issue #465, no silent fallbacks)
///
/// Payload-bearing variants require the JSON column — their meaning is
/// not captured by the discriminator alone. If the JSON is NULL or
/// malformed we return `Err(StoreError::Serialization)` with an
/// actionable message instead of fabricating a zero-valued record. A
/// silent fabrication (e.g. `CircuitBreakerTripped` with `measured=0,
/// limit=0`) is indistinguishable from a real trip whose counters
/// happened to be zero, which undermines every operator dashboard that
/// filters by breaker kind. Per `feedback_no_silent_fallbacks.md`:
/// fail clearly, never silently substitute.
///
/// Variants mapped:
/// - `complete_run`, `lease_lost`, `operator_cancel` — discriminator
///   fully describes the reason. NULL / missing / malformed JSON is
///   fine; the returned value carries no payload.
/// - `provider_error`, `crashed` — `message: String` payload. JSON is
///   required; NULL or malformed returns `Err`.
/// - `circuit_breaker_tripped` — `trip: CircuitBreakerTrip` payload
///   (which kind, measured, limit, at_iteration). JSON is required;
///   NULL or malformed returns `Err`.
///
/// One cross-kind corruption mode always errs regardless of payload
/// presence: JSON parses cleanly but its serde tag does not match the
/// discriminator column. The two columns disagreeing is real
/// corruption that no reader can resolve; surface it so the writer
/// can be investigated.
///
/// The error message names the column, root_run_id-equivalent context
/// the caller must supply, and suggests a remediation (backfill via
/// writer replay or manual SQL UPDATE) so operators have a runbook.
pub fn rehydrate_termination_reason(
    kind: &str,
    json: Option<&str>,
) -> Result<cairn_domain::TerminationReason, StoreError> {
    // Step 1: try to honour the JSON column. A present-and-consistent
    // JSON value is the authoritative shape for the variant payload.
    // Mismatches between the two columns are always data corruption
    // (writer bug or manual tampering) and fail closed regardless of
    // kind — no reader can meaningfully resolve them.
    match parse_json_column(kind, json)? {
        JsonColumnOutcome::Full(full) => return Ok(full),
        JsonColumnOutcome::PayloadlessFallthrough => {
            // Safe to fall through — payload-less variants carry no
            // data beyond the discriminator, so NULL/empty/malformed
            // JSON is not information loss. See rehydrate_from_kind
            // below.
        }
    }

    // Step 2: discriminator-only path. Payload-less variants succeed;
    // payload-bearing variants fail because the JSON column is the
    // only place the payload lives (step 1 already confirmed the JSON
    // is unusable). Per feedback_no_silent_fallbacks.md we must not
    // fabricate a zero-valued record here.
    rehydrate_from_kind(kind)
}

/// Outcome of consulting the `termination_reason_json` column.
enum JsonColumnOutcome {
    /// JSON parsed cleanly and its serde tag matches `kind` — return it.
    Full(cairn_domain::TerminationReason),
    /// JSON is NULL/empty, OR malformed for a payload-less variant
    /// (where the kind alone fully describes the reason). Caller should
    /// fall through to the discriminator path.
    PayloadlessFallthrough,
}

/// Returns `Ok(JsonColumnOutcome::Full)` when the JSON is usable,
/// `Ok(JsonColumnOutcome::PayloadlessFallthrough)` when the JSON is
/// absent/unusable but the `kind` is payload-less (discriminator is
/// enough), or `Err(StoreError::Serialization)` for all other
/// corruption cases (inconsistent columns, malformed JSON for a
/// payload-bearing kind, or malformed JSON whose remediation depends
/// on the actual payload).
fn parse_json_column(kind: &str, json: Option<&str>) -> Result<JsonColumnOutcome, StoreError> {
    let Some(raw) = json.filter(|s| !s.is_empty()) else {
        // NULL or empty json. Caller handles the discriminator path —
        // payload-less kinds succeed there, payload-bearing ones Err
        // with the dedicated NULL message.
        return Ok(JsonColumnOutcome::PayloadlessFallthrough);
    };

    match serde_json::from_str::<cairn_domain::TerminationReason>(raw) {
        Ok(full) if termination_reason_kind(&full) == kind => Ok(JsonColumnOutcome::Full(full)),
        Ok(full) => {
            // Kind mismatch — the two columns disagree. This is data
            // corruption, not a legacy row, and applies to every kind
            // (payload-less or payload-bearing) because no reader can
            // decide which column to believe. Fail clearly so
            // operators can investigate the writer.
            Err(StoreError::Serialization(format!(
                "session_outcomes row has inconsistent termination_reason: \
                 discriminator column = '{kind}', but \
                 termination_reason_json deserialises to '{}' — writer bug \
                 or manual tampering. Inspect the affected row and re-emit \
                 SessionOutcomeEmitted to overwrite both columns.",
                termination_reason_kind(&full)
            )))
        }
        Err(err) if is_payloadless_kind(kind) => {
            // Malformed JSON for a payload-less variant is still not
            // information loss: the discriminator column alone carries
            // the full meaning. Accept the corruption on this column,
            // fall through to the discriminator. Matches the function
            // doc: "NULL / missing / malformed JSON is fine; the
            // returned value carries no payload."
            //
            // We deliberately swallow the parse error here — if we
            // returned it the operator would be forced to triage a
            // non-actionable corruption on a column whose value is
            // redundant. Future: if this becomes a signal operators
            // want, expose it via telemetry rather than the Result.
            let _ = err;
            Ok(JsonColumnOutcome::PayloadlessFallthrough)
        }
        Err(err) => {
            // Malformed JSON for a payload-bearing variant. The
            // payload is gone; the kind alone (e.g. "provider_error")
            // is not enough to triage. Fail closed with the parse
            // error so operators can debug the writer.
            Err(StoreError::Serialization(format!(
                "session_outcomes.termination_reason_json for kind='{kind}' \
                 could not be parsed: {err}. The payload for this variant \
                 is load-bearing for operator triage. Row is corrupt — \
                 re-emit SessionOutcomeEmitted to overwrite the column, \
                 or UPDATE session_outcomes SET termination_reason_json = … \
                 with the correct payload."
            )))
        }
    }
}

/// Map a discriminator string to its `TerminationReason` when no JSON
/// payload is available. Payload-less kinds succeed because their
/// meaning is fully captured by the discriminator; payload-bearing
/// kinds return `Err` with a per-kind remediation hint rather than
/// fabricating a zero-valued record.
fn rehydrate_from_kind(kind: &str) -> Result<cairn_domain::TerminationReason, StoreError> {
    match kind {
        // Payload-less variants: the discriminator fully describes the
        // outcome. NULL / missing / malformed json is expected for
        // pre-column legacy rows and for these variants carries no
        // information loss.
        "complete_run" => Ok(cairn_domain::TerminationReason::CompleteRun),
        "lease_lost" => Ok(cairn_domain::TerminationReason::LeaseLost),
        "operator_cancel" => Ok(cairn_domain::TerminationReason::OperatorCancel),

        // Payload-bearing variants: fail closed. Returning a zero-valued
        // record here would silently mislead operators ("breaker tripped
        // with measured=0/limit=0" is nonsensical; "provider_error with
        // empty message" is untriageable). Per the no-silent-fallbacks
        // rule we surface the data-corruption explicitly.
        "provider_error" => Err(StoreError::Serialization(
            "session_outcomes row has kind='provider_error' but \
             termination_reason_json is NULL — writer always populates \
             this column on new writes (pg/projections.rs + \
             sqlite/projections.rs), so NULL means either a pre-column \
             legacy row or a writer regression. The ProviderError.message \
             is load-bearing for operator triage; returning an empty \
             placeholder would silently mislead the dashboard. Fix: \
             re-emit SessionOutcomeEmitted for the affected root_run_id, \
             or UPDATE session_outcomes SET termination_reason_json = … \
             with the correct payload."
                .to_owned(),
        )),
        "crashed" => Err(StoreError::Serialization(
            "session_outcomes row has kind='crashed' but \
             termination_reason_json is NULL — writer always populates \
             this column on new writes, so NULL means either a pre-column \
             legacy row or a writer regression. The Crashed.message \
             carries the crash details and is load-bearing for operator \
             triage. Fix: re-emit SessionOutcomeEmitted for the affected \
             root_run_id, or UPDATE session_outcomes SET \
             termination_reason_json = … with the correct payload."
                .to_owned(),
        )),
        "circuit_breaker_tripped" => Err(StoreError::Serialization(
            "session_outcomes row has kind='circuit_breaker_tripped' but \
             termination_reason_json is NULL — writer always populates \
             this column on new writes, so NULL means either a pre-column \
             legacy row or a writer regression. The CircuitBreakerTrip \
             payload (which kind: Round/Tokens/NoToolUseConsecutive/\
             WallClock; measured; limit; at_iteration) is load-bearing \
             for operator triage; fabricating a zero-valued record is \
             indistinguishable from a real trip whose counters happened \
             to be zero. Fix: re-emit SessionOutcomeEmitted for the \
             affected root_run_id, or UPDATE session_outcomes SET \
             termination_reason_json = … with the correct payload."
                .to_owned(),
        )),

        other => Err(StoreError::Serialization(format!(
            "unknown session_outcomes.termination_reason '{other}'"
        ))),
    }
}

/// True when `kind` maps to a `TerminationReason` variant that has no
/// payload beyond the discriminator itself. These variants can round-
/// trip cleanly from the discriminator column alone.
fn is_payloadless_kind(kind: &str) -> bool {
    matches!(kind, "complete_run" | "lease_lost" | "operator_cancel")
}

/// Reader for F65-style `checkpoints` rows (the extended columns on the
/// existing `checkpoints` table).
///
/// Rows written before PR-2 (legacy RFC 005 checkpoints) have NULL F65
/// columns and return `None` from `get_f65` — callers use the RFC 005
/// `CheckpointReadModel` for those.
#[async_trait]
pub trait F65CheckpointReadModel: Send + Sync {
    async fn get_f65(
        &self,
        checkpoint_id: &CheckpointId,
    ) -> Result<Option<F65CheckpointRecord>, StoreError>;

    /// List checkpoints for a session in iteration order (ascending).
    async fn list_by_session(
        &self,
        session_id: &SessionId,
    ) -> Result<Vec<F65CheckpointRecord>, StoreError>;
}
