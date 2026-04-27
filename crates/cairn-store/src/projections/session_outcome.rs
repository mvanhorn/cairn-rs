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
/// Falls back to the discriminator with an empty/zeroed payload when the
/// JSON column is NULL (legacy/hand-inserted rows) or cannot be parsed;
/// on parse failure the kind still comes from the canonical discriminator
/// so operator filters keep working.
pub fn rehydrate_termination_reason(
    kind: &str,
    json: Option<&str>,
) -> Result<cairn_domain::TerminationReason, StoreError> {
    // Prefer the JSON payload when present and the parse succeeds. The
    // discriminator is authoritative for the kind; we only accept the
    // JSON if its serde tag matches. This guards against the (unlikely)
    // case where the two columns are inconsistent.
    if let Some(raw) = json.filter(|s| !s.is_empty()) {
        match serde_json::from_str::<cairn_domain::TerminationReason>(raw) {
            Ok(full) => {
                if termination_reason_kind(&full) == kind {
                    return Ok(full);
                }
                // Kind mismatch — fall through to discriminator-only.
                // This should never happen in practice because the
                // writer serializes the same value as it derives the
                // kind from; silently preferring the discriminator is
                // the safer choice for operator tooling.
            }
            Err(_) => {
                // Parse failure — fall through to discriminator.
            }
        }
    }
    match kind {
        "complete_run" => Ok(cairn_domain::TerminationReason::CompleteRun),
        "lease_lost" => Ok(cairn_domain::TerminationReason::LeaseLost),
        "operator_cancel" => Ok(cairn_domain::TerminationReason::OperatorCancel),
        "provider_error" => Ok(cairn_domain::TerminationReason::ProviderError {
            message: String::new(),
        }),
        "crashed" => Ok(cairn_domain::TerminationReason::Crashed {
            message: String::new(),
        }),
        "circuit_breaker_tripped" => Ok(cairn_domain::TerminationReason::CircuitBreakerTripped {
            trip: cairn_domain::CircuitBreakerTrip {
                which: cairn_domain::BreakerKind::Round,
                measured: 0,
                limit: 0,
                at_iteration: 0,
            },
        }),
        other => Err(StoreError::Serialization(format!(
            "unknown session_outcomes.termination_reason '{other}'"
        ))),
    }
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
