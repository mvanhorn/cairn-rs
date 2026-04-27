//! F65: orchestrator session redesign — domain types (PR-1 foundation).
//!
//! These types describe the durable state and outcomes of an **orchestrated
//! session**: the long-lived envelope around one or more orchestrator runs
//! that iterate on a goal, emit checkpoints, and terminate with a rich
//! [`SessionOutcome`] for downstream consumers (summarizer, operator UI,
//! next-session seeding).
//!
//! PR-1 is strictly additive: it introduces the shapes only. No projection
//! writes against them yet (PR-2), no circuit breaker enforcement (PR-3), no
//! summarizer wiring (PR-6). See `docs/design/orchestrator-session-architecture.md`
//! (PR #328) for the full plan.
//!
//! Every new field on existing shapes carries `#[serde(default)]` so older
//! event-log shapes continue to deserialize cleanly during replay (F39 lesson).

use serde::{Deserialize, Serialize};

use crate::ids::{CheckpointId, RunId, SessionId, WorkspaceSnapshotId};
use crate::tenancy::ProjectKey;

// ── IssueBudget ───────────────────────────────────────────────────────────────

/// Per-session budget envelope.
///
/// `None` at any field means "unlimited at this layer" — the circuit-breaker
/// enforcement that lands in PR-3 falls back to per-run defaults when a field
/// is `None`. Operators may set one, any, or all fields.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct IssueBudget {
    /// Cap on total LLM tokens (input + output) spent across the session.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_tokens: Option<u64>,
    /// Cap on total provider cost, in **USD micros** (1 USD = 1_000_000).
    /// Integer storage matches the codebase-wide convention used by
    /// `SessionCostUpdated.delta_cost_micros` and keeps the field `Eq`-able
    /// so it can flow through the event log without loss or NaN hazards.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_cost_micros: Option<u64>,
    /// Cap on total wall-clock seconds elapsed from first attempt start to
    /// terminal outcome, including operator-pause time.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_wall_seconds: Option<u64>,
}

// ── BreakerKind + CircuitBreakerTrip ──────────────────────────────────────────

/// Kinds of circuit breakers that can trip and terminate a session attempt.
///
/// Each variant corresponds to a distinct limit enforced by the orchestrator
/// loop in PR-3. The trip reason flows through [`TerminationReason`] into the
/// [`SessionOutcome`] and is surfaced to operators.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BreakerKind {
    /// Orchestrator iteration count exceeded the per-attempt round cap.
    Round,
    /// Cumulative token spend exceeded the session budget or per-run cap.
    Tokens,
    /// Too many consecutive iterations produced no tool-use proposal.
    NoToolUseConsecutive,
    /// Wall-clock elapsed time exceeded the configured budget.
    WallClock,
}

/// One circuit-breaker trip event.
///
/// Recorded on the event log (via [`crate::events::RuntimeEvent::CircuitBreakerTripped`])
/// and embedded in [`TerminationReason::CircuitBreakerTripped`] when it causes
/// a session attempt to terminate.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CircuitBreakerTrip {
    /// Which breaker fired.
    pub which: BreakerKind,
    /// The measured value that crossed the limit.
    pub measured: u64,
    /// The configured limit that was exceeded.
    pub limit: u64,
    /// Iteration number (0-based) at which the trip was observed.
    pub at_iteration: u32,
}

// ── TerminationReason ─────────────────────────────────────────────────────────

/// Why a session attempt ended.
///
/// Exhaustive classification. Maps directly onto operator-facing copy and
/// the "retry decision" path the orchestrator uses to choose between a fresh
/// attempt and a terminal session outcome.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TerminationReason {
    /// LLM successfully called `complete_run` — the happy path.
    CompleteRun,
    /// A circuit breaker tripped. Carries the trip detail for operator copy.
    CircuitBreakerTripped {
        #[serde(flatten)]
        trip: CircuitBreakerTrip,
    },
    /// The orchestrator lost its task lease (e.g. heartbeat timeout).
    LeaseLost,
    /// A provider returned a terminal error for this attempt.
    ///
    /// **SEC-007**: `message` flows into operator-visible surfaces
    /// (SSE stream, `/v1/runs/:id`, audit log). Emitters MUST sanitize
    /// provider payloads before populating it — strip stack traces,
    /// absolute filesystem paths, and internal identifiers. The
    /// cairn-app `sanitize_for_event_message` helper applies on the
    /// SSE breadcrumb path; PR-3 (which first writes this field)
    /// must route through the same sanitizer or an equivalent before
    /// constructing the event.
    ProviderError { message: String },
    /// An operator explicitly cancelled the session or root run.
    OperatorCancel,
    /// The orchestrator crashed mid-attempt. Carries a diagnostic string.
    ///
    /// **SEC-007**: panic messages often embed stack traces, file paths,
    /// and environment details. Emitters MUST sanitize before
    /// populating `message` — see the note on [`TerminationReason::ProviderError`]
    /// for the same contract.
    Crashed { message: String },
}

// ── SessionOutcome ────────────────────────────────────────────────────────────

/// The rich terminal envelope emitted when a session attempt ends.
///
/// PR-1 defines the shape; PR-6 wires the summarizer that populates
/// `compacted_summary` and `next_step_hint`. Persisted through
/// [`crate::events::RuntimeEvent::SessionOutcomeEmitted`] so downstream
/// consumers (UI, next-session seeder, operator notification) can read it
/// off the event log without coordinating with in-flight services.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionOutcome {
    /// The session this outcome closes out.
    pub session_id: SessionId,
    /// The root (top-level) run of the session.
    pub root_run_id: RunId,
    /// Project scope carried through the event log for routing.
    pub project: ProjectKey,
    /// Checkpoint persisted alongside this outcome (final state snapshot).
    pub checkpoint_id: CheckpointId,
    /// Workspace snapshot captured for this outcome, if the workspace
    /// backend supports durable snapshots. `None` on ephemeral backends.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace_snapshot_id: Option<WorkspaceSnapshotId>,
    /// Classification of why the session terminated.
    pub termination_reason: TerminationReason,
    /// JSON-encoded summary produced by the LLM summarizer in PR-6.
    /// Empty-string placeholder on pre-PR-6 outcomes.
    pub compacted_summary: String,
    /// Optional operator-facing hint for the next manual action.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_step_hint: Option<String>,
    /// Total provider cost in **USD micros** (1 USD = 1_000_000), summed
    /// across all attempts of this session. Integer storage matches
    /// `SessionCostUpdated.delta_cost_micros` and keeps the outcome
    /// `Eq`-able for log-replay equality tests.
    pub cost_micros: u64,
    /// Unix-epoch milliseconds when the outcome was emitted.
    pub emitted_at: u64,
}

// ── Checkpoint ────────────────────────────────────────────────────────────────

/// A durable orchestrator-state snapshot.
///
/// One checkpoint per strategy-triggered iteration (see
/// `cairn_domain::checkpoint_strategy`). The body is a JSON-encoded state
/// blob — v1 is a full snapshot; later iterations may adopt delta encoding.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Checkpoint {
    pub checkpoint_id: CheckpointId,
    pub session_id: SessionId,
    pub root_run_id: RunId,
    /// 0-based iteration number within the current session attempt.
    pub iteration: u32,
    /// Unix-epoch milliseconds when the checkpoint was written.
    pub created_at: u64,
    /// JSON-encoded state blob. Format is orchestrator-defined and versioned.
    pub body_json: String,
}

// ── WorkspaceSnapshot ─────────────────────────────────────────────────────────

/// A durable workspace-filesystem snapshot.
///
/// Created by the workspace backend (overlayfs / reflink / copy fallback) at
/// session-outcome time or on explicit operator checkpoint. The `snapshot_path`
/// is host-local filesystem detail and does **not** appear on the event log
/// payload — only the id surfaces in
/// [`crate::events::RuntimeEvent::WorkspaceSnapshotCreated`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceSnapshot {
    pub snapshot_id: WorkspaceSnapshotId,
    pub workspace_id: crate::ids::WorkspaceId,
    /// Path to the snapshot directory.
    ///
    /// **Portability**: this SHOULD be a path **relative** to the configured
    /// workspace-snapshot root (e.g. `snapshots/<uuid>`) rather than an
    /// absolute host-local path. Storing absolute paths makes snapshots
    /// brittle across restarts, container rebuilds, and multi-node
    /// deployments where the filesystem root differs. The workspace
    /// backend resolves the full filesystem path at access time using the
    /// operator-configured root. PR-1 defines the shape; the workspace
    /// backend that populates this field (PR-3/PR-6) owns the
    /// relative-path contract.
    pub snapshot_path: String,
    /// Unix-epoch milliseconds when the snapshot was created.
    pub created_at: u64,
    /// Unix-epoch milliseconds at which the snapshot may be reaped.
    /// `None` means the snapshot is GC-managed via reachability from
    /// live `SessionOutcome` records rather than a wall-clock TTL.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<u64>,
    /// If this snapshot was built on top of an earlier one (e.g. overlayfs
    /// lower-layer reuse), the parent id is recorded here so the reaper can
    /// walk the chain before deleting.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_snapshot_id: Option<WorkspaceSnapshotId>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn proj() -> ProjectKey {
        ProjectKey::new(
            crate::ids::TenantId::new("t"),
            crate::ids::WorkspaceId::new("w"),
            "p",
        )
    }

    #[test]
    fn issue_budget_default_is_all_none() {
        let b = IssueBudget::default();
        assert!(b.max_tokens.is_none());
        assert!(b.max_cost_micros.is_none());
        assert!(b.max_wall_seconds.is_none());
    }

    #[test]
    fn issue_budget_roundtrip() {
        let b = IssueBudget {
            max_tokens: Some(100_000),
            max_cost_micros: Some(4_250_000),
            max_wall_seconds: Some(3_600),
        };
        let json = serde_json::to_string(&b).unwrap();
        let back: IssueBudget = serde_json::from_str(&json).unwrap();
        assert_eq!(back, b);
    }

    #[test]
    fn issue_budget_backward_compat_empty_object() {
        // Pre-F65 consumers may encode an empty object.
        let back: IssueBudget = serde_json::from_str("{}").unwrap();
        assert_eq!(back, IssueBudget::default());
    }

    #[test]
    fn breaker_kind_roundtrip_snake_case() {
        for k in [
            BreakerKind::Round,
            BreakerKind::Tokens,
            BreakerKind::NoToolUseConsecutive,
            BreakerKind::WallClock,
        ] {
            let s = serde_json::to_string(&k).unwrap();
            let back: BreakerKind = serde_json::from_str(&s).unwrap();
            assert_eq!(back, k);
        }
        let s = serde_json::to_string(&BreakerKind::NoToolUseConsecutive).unwrap();
        assert_eq!(s, "\"no_tool_use_consecutive\"");
    }

    #[test]
    fn circuit_breaker_trip_roundtrip() {
        let t = CircuitBreakerTrip {
            which: BreakerKind::Tokens,
            measured: 120_000,
            limit: 100_000,
            at_iteration: 12,
        };
        let back: CircuitBreakerTrip =
            serde_json::from_str(&serde_json::to_string(&t).unwrap()).unwrap();
        assert_eq!(back, t);
    }

    #[test]
    fn termination_reason_roundtrip_each_variant() {
        let cases = vec![
            TerminationReason::CompleteRun,
            TerminationReason::CircuitBreakerTripped {
                trip: CircuitBreakerTrip {
                    which: BreakerKind::WallClock,
                    measured: 4_000,
                    limit: 3_600,
                    at_iteration: 40,
                },
            },
            TerminationReason::LeaseLost,
            TerminationReason::ProviderError {
                message: "429 rate_limited".to_owned(),
            },
            TerminationReason::OperatorCancel,
            TerminationReason::Crashed {
                message: "panic in decide".to_owned(),
            },
        ];
        for tr in cases {
            let json = serde_json::to_string(&tr).unwrap();
            let back: TerminationReason = serde_json::from_str(&json).unwrap();
            assert_eq!(back, tr);
        }
    }

    #[test]
    fn session_outcome_roundtrip_with_and_without_optionals() {
        let o = SessionOutcome {
            session_id: SessionId::new("s1"),
            root_run_id: RunId::new("r1"),
            project: proj(),
            checkpoint_id: CheckpointId::new("ckpt_1"),
            workspace_snapshot_id: Some(WorkspaceSnapshotId::new("snap_1")),
            termination_reason: TerminationReason::CompleteRun,
            compacted_summary: "{\"headline\":\"done\"}".to_owned(),
            next_step_hint: Some("review PR #123".to_owned()),
            cost_micros: 1_230_000,
            emitted_at: 1_700_000_000_000,
        };
        let back: SessionOutcome =
            serde_json::from_str(&serde_json::to_string(&o).unwrap()).unwrap();
        assert_eq!(back, o);

        // Backward-compat: pre-F65 encoder would not emit optional fields.
        let minimal = serde_json::json!({
            "session_id": "s1",
            "root_run_id": "r1",
            "project": {"tenant_id":"t","workspace_id":"w","project_id":"p"},
            "checkpoint_id": "ckpt_1",
            "termination_reason": {"kind":"complete_run"},
            "compacted_summary": "",
            "cost_micros": 0,
            "emitted_at": 0
        });
        let back: SessionOutcome = serde_json::from_value(minimal).unwrap();
        assert!(back.workspace_snapshot_id.is_none());
        assert!(back.next_step_hint.is_none());
    }

    #[test]
    fn checkpoint_roundtrip() {
        let c = Checkpoint {
            checkpoint_id: CheckpointId::new("ckpt_1"),
            session_id: SessionId::new("s1"),
            root_run_id: RunId::new("r1"),
            iteration: 7,
            created_at: 1_700_000_000_000,
            body_json: "{\"v\":1}".to_owned(),
        };
        let back: Checkpoint = serde_json::from_str(&serde_json::to_string(&c).unwrap()).unwrap();
        assert_eq!(back, c);
    }

    #[test]
    fn workspace_snapshot_roundtrip_with_and_without_optionals() {
        // Use a relative path here to match the documented
        // `snapshot_path` contract (relative to the configured workspace-
        // snapshot root, not an absolute host-local path).
        let s = WorkspaceSnapshot {
            snapshot_id: WorkspaceSnapshotId::new("snap_1"),
            workspace_id: crate::ids::WorkspaceId::new("w"),
            snapshot_path: "snapshots/snap_1".to_owned(),
            created_at: 1_700_000_000_000,
            expires_at: Some(1_700_000_100_000),
            parent_snapshot_id: Some(WorkspaceSnapshotId::new("snap_0")),
        };
        let back: WorkspaceSnapshot =
            serde_json::from_str(&serde_json::to_string(&s).unwrap()).unwrap();
        assert_eq!(back, s);

        let minimal = serde_json::json!({
            "snapshot_id": "snap_1",
            "workspace_id": "w",
            "snapshot_path": "snapshots/snap_1",
            "created_at": 0
        });
        let back: WorkspaceSnapshot = serde_json::from_value(minimal).unwrap();
        assert!(back.expires_at.is_none());
        assert!(back.parent_snapshot_id.is_none());
    }
}
