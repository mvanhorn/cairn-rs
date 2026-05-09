use async_trait::async_trait;
use cairn_domain::{
    CompletionVerification, FailureClass, PauseReason, ProjectKey, PromptReleaseId, ResumeTrigger,
    RunId, RunState, SessionId, TenantId,
};
use serde::{Deserialize, Serialize};

use crate::error::StoreError;

/// Current-state record for a run.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RunRecord {
    pub run_id: RunId,
    pub session_id: SessionId,
    pub parent_run_id: Option<RunId>,
    pub project: ProjectKey,
    pub state: RunState,
    pub prompt_release_id: Option<PromptReleaseId>,
    /// GAP-011: role attached at run creation (e.g. "researcher", "executor").
    #[serde(default)]
    pub agent_role_id: Option<String>,
    pub failure_class: Option<FailureClass>,
    pub pause_reason: Option<PauseReason>,
    pub resume_trigger: Option<ResumeTrigger>,
    pub version: u64,
    pub created_at: u64,
    pub updated_at: u64,
    /// F47 PR2: LLM free-text summary from `LoopTermination::Completed`.
    /// `None` until the run terminates via the normal completion path;
    /// also `None` for records projected from pre-F47-PR2 event logs
    /// (no `RunCompletionAnnotated` ever landed).
    ///
    /// `skip_serializing_if`: the public run surface exposes completion
    /// only via the top-level `completion: RunCompletion` object on
    /// `GET /v1/runs/:id` (F47 PR2). Emitting `completion_summary: null`
    /// on `RunRecord` / `RunRecordView` would double-publish the same
    /// data and leak an internal field name into every list/detail
    /// response — Copilot review on #313 flagged this. Keep the
    /// projection field so the store roundtrips correctly, but omit
    /// it from wire responses unless populated.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completion_summary: Option<String>,
    /// F47 PR2: extractor-produced evidence from tool_results observed
    /// during the run. Paired with `completion_summary`; both populate
    /// on `RunCompletionAnnotated`. `None` mirrors the summary —
    /// post-completion annotation has not (yet) been projected.
    ///
    /// `skip_serializing_if`: see `completion_summary` above — the
    /// top-level `completion` REST object is the intended public
    /// surface; the projection field stays but is omitted from the
    /// wire unless populated.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completion_verification: Option<CompletionVerification>,
    /// F47 PR2: wall-clock ms when `RunCompletionAnnotated` was emitted.
    /// Distinct from `updated_at` because `updated_at` is set by the
    /// projection applier to the current wall-clock on every event it
    /// handles; `completion_annotated_at_ms` is the domain-time the
    /// orchestrator recorded the completion.
    ///
    /// `skip_serializing_if`: see `completion_summary` above.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completion_annotated_at_ms: Option<u64>,
    /// F64: summary of the most recent terminal-write recovery loop, if
    /// one fired for this run. `None` on the hot path (no recovery
    /// needed). Populated by the `TerminalRecoveryAttempted` event
    /// projection and stored as JSON alongside the run row.
    ///
    /// This is a supported optional API field on `RunRecord` — it
    /// appears in the OpenAPI spec and is intended for operator +
    /// audit inspection of runs that hit the recovery path. The
    /// underlying bridge loop retires when FF#371 lands upstream; at
    /// that point writes stop, but existing annotations remain
    /// queryable and the column/event variant stay in the schema.
    ///
    /// `skip_serializing_if`: runs that never hit the recovery path
    /// stay silent in the response body — no noisy
    /// `terminal_write_recovery: null` on every run.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub terminal_write_recovery: Option<TerminalRecoveryRecord>,
    /// #670 G4 PR-1b-1: concurrent-descendants counter per root run.
    ///
    /// On the root run, this tracks the number of in-flight (non-
    /// terminal) descendant runs spawned under it. Each
    /// `SubagentSpawned` event increments the root's counter
    /// atomically via `try_increment_descendants`; each non-root
    /// descendant's terminal event (`RunCompleted` / `RunFailed` /
    /// `RunCanceled`) decrements it.
    ///
    /// Non-root runs always have `0` here — the counter only
    /// accumulates on roots. Value is `i64` (not `u64`) so
    /// underflow is auditable rather than catastrophic.
    ///
    /// `skip_serializing_if == 0`: runs with no descendants
    /// in-flight stay silent in the response body — no noisy
    /// `in_flight_descendants: 0` on every run.
    #[serde(default, skip_serializing_if = "is_zero_i64")]
    pub in_flight_descendants: i64,
    /// #670 G4 PR-1b-1: absolute-root pointer for descendant counter
    /// accounting.
    ///
    /// On a root run (`parent_run_id IS NULL`), this is set to the
    /// run's own `run_id` by the schema default / backfill. On a
    /// non-root descendant, this is set at spawn time to the
    /// captured root id so the decrement path on the descendant's
    /// terminal event targets the correct counter without
    /// re-traversing the `parent_run_id` chain.
    ///
    /// `None` for pre-V069 rows not touched by the backfill (older
    /// child runs created before this migration landed). The
    /// decrement path is a no-op when this is `None` — correct
    /// because pre-V069 spawns never incremented any counter.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub root_run_id: Option<RunId>,
    /// #791: prior-iteration count materialized from
    /// `RunStateChanged { from: WaitingApproval, to: Running }`
    /// projection-apply increments. Read on every `/orchestrate`
    /// POST so `OrchestrationContext.iteration` survives
    /// approval-resume boundaries (#788). Replaces the interim
    /// hot-path event-log scan that PR #790 introduced.
    ///
    /// Initialized to 0 at `RunCreated` apply. Incremented exactly
    /// once per resumed iteration (running→waiting_approval→running
    /// is exactly one suspend/resume cycle). Runs that never
    /// suspend keep iteration=0, which is the correct value the
    /// in-process loop counter would have produced.
    ///
    /// `skip_serializing_if == 0`: runs that have never resumed
    /// stay silent in the response body. Pre-V073 rows projected
    /// without this column default to 0 via `#[serde(default)]`.
    #[serde(default, skip_serializing_if = "is_zero_u32")]
    pub iteration: u32,
}

/// Helper for `#[serde(skip_serializing_if = ...)]` on `u32`
/// fields that are silent-when-zero (e.g., `iteration`).
fn is_zero_u32(v: &u32) -> bool {
    *v == 0
}

/// Helper for `#[serde(skip_serializing_if = ...)]` on `i64` fields
/// that are silent-when-zero. serde's builtin `is_zero` only works
/// on unsigned integers, so define the predicate explicitly.
fn is_zero_i64(v: &i64) -> bool {
    *v == 0
}

/// F64: projection shape for the latest terminal-write recovery attempt.
/// Mirrors `cairn_domain::events::TerminalRecoveryAttempted` minus the
/// ProjectKey + RunId (already carried by the parent `RunRecord`).
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct TerminalRecoveryRecord {
    pub fcall: String,
    pub attempts: u32,
    pub wall_time_ms: u64,
    pub outcome: String,
    pub occurred_at_ms: u64,
}

/// Read-model for run current state.
#[async_trait]
pub trait RunReadModel: Send + Sync {
    async fn get(&self, run_id: &RunId) -> Result<Option<RunRecord>, StoreError>;

    async fn list_by_session(
        &self,
        session_id: &SessionId,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<RunRecord>, StoreError>;

    /// List non-terminal runs in a session (used by session state derivation).
    async fn any_non_terminal(&self, session_id: &SessionId) -> Result<bool, StoreError>;

    /// Get the latest root run (no parent_run_id) in a session.
    async fn latest_root_run(
        &self,
        session_id: &SessionId,
    ) -> Result<Option<RunRecord>, StoreError>;

    /// List runs in a specific state (used by recovery sweeps).
    async fn list_by_state(
        &self,
        state: RunState,
        limit: usize,
    ) -> Result<Vec<RunRecord>, StoreError>;

    /// #670 G4 / RFC 027 + PR-1b-4: list child runs the
    /// `ChildRunDriver` is eligible to claim — `parent_run_id IS
    /// NOT NULL` AND `state IN (Pending, Running)`, ordered by
    /// `(created_at, run_id)` ASC.
    ///
    /// **Why Running too?** Pending alone starves crashed children:
    /// post-SIGKILL the child row stays in `Running` because RFC
    /// 020's recovery sweep emits only advisory markers for
    /// non-wedged Running runs (the main orchestrator's HTTP-pull
    /// model expects an operator to re-POST `/orchestrate` to
    /// resume). Child runs have no operator — the driver IS the
    /// resumption actor. FF's atomic `issue_grant_and_claim`
    /// rejects duplicate claims, so including live-lease children
    /// in the scan is safe: the driver attempts a claim via
    /// `drive_run_iteration` → `renew_lease_if_stale`; if the
    /// lease is healthy elsewhere, FF rejects with
    /// `execution_not_eligible` and the iteration surfaces via
    /// `iterations_err_total`.
    ///
    /// The predicate is pushed into the SQL layer so a tenant with
    /// many pending/running root runs cannot starve the driver by
    /// overflowing a generic `list_by_state` paged scan (Gemini
    /// review on PR #678, HIGH).
    ///
    /// Default implementation (fallback for backends that haven't
    /// specialised yet) union-scans Pending + Running via two
    /// `list_by_state` calls and filters in memory. Backends SHOULD
    /// override with an indexed SQL query; in-memory and pg/sqlite
    /// all override below.
    async fn list_driver_claimable_children(
        &self,
        limit: usize,
    ) -> Result<Vec<RunRecord>, StoreError> {
        let wide_limit = limit.saturating_mul(4);
        let mut pending = self.list_by_state(RunState::Pending, wide_limit).await?;
        let running = self.list_by_state(RunState::Running, wide_limit).await?;
        pending.extend(running);
        pending.sort_by(|a, b| {
            a.created_at
                .cmp(&b.created_at)
                .then_with(|| a.run_id.as_str().cmp(b.run_id.as_str()))
        });
        Ok(pending
            .into_iter()
            .filter(|r| r.parent_run_id.is_some())
            .take(limit)
            .collect())
    }

    /// RFC 010: list non-terminal (active) runs across ALL sessions in a project.
    ///
    /// Operators must be able to view active runs regardless of which session
    /// originated them — session membership is irrelevant to the control-plane
    /// view.
    async fn list_active_by_project(
        &self,
        project: &ProjectKey,
        limit: usize,
    ) -> Result<Vec<RunRecord>, StoreError>;

    /// List child runs of `parent_run_id`, ordered `(created_at, run_id)` ASC.
    ///
    /// Postgres / SQLite use `idx_runs_parent` (partial index on
    /// `parent_run_id WHERE NOT NULL`); InMemoryStore filters the live
    /// map. Caller supplies `limit`; there is no implicit cap.
    async fn list_by_parent_run(
        &self,
        parent_run_id: &RunId,
        limit: usize,
    ) -> Result<Vec<RunRecord>, StoreError>;

    /// List stalled runs for a tenant — non-terminal runs that have
    /// not updated their projection row for longer than
    /// `stale_after_ms` relative to `now_ms` (issue #570).
    ///
    /// Combines state + staleness at the query surface so callers
    /// stop scanning 10 000 Running + 10 000 Pending rows + filtering
    /// in memory on every `/v1/runs/stalled` refresh (the pre-#570
    /// shape). Results are ordered `updated_at ASC, run_id ASC` so
    /// the most-stale runs surface on page 1.
    ///
    /// Callers pass `limit + 1` to detect `has_more`. The InMemory
    /// implementation filters in-memory; pg/sqlite implementations
    /// (if added later) should apply the predicate at the SQL layer.
    async fn list_stalled(
        &self,
        tenant_id: &TenantId,
        now_ms: u64,
        stale_after_ms: u64,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<RunRecord>, StoreError>;
}

/// #670 G4 PR-1b-1: atomic compare-and-increment / decrement primitive
/// for the `in_flight_descendants` counter on a root run's row.
///
/// **Scope of this primitive**: it reads + mutates only the **root**
/// row identified by `root_run_id`. It does NOT populate the child
/// run's own `root_run_id` column — the child's row is created
/// separately by the `RunCreated` projection, which currently leaves
/// `root_run_id = None` on non-root runs (see the module-level
/// comment above). PR-1b-3 ships the complementary change: when the
/// spawn path mints a child, it will (a) call
/// [`Self::try_increment_descendants`] on the captured root id to
/// reserve a slot under the cap, and (b) emit the `RunCreated` event
/// carrying the resolved root id so the `RunCreated` projection sets
/// the child's `root_run_id` atomically in the same transaction.
/// Those are two separate writes that the spawn path composes; they
/// are not rolled into this one primitive.
///
/// **Fan-out gate**: this is the authoritative cap check. The
/// durable backend's atomic SQL `UPDATE ... WHERE counter < :cap
/// RETURNING` (on pg/sqlite) or `i64` CAS loop (InMemory) is what
/// prevents two concurrent spawns from both admitting above the cap.
/// The in-memory projection dual-writes follow the durable-backend
/// decision via the adapter-layer rollback-on-reject path specified
/// in RFC 027 §`in_flight_descendants` counter.
///
/// Consumers (the subagent driver in PR-1b-3) call
/// [`Self::try_increment_descendants`] at spawn time; if it returns
/// [`DescendantsCapOutcome::CapReached`], the caller rejects the spawn
/// with `SubagentFanoutLimitReached` + rolls back any Phase-1 side
/// effects. [`Self::decrement_descendants`] fires on every non-root
/// descendant's terminal event and on the orphan-child compensating
/// path (per RFC 027 §Orphan-child).
///
/// The primitive lives on main from PR-1b-1 onward but has no
/// callers until PR-1b-3. PR-1b-1 ships the primitive, PR-1b-3
/// wires it into the spawn path.
#[async_trait]
pub trait RunDescendantsCounter: Send + Sync {
    /// Conditionally increment `in_flight_descendants` on the row
    /// identified by `root_run_id`. Returns `Admitted { new_count }`
    /// on success, `CapReached` when the counter would exceed `cap`,
    /// and `RootNotFound` when no row matches `root_run_id` (a bug
    /// condition the caller can surface as `RuntimeError::Internal`).
    ///
    /// Atomicity: the durable backend performs the check + increment
    /// in a single SQL statement; InMemory uses a CAS loop on an
    /// `i64`. Two concurrent calls against the same root cannot both
    /// admit above the cap.
    async fn try_increment_descendants(
        &self,
        root_run_id: &RunId,
        cap: i64,
    ) -> Result<DescendantsCapOutcome, StoreError>;

    /// Unconditionally decrement `in_flight_descendants` on the row
    /// identified by `root_run_id`. Returns the post-decrement value.
    /// Underflow (post-decrement < 0) returns `new_count` negative —
    /// the adapter surfaces this as a WARN metric per RFC 027; the
    /// primitive itself does not panic.
    ///
    /// Called on every non-root descendant's terminal event, keyed by
    /// the `root_run_id` captured at spawn time on the descendant's
    /// row. `RootNotFound` returns without side effect (caller logs).
    ///
    /// **No-op on a `None` root_run_id**: callers that pass a pre-
    /// V069 descendant with `root_run_id = None` see this reflected
    /// as `RootNotFound`; per RFC 027 that's the correct no-op
    /// because pre-V069 spawns never incremented anything.
    async fn decrement_descendants(
        &self,
        root_run_id: &RunId,
    ) -> Result<DescendantsCapOutcome, StoreError>;

    /// #670 G4 PR-1b-4: list every root run with a non-zero
    /// `in_flight_descendants` counter. Used by cairn-app's boot
    /// reconciliation pass: the counter is mutated via direct SQL
    /// UPDATE (not event-sourced), so the event-log replay that
    /// rebuilds the in-memory projection does NOT restore the
    /// counter's authoritative value. After replay, cairn-app reads
    /// this list from the durable backend and writes the values onto
    /// the in-memory projection via
    /// [`InMemoryStore::restore_descendants_counter`], reconciling
    /// the two views.
    ///
    /// Implementations SHOULD cap the result set at a defensive
    /// limit to avoid loading millions of rows at boot; real
    /// deployments of cairn today have at most thousands of live
    /// roots with active descendants, so a 10k cap is more than
    /// sufficient. Rows past the cap would have their counter stay
    /// at 0 on the in-memory projection until a live write resets
    /// it, which is worse than reconciling them but still
    /// durable-side-correct (the durable backend is authoritative).
    async fn list_nonzero_descendant_counters(&self) -> Result<Vec<(RunId, i64)>, StoreError>;
}

/// Outcome of a [`RunDescendantsCounter`] operation. Returning a
/// typed enum (rather than `Result<i64, CapError>`) keeps the caller's
/// match arms explicit — the cap-reached path is business logic, not
/// a surprise error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DescendantsCapOutcome {
    /// Increment or decrement succeeded. `new_count` is the post-op
    /// value of `in_flight_descendants`. May be negative on decrement
    /// if a bug caused underflow; callers log, don't panic.
    Admitted { new_count: i64 },
    /// Increment rejected because `in_flight_descendants + 1 > cap`.
    /// Caller rejects the spawn with `SubagentFanoutLimitReached` and
    /// rolls back any Phase-1 side effects. Only returned from
    /// [`RunDescendantsCounter::try_increment_descendants`].
    CapReached,
    /// No row matched the supplied `root_run_id`. On increment this
    /// is a bug (root was deleted mid-spawn?) — caller should
    /// surface `RuntimeError::Internal`. On decrement this is the
    /// no-op path for pre-V069 descendants (`root_run_id = None`
    /// resolved on the caller side; adapter sees the absence here).
    RootNotFound,
}
