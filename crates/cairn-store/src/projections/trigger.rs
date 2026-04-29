//! RFC-025 Phase 1.5a: projection records + read-model traits for the
//! trigger / run-template / trigger-fire read models.
//!
//! These replace the in-process HashMaps on `cairn_runtime::TriggerService`
//! that used to be rebuilt by `AppState::replay_triggers` on every boot.
//! Eight state-carrying lifecycle events (TriggerCreated/Enabled/Disabled/
//! Suspended/Resumed/Deleted + RunTemplateCreated/Deleted) project into
//! `triggers` + `run_templates`. Five audit events (TriggerFired/Skipped/
//! Denied/RateLimited/PendingApproval) append into `trigger_fires`; the
//! registry classification stays Ephemeral because no runtime recovery
//! path reads individual rows back, but the table is persistent so
//! rolling-window rate-limit + project-budget counts (and the
//! duplicate-fire ledger) survive restart.
//!
//! Record shapes mirror the pg V035 + sqlite schema exactly — JSON-in-TEXT
//! for the variable-length fields (conditions, allowlists, required
//! fields, fire metadata) per `feedback_no_db_specific_features.md`.

use async_trait::async_trait;
use cairn_domain::ids::{OperatorId, RunTemplateId, TriggerId};
use cairn_domain::tenancy::ProjectKey;
use serde::{Deserialize, Serialize};

use crate::error::StoreError;

/// Current-state row for a trigger.
///
/// Mirror of the `triggers` table. Lifecycle events mutate individual
/// columns in place; `TriggerDeleted` removes the row entirely.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct TriggerRecord {
    pub trigger_id: TriggerId,
    pub project: ProjectKey,
    pub name: String,
    pub description: Option<String>,
    pub signal_type: String,
    pub plugin_id: Option<String>,
    /// `Vec<cairn_runtime::TriggerCondition>` stored as a serde-JSON string
    /// (the runtime type isn't reachable from cairn-store, so we carry the
    /// raw JSON and let the trigger-service layer deserialise).
    pub conditions_json: String,
    pub run_template_id: RunTemplateId,
    pub state: TriggerStateKind,
    /// Populated when `state == Disabled`; operator-supplied reason.
    pub state_reason: Option<String>,
    /// Populated when `state == Suspended`; one of the
    /// `TriggerSuspensionReason` discriminants serialised to its string
    /// form via `enum_to_str`.
    pub suspension_reason: Option<String>,
    /// Wall-clock ms since epoch for the Disabled/Suspended transition.
    /// `None` when state is Enabled.
    pub state_since: Option<u64>,
    pub max_per_minute: u32,
    pub max_burst: u32,
    pub max_chain_depth: u8,
    pub created_by: OperatorId,
    pub created_at: u64,
    pub updated_at: u64,
}

/// Serialised trigger lifecycle state — matches the `state` column on
/// pg/sqlite. The full `TriggerState` (with attached reason + timestamp)
/// is reconstructed from `state` + `state_reason` + `suspension_reason` +
/// `state_since` by the trigger-service layer.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TriggerStateKind {
    Enabled,
    Disabled,
    Suspended,
}

impl TriggerStateKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            TriggerStateKind::Enabled => "enabled",
            TriggerStateKind::Disabled => "disabled",
            TriggerStateKind::Suspended => "suspended",
        }
    }

    /// Parse the string form produced by `as_str`. Name kept as
    /// `parse_str` rather than `from_str` to avoid shadowing
    /// `std::str::FromStr` (clippy::should_implement_trait).
    pub fn parse_str(value: &str) -> Result<Self, StoreError> {
        match value {
            "enabled" => Ok(TriggerStateKind::Enabled),
            "disabled" => Ok(TriggerStateKind::Disabled),
            "suspended" => Ok(TriggerStateKind::Suspended),
            other => Err(StoreError::Internal(format!(
                "unknown trigger state `{other}`"
            ))),
        }
    }
}

/// Canonical string form for `TriggerSuspensionReason` discriminants
/// stored in the `triggers.suspension_reason` column. Kept separate from
/// the serde derive's default representation because serde's
/// `#[serde(rename_all = "snake_case")]` serialises struct variants
/// (namely `RepeatedFailures { failure_count }`) as a JSON object
/// (`{"repeated_failures": {"failure_count": N}}`), which would store a
/// full object string in a TEXT column and break the rehydration match
/// on both the pg/sqlite appliers and the in-memory one. Storing only
/// the discriminant name keeps all three backends byte-identical. The
/// `failure_count` payload for `RepeatedFailures` stays in the event
/// log for forensics.
pub fn suspension_reason_discriminant(
    reason: &cairn_domain::events::TriggerSuspensionReason,
) -> &'static str {
    match reason {
        cairn_domain::events::TriggerSuspensionReason::RateLimitExceeded => "rate_limit_exceeded",
        cairn_domain::events::TriggerSuspensionReason::BudgetExceeded => "budget_exceeded",
        cairn_domain::events::TriggerSuspensionReason::RepeatedFailures { .. } => {
            "repeated_failures"
        }
        cairn_domain::events::TriggerSuspensionReason::OperatorPaused => "operator_paused",
    }
}

/// Canonical string form for `TriggerSkipReason` discriminants. Kept
/// parallel to `suspension_reason_discriminant` — the
/// `MissingRequiredField { field }` variant would otherwise serialise
/// as a JSON object and diverge from the in-memory path which flattens
/// to the discriminant. The `field` payload rides on the
/// `trigger_fires.metadata_json` column when callers need it for
/// debugging.
pub fn skip_reason_discriminant(reason: &cairn_domain::events::TriggerSkipReason) -> &'static str {
    match reason {
        cairn_domain::events::TriggerSkipReason::ConditionMismatch => "condition_mismatch",
        cairn_domain::events::TriggerSkipReason::ChainTooDeep => "chain_too_deep",
        cairn_domain::events::TriggerSkipReason::AlreadyFired => "already_fired",
        cairn_domain::events::TriggerSkipReason::MissingRequiredField { .. } => {
            "missing_required_field"
        }
    }
}

/// Current-state row for a run template.
///
/// Mirror of the `run_templates` table. `RunTemplateCreated` inserts a
/// row; `RunTemplateDeleted` removes it.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct RunTemplateRecord {
    pub template_id: RunTemplateId,
    pub project: ProjectKey,
    pub name: String,
    pub description: Option<String>,
    /// `RunMode` serialised via `enum_to_str`.
    pub default_mode: String,
    pub system_prompt: String,
    pub initial_user_message: Option<String>,
    /// `Option<Vec<String>>` stored as a serde-JSON blob; `None` means "no
    /// restriction" (plugin allowlist absent).
    pub plugin_allowlist_json: Option<String>,
    pub tool_allowlist_json: Option<String>,
    pub budget_max_tokens: Option<u64>,
    pub budget_max_wall_clock_ms: Option<u64>,
    pub budget_max_iterations: Option<u32>,
    pub budget_exploration_budget_share: Option<f32>,
    pub sandbox_hint: Option<String>,
    /// `Vec<String>` stored as a serde-JSON blob — always present (may be
    /// an empty JSON array `[]`).
    pub required_fields_json: String,
    pub created_by: OperatorId,
    pub created_at: u64,
    pub updated_at: u64,
}

/// Outcome classifier for a `trigger_fires` row. Matches the string
/// values stored in the `outcome` TEXT column.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TriggerFireOutcome {
    /// A decision was Approved and a run was created (the event log's
    /// `TriggerFired`). Duplicate-fire ledger queries filter on
    /// `outcome = 'fired'` only; skipped/denied/rate-limited rows do not
    /// block retries.
    Fired,
    /// Precondition failed — condition mismatch, chain too deep, missing
    /// required field, or already-fired ledger hit.
    Skipped,
    /// Decision layer denied the fire.
    Denied,
    /// Rolling-window rate limit was exceeded.
    RateLimited,
    /// Decision layer returned PendingApproval; no run was created but a
    /// subsequent approval event may fire the trigger.
    PendingApproval,
}

impl TriggerFireOutcome {
    pub fn as_str(&self) -> &'static str {
        match self {
            TriggerFireOutcome::Fired => "fired",
            TriggerFireOutcome::Skipped => "skipped",
            TriggerFireOutcome::Denied => "denied",
            TriggerFireOutcome::RateLimited => "rate_limited",
            TriggerFireOutcome::PendingApproval => "pending_approval",
        }
    }

    /// Parse the string form produced by `as_str`. Name kept as
    /// `parse_str` rather than `from_str` to avoid shadowing
    /// `std::str::FromStr` (clippy::should_implement_trait).
    pub fn parse_str(value: &str) -> Result<Self, StoreError> {
        match value {
            "fired" => Ok(TriggerFireOutcome::Fired),
            "skipped" => Ok(TriggerFireOutcome::Skipped),
            "denied" => Ok(TriggerFireOutcome::Denied),
            "rate_limited" => Ok(TriggerFireOutcome::RateLimited),
            "pending_approval" => Ok(TriggerFireOutcome::PendingApproval),
            other => Err(StoreError::Internal(format!(
                "unknown trigger_fires outcome `{other}`"
            ))),
        }
    }
}

/// One append-only row on `trigger_fires`. All five audit variants share
/// the same shape; outcome-specific fields ride on `metadata_json` as a
/// serde-JSON blob (e.g. `{"run_id": "...", "chain_depth": 1}` for Fired,
/// `{"reason": "condition_mismatch"}` for Skipped).
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct TriggerFireRecord {
    pub trigger_id: TriggerId,
    pub project: ProjectKey,
    pub signal_id: String,
    pub outcome: TriggerFireOutcome,
    pub signal_type: Option<String>,
    /// Free-form outcome-specific detail; `None` when nothing beyond the
    /// discriminant matters (e.g. a bare Fired with no run linkage yet).
    pub metadata_json: Option<String>,
    pub at_ms: u64,
}

/// Read model for the `triggers` projection. Durable across restart; all
/// reads go through this trait instead of walking the event log.
#[async_trait]
pub trait TriggerReadModel: Send + Sync {
    async fn get_trigger(
        &self,
        trigger_id: &TriggerId,
    ) -> Result<Option<TriggerRecord>, StoreError>;

    async fn list_triggers_by_project(
        &self,
        project: &ProjectKey,
    ) -> Result<Vec<TriggerRecord>, StoreError>;

    /// Used by the signal evaluation hot path: return all Enabled
    /// triggers in the project whose `signal_type` exactly matches, with
    /// an optional `plugin_id` filter (None passes, Some requires match).
    /// Returns them sorted by `trigger_id` for deterministic evaluation
    /// order (parity with the pre-refactor in-memory path).
    async fn list_matching_enabled(
        &self,
        project: &ProjectKey,
        signal_type: &str,
        plugin_id: &str,
    ) -> Result<Vec<TriggerRecord>, StoreError>;
}

/// Read model for the `run_templates` projection.
#[async_trait]
pub trait RunTemplateReadModel: Send + Sync {
    async fn get_template(
        &self,
        template_id: &RunTemplateId,
    ) -> Result<Option<RunTemplateRecord>, StoreError>;

    async fn list_templates_by_project(
        &self,
        project: &ProjectKey,
    ) -> Result<Vec<RunTemplateRecord>, StoreError>;

    /// Used by `TriggerService::delete_template` to enforce the
    /// "cannot delete a template referenced by any trigger" invariant.
    /// Returns the set of trigger_ids still pointing at `template_id`.
    async fn triggers_referencing_template(
        &self,
        template_id: &RunTemplateId,
    ) -> Result<Vec<TriggerId>, StoreError>;
}

/// Read model for the `trigger_fires` audit projection. Queries here
/// back the fire ledger (duplicate detection) + rate-limit windows +
/// project-budget counting.
#[async_trait]
pub trait TriggerFireReadModel: Send + Sync {
    /// Returns `true` iff there is at least one `outcome = 'fired'` row
    /// for `(trigger_id, signal_id)`. This replaces the in-memory
    /// `HashMap<(TriggerId, SignalId), u64>` fire ledger.
    async fn has_fired(&self, trigger_id: &TriggerId, signal_id: &str) -> Result<bool, StoreError>;

    /// Count `outcome = 'fired'` rows for `trigger_id` with `at_ms > since_ms`.
    /// Backs the per-trigger per-minute rate-limit check.
    async fn count_fires_since(
        &self,
        trigger_id: &TriggerId,
        since_ms: u64,
    ) -> Result<u32, StoreError>;

    /// Count `outcome = 'fired'` rows for `project` with `at_ms > since_ms`.
    /// Backs the per-project per-hour budget check.
    async fn count_project_fires_since(
        &self,
        project: &ProjectKey,
        since_ms: u64,
    ) -> Result<u32, StoreError>;
}
