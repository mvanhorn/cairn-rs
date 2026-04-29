//! Trigger service — RFC 022: Binding Signals to Runs.
//!
//! A Trigger is a project-scoped declarative rule: "when a signal of type X
//! arrives matching condition Y, create a run from template Z."
//!
//! A RunTemplate is a reusable run configuration that a Trigger references.
//!
//! The trigger evaluator is a runtime worker that subscribes to the signal
//! router (RFC 015) and creates runs for matching triggers.

use std::collections::HashSet;
use std::time::{SystemTime, UNIX_EPOCH};

use cairn_domain::decisions::RunMode;
use cairn_domain::ids::{
    ApprovalId, DecisionId, OperatorId, RunId, RunTemplateId, SignalId, TriggerId,
};
use cairn_domain::tenancy::ProjectKey;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

// ── Trigger Entity (RFC 022 §"The Trigger Entity") ──────────────────────────

/// A project-scoped rule that binds signals to runs.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Trigger {
    pub id: TriggerId,
    pub project: ProjectKey,
    pub name: String,
    pub description: Option<String>,
    pub signal_pattern: SignalPattern,
    pub conditions: Vec<TriggerCondition>,
    pub run_template_id: RunTemplateId,
    pub state: TriggerState,
    pub rate_limit: RateLimitConfig,
    pub max_chain_depth: u8,
    pub created_by: OperatorId,
    pub created_at: u64,
    pub updated_at: u64,
}

/// Which signals this trigger matches.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignalPattern {
    /// Signal type (exact match in v1, e.g. "github.issue.labeled").
    pub signal_type: String,
    /// Optional plugin ID restriction. If set, only signals from this plugin match.
    pub plugin_id: Option<String>,
}

/// Trigger lifecycle state.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum TriggerState {
    Enabled,
    Disabled {
        reason: Option<String>,
        since: u64,
    },
    Suspended {
        reason: SuspensionReason,
        since: u64,
    },
}

/// Why a trigger was automatically suspended.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SuspensionReason {
    RateLimitExceeded,
    BudgetExceeded,
    RepeatedFailures { failure_count: u32 },
    OperatorPaused,
}

// ── Trigger Condition DSL (RFC 022 §"The Trigger Entity") ────────────────────

/// Condition for matching a signal payload.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TriggerCondition {
    /// JSON path equals value: payload.action == "labeled"
    Equals {
        path: String,
        value: serde_json::Value,
    },
    /// JSON path's array contains a value: payload.labels[].name contains "cairn-ready"
    Contains {
        path: String,
        value: serde_json::Value,
    },
    /// JSON path is non-null
    Exists { path: String },
    /// Negate a child condition
    Not(Box<TriggerCondition>),
}

impl Serialize for TriggerCondition {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        trigger_condition_to_value(self).serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for TriggerCondition {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = serde_json::Value::deserialize(deserializer)?;
        trigger_condition_from_value(value).map_err(serde::de::Error::custom)
    }
}

fn trigger_condition_to_value(condition: &TriggerCondition) -> serde_json::Value {
    match condition {
        TriggerCondition::Equals { path, value } => serde_json::json!({
            "type": "equals",
            "path": path,
            "value": value,
        }),
        TriggerCondition::Contains { path, value } => serde_json::json!({
            "type": "contains",
            "path": path,
            "value": value,
        }),
        TriggerCondition::Exists { path } => serde_json::json!({
            "type": "exists",
            "path": path,
        }),
        TriggerCondition::Not(inner) => serde_json::json!({
            "type": "not",
            "condition": trigger_condition_to_value(inner),
        }),
    }
}

fn trigger_condition_from_value(value: serde_json::Value) -> Result<TriggerCondition, String> {
    let object = value
        .as_object()
        .ok_or_else(|| "trigger condition must be an object".to_owned())?;
    let kind = object
        .get("type")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| "trigger condition is missing string field `type`".to_owned())?;

    match kind {
        "equals" => Ok(TriggerCondition::Equals {
            path: condition_path(object)?,
            value: condition_value(object)?,
        }),
        "contains" => Ok(TriggerCondition::Contains {
            path: condition_path(object)?,
            value: condition_value(object)?,
        }),
        "exists" => Ok(TriggerCondition::Exists {
            path: condition_path(object)?,
        }),
        "not" => {
            let nested = object
                .get("condition")
                .or_else(|| object.get("inner"))
                .cloned()
                .ok_or_else(|| {
                    "trigger condition `not` is missing object field `condition`".to_owned()
                })?;
            Ok(TriggerCondition::Not(Box::new(
                trigger_condition_from_value(nested)?,
            )))
        }
        other => Err(format!("unsupported trigger condition type `{other}`")),
    }
}

fn condition_path(object: &serde_json::Map<String, serde_json::Value>) -> Result<String, String> {
    object
        .get("path")
        .and_then(serde_json::Value::as_str)
        .map(ToOwned::to_owned)
        .ok_or_else(|| "trigger condition is missing string field `path`".to_owned())
}

fn condition_value(
    object: &serde_json::Map<String, serde_json::Value>,
) -> Result<serde_json::Value, String> {
    object
        .get("value")
        .cloned()
        .ok_or_else(|| "trigger condition is missing field `value`".to_owned())
}

/// Rate limit configuration for a trigger (token bucket).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RateLimitConfig {
    pub max_per_minute: u32,
    pub max_burst: u32,
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            max_per_minute: 10,
            max_burst: 20,
        }
    }
}

// ── RunTemplate Entity (RFC 022 §"The RunTemplate Entity") ──────────────────

/// A reusable run configuration that a Trigger references.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RunTemplate {
    pub id: RunTemplateId,
    pub project: ProjectKey,
    pub name: String,
    pub description: Option<String>,
    pub default_mode: RunMode,
    pub system_prompt: String,
    pub initial_user_message: Option<String>,
    pub plugin_allowlist: Option<Vec<String>>,
    pub tool_allowlist: Option<Vec<String>>,
    pub budget: TemplateBudget,
    pub sandbox_hint: Option<String>,
    pub required_fields: Vec<String>,
    pub created_by: OperatorId,
    pub created_at: u64,
    pub updated_at: u64,
}

/// Default budget caps for runs created from a template.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct TemplateBudget {
    pub max_tokens: Option<u64>,
    pub max_wall_clock_ms: Option<u64>,
    pub max_iterations: Option<u32>,
    pub exploration_budget_share: Option<f32>,
}

// ── Trigger Events (RFC 022 §"Events") ──────────────────────────────────────

/// Events emitted by the trigger service.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum TriggerEvent {
    TriggerCreated {
        trigger_id: TriggerId,
        project: ProjectKey,
        signal_pattern: SignalPattern,
        run_template_id: RunTemplateId,
        created_by: OperatorId,
        created_at: u64,
    },
    TriggerUpdated {
        trigger_id: TriggerId,
        updated_by: OperatorId,
        updated_at: u64,
    },
    TriggerEnabled {
        trigger_id: TriggerId,
        by: OperatorId,
        at: u64,
    },
    TriggerDisabled {
        trigger_id: TriggerId,
        by: OperatorId,
        reason: Option<String>,
        at: u64,
    },
    TriggerSuspended {
        trigger_id: TriggerId,
        reason: SuspensionReason,
        at: u64,
    },
    TriggerResumed {
        trigger_id: TriggerId,
        at: u64,
    },
    TriggerDeleted {
        trigger_id: TriggerId,
        by: OperatorId,
        at: u64,
    },
    TriggerFired {
        trigger_id: TriggerId,
        signal_id: SignalId,
        signal_type: String,
        run_id: RunId,
        chain_depth: u8,
        fired_at: u64,
    },
    TriggerSkipped {
        trigger_id: TriggerId,
        signal_id: SignalId,
        reason: SkipReason,
        skipped_at: u64,
    },
    TriggerDenied {
        trigger_id: TriggerId,
        signal_id: SignalId,
        decision_id: DecisionId,
        reason: String,
        denied_at: u64,
    },
    TriggerRateLimited {
        trigger_id: TriggerId,
        signal_id: SignalId,
        bucket_remaining: u32,
        bucket_capacity: u32,
        rate_limited_at: u64,
    },
    TriggerPendingApproval {
        trigger_id: TriggerId,
        signal_id: SignalId,
        approval_id: ApprovalId,
        pending_at: u64,
    },
    RunTemplateCreated {
        template_id: RunTemplateId,
        project: ProjectKey,
        name: String,
        default_mode: RunMode,
        created_by: OperatorId,
        created_at: u64,
    },
    RunTemplateUpdated {
        template_id: RunTemplateId,
        updated_by: OperatorId,
        updated_at: u64,
    },
    RunTemplateDeleted {
        template_id: RunTemplateId,
        by: OperatorId,
        at: u64,
    },
}

/// Reason a trigger fire was skipped.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SkipReason {
    ConditionMismatch,
    ChainTooDeep,
    AlreadyFired,
    MissingRequiredField { field: String },
}

// ── Condition Evaluator ─────────────────────────────────────────────────────

/// Evaluate a trigger condition against a JSON payload.
pub fn evaluate_condition(condition: &TriggerCondition, payload: &serde_json::Value) -> bool {
    match condition {
        TriggerCondition::Equals { path, value } => resolve_path(payload, path) == Some(value),
        TriggerCondition::Contains { path, value } => {
            // For array paths like "labels[].name", check if any element matches
            resolve_array_path(payload, path).iter().any(|v| v == value)
        }
        TriggerCondition::Exists { path } => resolve_path(payload, path).is_some(),
        TriggerCondition::Not(inner) => !evaluate_condition(inner, payload),
    }
}

/// Evaluate all conditions — all must pass (AND semantics).
pub fn evaluate_conditions(conditions: &[TriggerCondition], payload: &serde_json::Value) -> bool {
    conditions.iter().all(|c| evaluate_condition(c, payload))
}

/// Resolve a dot-notation path to a JSON value.
/// E.g. "issue.number" resolves `{"issue": {"number": 42}}` to `42`.
fn resolve_path<'a>(value: &'a serde_json::Value, path: &str) -> Option<&'a serde_json::Value> {
    let mut current = value;
    for segment in path.split('.') {
        // Handle array access like "labels[]"
        if segment.ends_with("[]") {
            return None; // Array paths use resolve_array_path
        }
        current = current.get(segment)?;
    }
    Some(current)
}

/// Resolve a dot-notation path with array expansion.
/// E.g. "labels[].name" on `{"labels": [{"name": "bug"}, {"name": "cairn-ready"}]}`
/// returns `["bug", "cairn-ready"]`.
fn resolve_array_path(value: &serde_json::Value, path: &str) -> Vec<serde_json::Value> {
    let parts: Vec<&str> = path.splitn(2, "[].").collect();
    if parts.len() != 2 {
        // No array expansion — fall back to scalar
        return resolve_path(value, path).cloned().into_iter().collect();
    }

    let array_path = parts[0];
    let field_path = parts[1];

    let array = match resolve_path(value, array_path) {
        Some(serde_json::Value::Array(arr)) => arr,
        _ => return Vec::new(),
    };

    array
        .iter()
        .filter_map(|item| resolve_path(item, field_path).cloned())
        .collect()
}

// ── Variable Substitution (RFC 022 §"Variable Substitution") ────────────────

/// Substitute `{{path.to.field}}` placeholders with values from the signal payload.
///
/// Returns the expanded string and a list of missing required fields (if any).
pub fn substitute_variables(
    template: &str,
    payload: &serde_json::Value,
    required_fields: &[String],
) -> Result<String, Vec<String>> {
    let result = template.to_string();
    let mut missing = Vec::new();

    // Find all {{...}} patterns
    let mut start = 0;
    let mut output = String::with_capacity(template.len());

    while let Some(open) = result[start..].find("{{") {
        let abs_open = start + open;
        output.push_str(&result[start..abs_open]);

        if let Some(close) = result[abs_open + 2..].find("}}") {
            let abs_close = abs_open + 2 + close;
            let path = &result[abs_open + 2..abs_close];

            // Resolve the path
            let value = if path.contains("[].") {
                let values = resolve_array_path(payload, path);
                if values.is_empty() {
                    String::new()
                } else {
                    values
                        .iter()
                        .map(value_to_string)
                        .collect::<Vec<_>>()
                        .join(", ")
                }
            } else {
                resolve_path(payload, path)
                    .map(value_to_string)
                    .unwrap_or_default()
            };

            output.push_str(&value);
            start = abs_close + 2;
        } else {
            // No closing braces — leave as-is
            output.push_str("{{");
            start = abs_open + 2;
        }
    }
    output.push_str(&result[start..]);

    // Check required fields
    for field in required_fields {
        if resolve_path(payload, field).is_none() {
            missing.push(field.clone());
        }
    }

    if missing.is_empty() {
        Ok(output)
    } else {
        Err(missing)
    }
}

fn value_to_string(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::Bool(b) => b.to_string(),
        serde_json::Value::Null => String::new(),
        other => other.to_string(),
    }
}

// ── Decision Layer Integration (RFC 019 × RFC 022) ──────────────────────────

/// Outcome of submitting a trigger fire to the decision layer.
///
/// In production, this is the result of `DecisionService::evaluate()` for
/// `DecisionKind::TriggerFire`. In tests, callers can supply a closure
/// that returns the desired outcome.
#[derive(Clone, Debug)]
pub enum TriggerDecisionOutcome {
    /// Decision layer approved the fire.
    Approved { decision_id: DecisionId },
    /// Decision layer denied the fire.
    Denied {
        decision_id: DecisionId,
        reason: String,
    },
    /// Decision layer requires human/guardian approval before proceeding.
    PendingApproval { approval_id: ApprovalId },
}

/// Default decision function that auto-approves all trigger fires.
/// Used in tests and when no DecisionService is configured.
pub fn auto_approve_decision(
    _trigger_id: &TriggerId,
    _signal_type: &str,
) -> TriggerDecisionOutcome {
    TriggerDecisionOutcome::Approved {
        decision_id: DecisionId::new(format!("auto_{}", now_ms())),
    }
}

// ── TriggerService (RFC-025 Phase 1.5a: projection-backed, async) ───────────

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Rolling-window sizes for the rate-limit + project-budget checks.
///
/// These live at module scope (rather than inside `TriggerService`) so the
/// in-memory test harness can override them in the rare case one day, and
/// so they stay as a single source of truth across the hot path + the
/// decision_candidates_for_signal preview path.
const TRIGGER_RATE_LIMIT_WINDOW_MS: u64 = 60_000;
const PROJECT_BUDGET_WINDOW_MS: u64 = 3_600_000;
/// Default per-project budget (fires per hour). Equals the legacy
/// in-memory service's value so the first cut of Phase 1.5a doesn't
/// change observable behaviour. Config-driven override is out of scope;
/// track via RFC-025 Phase 2.
pub const DEFAULT_PROJECT_BUDGET_PER_HOUR: u32 = 100;

/// Errors raised by the async `TriggerService` CRUD + evaluation paths.
#[derive(Clone, Debug)]
pub enum TriggerError {
    TriggerNotFound(TriggerId),
    TemplateNotFound(RunTemplateId),
    TemplateInUse {
        template_id: RunTemplateId,
        trigger_ids: Vec<TriggerId>,
    },
    NotSuspended(TriggerId),
    /// Persistence failed (projection read, projection write, or event-log
    /// append). Carries the upstream message so the operator sees an
    /// actionable line rather than a bare "I/O".
    Store(String),
}

impl std::fmt::Display for TriggerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TriggerNotFound(id) => write!(f, "trigger not found: {id}"),
            Self::TemplateNotFound(id) => write!(f, "run template not found: {id}"),
            Self::TemplateInUse {
                template_id,
                trigger_ids,
            } => write!(
                f,
                "template {template_id} is referenced by triggers: {}",
                trigger_ids
                    .iter()
                    .map(|id| id.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            Self::NotSuspended(id) => write!(f, "trigger {id} is not suspended"),
            Self::Store(msg) => write!(f, "trigger store error: {msg}"),
        }
    }
}

impl std::error::Error for TriggerError {}

impl From<cairn_store::StoreError> for TriggerError {
    fn from(err: cairn_store::StoreError) -> Self {
        TriggerError::Store(err.to_string())
    }
}

/// Intermediate pre-decision outcome used by both the evaluator (which
/// drives the fire path) and the preview (`decision_candidates_for_signal`).
/// Private — callers see `TriggerEvent` variants in the return vector.
#[derive(Clone, Debug)]
enum PreDecision {
    Ready,
    Skipped(SkipReason),
    RateLimited { bucket_capacity: u32 },
    BudgetExceeded,
}

/// Runtime-side, projection-backed trigger service.
///
/// Previous in-memory `TriggerService` held four HashMaps + a Vec that
/// had to be rebuilt on every process boot via `AppState::replay_triggers`
/// (deleted in RFC-025 Phase 1.5a). Durable state now lives in the
/// `triggers` / `run_templates` / `trigger_fires` projection tables; this
/// struct is a thin stateless facade that issues projection reads and
/// runs the sync decision logic (condition matching / chain-depth /
/// rate-limit / project-budget / required-fields).
///
/// Write-path split between CRUD and evaluation:
/// * **CRUD** (`create_template`, `create_trigger`, `enable_trigger`,
///   `disable_trigger`, `resume_trigger`, `delete_trigger`,
///   `delete_template`) — the service builds the `RuntimeEvent` and
///   appends it through the event log itself. The projection applier
///   updates `triggers` / `run_templates` inside the same transaction.
///   Callers receive the `TriggerEvent` for HTTP response bodies.
/// * **Evaluation** (`decision_candidates_for_signal`,
///   `evaluate_signal_for_candidates`, `evaluate_signal`) — returns a
///   `Vec<TriggerEvent>` for the caller to persist via the event log
///   (see the signal handler for the standard write pattern). The
///   evaluator does NOT append on its own because the caller often
///   wants to record telemetry + append a single batched envelope for
///   the whole signal (matching the pre-refactor behaviour).
///
/// The service is cheap to clone (wraps an `Arc<S>`) and holds no
/// writable state of its own. Concurrent evaluate calls for the same
/// project are serialised only by the projection's append transaction;
/// the rate-limit + duplicate-fire window check relies on SQL COUNT(*)
/// being consistent at that isolation level. True concurrency window:
/// if two ingest paths evaluate the SAME `(trigger_id, signal_id)`
/// pair simultaneously they can both clear the `has_fired` pre-check
/// before either writes a 'fired' row — both will emit TriggerFired
/// and both rows can land in `trigger_fires`. The event log does NOT
/// deduplicate on signal_id, so a duplicate row is possible under
/// race. Callers that need strict at-most-once per-signal semantics
/// must serialise signal ingest on `signal_id` upstream of
/// `evaluate_signal_for_candidates`. In practice cairn-app routes each
/// signal through a single handler invocation so this race is not
/// observed; the ledger check is a defensive barrier rather than a
/// strong concurrency guarantee (PR #569 Copilot review).
pub struct TriggerService<S> {
    store: std::sync::Arc<S>,
    /// Hourly budget cap used by `pre_decision_status` when the signal's
    /// project lacks an explicit limit. Kept public so tests + future
    /// config overrides can adjust it without reaching into the service.
    pub default_project_budget: u32,
}

impl<S> Clone for TriggerService<S> {
    fn clone(&self) -> Self {
        Self {
            store: self.store.clone(),
            default_project_budget: self.default_project_budget,
        }
    }
}

impl<S> TriggerService<S>
where
    S: cairn_store::projections::TriggerReadModel
        + cairn_store::projections::RunTemplateReadModel
        + cairn_store::projections::TriggerFireReadModel
        + cairn_store::EventLog
        + 'static,
{
    pub fn new(store: std::sync::Arc<S>) -> Self {
        Self {
            store,
            default_project_budget: DEFAULT_PROJECT_BUDGET_PER_HOUR,
        }
    }

    /// Append a lifecycle event with `EventSource::Operator { operator_id }`
    /// so the audit trail reflects who made the change. Preserves the
    /// pre-refactor auditability (PR #569 review) — the previous handler
    /// path wrapped events with the principal's operator id before
    /// appending; the service now does the wrap internally.
    async fn append_as_operator(
        &self,
        operator_id: cairn_domain::ids::OperatorId,
        event: cairn_domain::RuntimeEvent,
    ) -> Result<(), TriggerError> {
        let event_id = super::event_helpers::next_event_id();
        let mut envelope = cairn_domain::EventEnvelope::for_runtime_event(
            event_id,
            cairn_domain::EventSource::Operator { operator_id },
            event,
        );
        let trace_id = crate::get_current_trace_id();
        if !trace_id.is_empty() {
            envelope = envelope.with_correlation_id(trace_id);
        }
        self.store
            .append(&[envelope])
            .await
            .map(|_| ())
            .map_err(TriggerError::from)
    }

    // ── Template CRUD ────────────────────────────────────────────────

    pub async fn create_template(
        &self,
        template: RunTemplate,
    ) -> Result<TriggerEvent, TriggerError> {
        let event = cairn_domain::RuntimeEvent::RunTemplateCreated(
            cairn_domain::events::RunTemplateCreated {
                project: template.project.clone(),
                template_id: template.id.clone(),
                name: template.name.clone(),
                description: template.description.clone(),
                default_mode: template.default_mode.clone(),
                system_prompt: template.system_prompt.clone(),
                initial_user_message: template.initial_user_message.clone(),
                plugin_allowlist: template.plugin_allowlist.clone(),
                tool_allowlist: template.tool_allowlist.clone(),
                budget_max_tokens: template.budget.max_tokens,
                budget_max_wall_clock_ms: template.budget.max_wall_clock_ms,
                budget_max_iterations: template.budget.max_iterations,
                budget_exploration_budget_share: template.budget.exploration_budget_share,
                sandbox_hint: template.sandbox_hint.clone(),
                required_fields: template.required_fields.clone(),
                created_by: template.created_by.clone(),
                created_at: template.created_at,
            },
        );
        self.append_as_operator(template.created_by.clone(), event)
            .await?;
        Ok(TriggerEvent::RunTemplateCreated {
            template_id: template.id,
            project: template.project,
            name: template.name,
            default_mode: template.default_mode,
            created_by: template.created_by,
            created_at: template.created_at,
        })
    }

    pub async fn get_template(
        &self,
        id: &RunTemplateId,
    ) -> Result<Option<RunTemplate>, TriggerError> {
        let rec =
            cairn_store::projections::RunTemplateReadModel::get_template(self.store.as_ref(), id)
                .await?;
        rec.map(run_template_from_record).transpose()
    }

    pub async fn list_templates_for_project(
        &self,
        project: &ProjectKey,
    ) -> Result<Vec<RunTemplate>, TriggerError> {
        let records = cairn_store::projections::RunTemplateReadModel::list_templates_by_project(
            self.store.as_ref(),
            project,
        )
        .await?;
        records.into_iter().map(run_template_from_record).collect()
    }

    pub async fn delete_template(
        &self,
        id: &RunTemplateId,
        by: OperatorId,
    ) -> Result<TriggerEvent, TriggerError> {
        // Fetch once — carry the record forward for the event's
        // `project` field so we don't issue a second read after the
        // referential-integrity check (Copilot review PR #569).
        let record = match cairn_store::projections::RunTemplateReadModel::get_template(
            self.store.as_ref(),
            id,
        )
        .await?
        {
            Some(rec) => rec,
            None => return Err(TriggerError::TemplateNotFound(id.clone())),
        };

        // Referential integrity: block deletion while any trigger still
        // points at the template. Projection read backs this check on
        // all three backends.
        let referencing =
            cairn_store::projections::RunTemplateReadModel::triggers_referencing_template(
                self.store.as_ref(),
                id,
            )
            .await?;
        if !referencing.is_empty() {
            return Err(TriggerError::TemplateInUse {
                template_id: id.clone(),
                trigger_ids: referencing,
            });
        }

        let at = now_ms();
        let event = cairn_domain::RuntimeEvent::RunTemplateDeleted(
            cairn_domain::events::RunTemplateDeleted {
                project: record.project.clone(),
                template_id: id.clone(),
                by: by.clone(),
                at,
            },
        );
        self.append_as_operator(by.clone(), event).await?;
        Ok(TriggerEvent::RunTemplateDeleted {
            template_id: id.clone(),
            by,
            at,
        })
    }

    // ── Trigger CRUD ────────────────────────────────────────────────

    pub async fn create_trigger(&self, trigger: Trigger) -> Result<TriggerEvent, TriggerError> {
        // Template must exist; matches the pre-refactor invariant.
        if cairn_store::projections::RunTemplateReadModel::get_template(
            self.store.as_ref(),
            &trigger.run_template_id,
        )
        .await?
        .is_none()
        {
            return Err(TriggerError::TemplateNotFound(
                trigger.run_template_id.clone(),
            ));
        }

        // Serialise each condition explicitly so a serde error surfaces
        // as TriggerError::Store — the previous `unwrap_or(Null)` would
        // have silently written a `null` condition that then failed to
        // deserialise when the projection was read back, effectively
        // bricking the trigger (Copilot review PR #569). In practice
        // serde_json::to_value on a `TriggerCondition` (which has hand-
        // rolled Serialize/Deserialize) cannot fail, but the error path
        // is the correct shape.
        let conditions: Result<Vec<serde_json::Value>, TriggerError> = trigger
            .conditions
            .iter()
            .map(|c| {
                serde_json::to_value(c).map_err(|err| {
                    TriggerError::Store(format!(
                        "trigger {} condition serialisation failed: {err}",
                        trigger.id
                    ))
                })
            })
            .collect();
        let event =
            cairn_domain::RuntimeEvent::TriggerCreated(cairn_domain::events::TriggerCreated {
                project: trigger.project.clone(),
                trigger_id: trigger.id.clone(),
                name: trigger.name.clone(),
                description: trigger.description.clone(),
                signal_type: trigger.signal_pattern.signal_type.clone(),
                plugin_id: trigger.signal_pattern.plugin_id.clone(),
                conditions: conditions?,
                run_template_id: trigger.run_template_id.clone(),
                max_per_minute: trigger.rate_limit.max_per_minute,
                max_burst: trigger.rate_limit.max_burst,
                max_chain_depth: trigger.max_chain_depth,
                created_by: trigger.created_by.clone(),
                created_at: trigger.created_at,
            });
        self.append_as_operator(trigger.created_by.clone(), event)
            .await?;
        Ok(TriggerEvent::TriggerCreated {
            trigger_id: trigger.id,
            project: trigger.project,
            signal_pattern: trigger.signal_pattern,
            run_template_id: trigger.run_template_id,
            created_by: trigger.created_by,
            created_at: trigger.created_at,
        })
    }

    pub async fn get_trigger(&self, id: &TriggerId) -> Result<Option<Trigger>, TriggerError> {
        let rec = cairn_store::projections::TriggerReadModel::get_trigger(self.store.as_ref(), id)
            .await?;
        rec.map(trigger_from_record).transpose()
    }

    pub async fn list_triggers_for_project(
        &self,
        project: &ProjectKey,
    ) -> Result<Vec<Trigger>, TriggerError> {
        let records = cairn_store::projections::TriggerReadModel::list_triggers_by_project(
            self.store.as_ref(),
            project,
        )
        .await?;
        records.into_iter().map(trigger_from_record).collect()
    }

    pub async fn enable_trigger(
        &self,
        id: &TriggerId,
        by: OperatorId,
    ) -> Result<TriggerEvent, TriggerError> {
        let trigger = self.require_trigger(id).await?;
        let at = now_ms();
        let event =
            cairn_domain::RuntimeEvent::TriggerEnabled(cairn_domain::events::TriggerEnabled {
                project: trigger.project.clone(),
                trigger_id: id.clone(),
                by: by.clone(),
                at,
            });
        self.append_as_operator(by.clone(), event).await?;
        Ok(TriggerEvent::TriggerEnabled {
            trigger_id: id.clone(),
            by,
            at,
        })
    }

    pub async fn disable_trigger(
        &self,
        id: &TriggerId,
        by: OperatorId,
        reason: Option<String>,
    ) -> Result<TriggerEvent, TriggerError> {
        let trigger = self.require_trigger(id).await?;
        let at = now_ms();
        let event =
            cairn_domain::RuntimeEvent::TriggerDisabled(cairn_domain::events::TriggerDisabled {
                project: trigger.project.clone(),
                trigger_id: id.clone(),
                by: by.clone(),
                reason: reason.clone(),
                at,
            });
        self.append_as_operator(by.clone(), event).await?;
        Ok(TriggerEvent::TriggerDisabled {
            trigger_id: id.clone(),
            by,
            reason,
            at,
        })
    }

    /// Resume a suspended trigger. `by` is the operator attribution for
    /// the resulting `TriggerResumed` event — cairn-app's
    /// `resume_trigger_handler` passes the authenticated principal so
    /// the audit trail reflects who pressed the resume button (PR #569
    /// Copilot review; earlier draft used `EventSource::Runtime` which
    /// lost the attribution). System-initiated resumes (e.g. automated
    /// budget-window recovery) can still pass a synthetic "system"
    /// operator id.
    pub async fn resume_trigger(
        &self,
        id: &TriggerId,
        by: OperatorId,
    ) -> Result<TriggerEvent, TriggerError> {
        let trigger = self.require_trigger(id).await?;
        if !matches!(trigger.state, TriggerState::Suspended { .. }) {
            return Err(TriggerError::NotSuspended(id.clone()));
        }
        let at = now_ms();
        let event =
            cairn_domain::RuntimeEvent::TriggerResumed(cairn_domain::events::TriggerResumed {
                project: trigger.project,
                trigger_id: id.clone(),
                at,
            });
        self.append_as_operator(by, event).await?;
        Ok(TriggerEvent::TriggerResumed {
            trigger_id: id.clone(),
            at,
        })
    }

    pub async fn delete_trigger(
        &self,
        id: &TriggerId,
        by: OperatorId,
    ) -> Result<TriggerEvent, TriggerError> {
        let trigger = self.require_trigger(id).await?;
        let at = now_ms();
        let event =
            cairn_domain::RuntimeEvent::TriggerDeleted(cairn_domain::events::TriggerDeleted {
                project: trigger.project,
                trigger_id: id.clone(),
                by: by.clone(),
                at,
            });
        self.append_as_operator(by.clone(), event).await?;
        Ok(TriggerEvent::TriggerDeleted {
            trigger_id: id.clone(),
            by,
            at,
        })
    }

    async fn require_trigger(&self, id: &TriggerId) -> Result<Trigger, TriggerError> {
        match cairn_store::projections::TriggerReadModel::get_trigger(self.store.as_ref(), id)
            .await?
        {
            Some(rec) => trigger_from_record(rec),
            None => Err(TriggerError::TriggerNotFound(id.clone())),
        }
    }

    // ── Pre-decision status + fire evaluation ────────────────────────

    async fn pre_decision_status(
        &self,
        trigger: &Trigger,
        signal_id: &SignalId,
        payload: &serde_json::Value,
        source_run_chain_depth: Option<u8>,
        now: u64,
    ) -> Result<PreDecision, TriggerError> {
        // 1. Duplicate-fire ledger: if this (trigger_id, signal_id) ever
        //    produced a 'fired' row, skip — the run was (or will be)
        //    created on the first attempt.
        if cairn_store::projections::TriggerFireReadModel::has_fired(
            self.store.as_ref(),
            &trigger.id,
            signal_id.as_str(),
        )
        .await?
        {
            return Ok(PreDecision::Skipped(SkipReason::AlreadyFired));
        }

        // 2. Condition DSL match.
        if !evaluate_conditions(&trigger.conditions, payload) {
            return Ok(PreDecision::Skipped(SkipReason::ConditionMismatch));
        }

        // 3. Chain-depth guard.
        let next_depth = source_run_chain_depth.map_or(1u8, |d| d.saturating_add(1));
        if next_depth > trigger.max_chain_depth {
            return Ok(PreDecision::Skipped(SkipReason::ChainTooDeep));
        }

        // 4. Per-trigger per-minute rate limit.
        let window_start = now.saturating_sub(TRIGGER_RATE_LIMIT_WINDOW_MS);
        let fires_in_window = cairn_store::projections::TriggerFireReadModel::count_fires_since(
            self.store.as_ref(),
            &trigger.id,
            window_start,
        )
        .await?;
        if fires_in_window >= trigger.rate_limit.max_per_minute {
            return Ok(PreDecision::RateLimited {
                bucket_capacity: trigger.rate_limit.max_per_minute,
            });
        }

        // 5. Per-project per-hour budget.
        let hour_ago = now.saturating_sub(PROJECT_BUDGET_WINDOW_MS);
        let project_fires =
            cairn_store::projections::TriggerFireReadModel::count_project_fires_since(
                self.store.as_ref(),
                &trigger.project,
                hour_ago,
            )
            .await?;
        if project_fires >= self.default_project_budget {
            return Ok(PreDecision::BudgetExceeded);
        }

        // 6. Required-fields on the template.
        let template = match cairn_store::projections::RunTemplateReadModel::get_template(
            self.store.as_ref(),
            &trigger.run_template_id,
        )
        .await?
        {
            Some(rec) => rec,
            // Trigger referencing a deleted template — data corruption
            // (delete_template is supposed to block on referencing
            // triggers). Warn loudly so the operator sees the situation
            // in logs rather than only through a silent skip. Surface
            // as Store error so the caller's decision log records the
            // real failure mode (Copilot review PR #569). The fire
            // attempt is aborted before any run is materialised.
            None => {
                tracing::warn!(
                    trigger_id = %trigger.id,
                    run_template_id = %trigger.run_template_id,
                    "trigger evaluation: run template missing; skipping fire (data corruption — delete_template is supposed to block on referencing triggers)"
                );
                return Err(TriggerError::TemplateNotFound(
                    trigger.run_template_id.clone(),
                ));
            }
        };
        // Parse required_fields_json; surface a corrupted row as a
        // Store error rather than silently treating it as "no required
        // fields" (which would disable the validation until the next
        // template write). PR #569 Copilot review.
        let required_fields: Vec<String> = serde_json::from_str(&template.required_fields_json)
            .map_err(|e| {
                TriggerError::Store(format!(
                    "template {} required_fields_json parse error: {e}",
                    template.template_id
                ))
            })?;
        if let Some(field) = required_fields
            .iter()
            .find(|field| resolve_path(payload, field).is_none())
        {
            return Ok(PreDecision::Skipped(SkipReason::MissingRequiredField {
                field: field.clone(),
            }));
        }

        Ok(PreDecision::Ready)
    }

    /// Preview which triggers are eligible for decision-layer evaluation for
    /// a signal. Runs the pre-decision checks without mutating any state so
    /// callers can consult an async decision service, then call
    /// `evaluate_signal_for_candidates` with the resulting outcomes.
    pub async fn decision_candidates_for_signal(
        &self,
        project: &ProjectKey,
        signal_id: &SignalId,
        signal_type: &str,
        plugin_id: &str,
        payload: &serde_json::Value,
        source_run_chain_depth: Option<u8>,
    ) -> Result<Vec<TriggerId>, TriggerError> {
        let now = now_ms();
        let matching = cairn_store::projections::TriggerReadModel::list_matching_enabled(
            self.store.as_ref(),
            project,
            signal_type,
            plugin_id,
        )
        .await?;

        let mut ready = Vec::new();
        for record in matching {
            let trigger = trigger_from_record(record)?;
            if matches!(
                self.pre_decision_status(&trigger, signal_id, payload, source_run_chain_depth, now)
                    .await?,
                PreDecision::Ready
            ) {
                ready.push(trigger.id);
            }
        }
        Ok(ready)
    }

    /// Evaluate a signal against the pre-selected trigger ids, applying the
    /// provided decision outcomes. Emits the full `TriggerEvent` list (fired,
    /// skipped, denied, rate-limited, pending-approval, possibly
    /// suspended-for-budget). Each emitted durable `RuntimeEvent` is
    /// persisted by the caller via `runtime_event_for_trigger_service_event`.
    ///
    /// The prepared_trigger_ids set is how the caller ensures a trigger
    /// created/enabled between the preview and this call does not sneak in
    /// without a matching decision outcome.
    pub async fn evaluate_signal_for_candidates(
        &self,
        project: &ProjectKey,
        signal_id: &SignalId,
        signal_type: &str,
        plugin_id: &str,
        payload: &serde_json::Value,
        source_run_chain_depth: Option<u8>,
        prepared_trigger_ids: &HashSet<TriggerId>,
        decision_fn: &(dyn Fn(&TriggerId, &str) -> TriggerDecisionOutcome + Send + Sync),
    ) -> Result<Vec<TriggerEvent>, TriggerError> {
        let matching = cairn_store::projections::TriggerReadModel::list_matching_enabled(
            self.store.as_ref(),
            project,
            signal_type,
            plugin_id,
        )
        .await?;
        self.evaluate_with_matching(
            signal_id,
            signal_type,
            payload,
            source_run_chain_depth,
            prepared_trigger_ids,
            decision_fn,
            matching,
        )
        .await
    }

    /// Private core of the signal evaluation loop. Takes the matching
    /// set as an argument so callers that already have it (e.g.
    /// `evaluate_signal`) don't issue a second
    /// `list_matching_enabled` read (Copilot review PR #569).
    #[allow(clippy::too_many_arguments)]
    async fn evaluate_with_matching(
        &self,
        signal_id: &SignalId,
        signal_type: &str,
        payload: &serde_json::Value,
        source_run_chain_depth: Option<u8>,
        prepared_trigger_ids: &HashSet<TriggerId>,
        decision_fn: &(dyn Fn(&TriggerId, &str) -> TriggerDecisionOutcome + Send + Sync),
        matching: Vec<cairn_store::projections::TriggerRecord>,
    ) -> Result<Vec<TriggerEvent>, TriggerError> {
        let now = now_ms();
        let mut events = Vec::new();

        for record in matching {
            let trigger = trigger_from_record(record)?;
            if !prepared_trigger_ids.contains(&trigger.id) {
                continue;
            }
            match self
                .pre_decision_status(&trigger, signal_id, payload, source_run_chain_depth, now)
                .await?
            {
                PreDecision::Ready => {}
                PreDecision::Skipped(reason) => {
                    events.push(TriggerEvent::TriggerSkipped {
                        trigger_id: trigger.id.clone(),
                        signal_id: signal_id.clone(),
                        reason,
                        skipped_at: now,
                    });
                    continue;
                }
                PreDecision::RateLimited { bucket_capacity } => {
                    events.push(TriggerEvent::TriggerRateLimited {
                        trigger_id: trigger.id.clone(),
                        signal_id: signal_id.clone(),
                        bucket_remaining: 0,
                        bucket_capacity,
                        rate_limited_at: now,
                    });
                    continue;
                }
                PreDecision::BudgetExceeded => {
                    events.push(TriggerEvent::TriggerSuspended {
                        trigger_id: trigger.id.clone(),
                        reason: SuspensionReason::BudgetExceeded,
                        at: now,
                    });
                    continue;
                }
            }

            let next_depth = source_run_chain_depth.map_or(1u8, |d| d.saturating_add(1));
            let decision_outcome = (decision_fn)(&trigger.id, signal_type);

            match &decision_outcome {
                TriggerDecisionOutcome::Approved { .. } => {
                    // Approved — proceed to fire.
                }
                TriggerDecisionOutcome::Denied {
                    decision_id,
                    reason,
                } => {
                    events.push(TriggerEvent::TriggerDenied {
                        trigger_id: trigger.id.clone(),
                        signal_id: signal_id.clone(),
                        decision_id: decision_id.clone(),
                        reason: reason.clone(),
                        denied_at: now,
                    });
                    continue;
                }
                TriggerDecisionOutcome::PendingApproval { approval_id } => {
                    events.push(TriggerEvent::TriggerPendingApproval {
                        trigger_id: trigger.id.clone(),
                        signal_id: signal_id.clone(),
                        approval_id: approval_id.clone(),
                        pending_at: now,
                    });
                    continue;
                }
            }

            let run_id = RunId::new(format!("run_trigger_{}_{}", trigger.id.as_str(), now));
            events.push(TriggerEvent::TriggerFired {
                trigger_id: trigger.id.clone(),
                signal_id: signal_id.clone(),
                signal_type: signal_type.to_owned(),
                run_id,
                chain_depth: next_depth,
                fired_at: now,
            });
        }

        Ok(events)
    }

    /// Convenience: preview + evaluate in one call. Used by callers that
    /// auto-approve every fire (tests + legacy callers that don't integrate
    /// with the decision layer).
    pub async fn evaluate_signal(
        &self,
        project: &ProjectKey,
        signal_id: &SignalId,
        signal_type: &str,
        plugin_id: &str,
        payload: &serde_json::Value,
        source_run_chain_depth: Option<u8>,
        decision_fn: &(dyn Fn(&TriggerId, &str) -> TriggerDecisionOutcome + Send + Sync),
    ) -> Result<Vec<TriggerEvent>, TriggerError> {
        // Review PR #569: `evaluate_signal` is a convenience that
        // auto-approves every fire. Fetch the matching set exactly
        // once and hand it to `evaluate_with_matching` — there's no
        // second `list_matching_enabled` round-trip on the hot path.
        let matching = cairn_store::projections::TriggerReadModel::list_matching_enabled(
            self.store.as_ref(),
            project,
            signal_type,
            plugin_id,
        )
        .await?;
        let prepared: HashSet<TriggerId> = matching.iter().map(|t| t.trigger_id.clone()).collect();
        self.evaluate_with_matching(
            signal_id,
            signal_type,
            payload,
            source_run_chain_depth,
            &prepared,
            decision_fn,
            matching,
        )
        .await
    }
}

// ── Record → domain conversions ─────────────────────────────────────────────

fn trigger_from_record(
    rec: cairn_store::projections::TriggerRecord,
) -> Result<Trigger, TriggerError> {
    let conditions: Vec<TriggerCondition> =
        serde_json::from_str(&rec.conditions_json).map_err(|e| {
            TriggerError::Store(format!(
                "trigger {} conditions_json parse error: {e}",
                rec.trigger_id
            ))
        })?;
    let state = match rec.state {
        cairn_store::projections::TriggerStateKind::Enabled => TriggerState::Enabled,
        cairn_store::projections::TriggerStateKind::Disabled => TriggerState::Disabled {
            reason: rec.state_reason.clone(),
            since: rec.state_since.unwrap_or(0),
        },
        cairn_store::projections::TriggerStateKind::Suspended => {
            let reason = match rec.suspension_reason.as_deref() {
                Some("rate_limit_exceeded") => SuspensionReason::RateLimitExceeded,
                Some("budget_exceeded") => SuspensionReason::BudgetExceeded,
                Some("operator_paused") => SuspensionReason::OperatorPaused,
                // RepeatedFailures carries a failure_count on the wire, but
                // the projection row only keeps the discriminant string.
                // Rehydrate with 0; the original count is still in the
                // event log if anyone needs it for forensics.
                Some("repeated_failures") => {
                    SuspensionReason::RepeatedFailures { failure_count: 0 }
                }
                _ => SuspensionReason::OperatorPaused,
            };
            TriggerState::Suspended {
                reason,
                since: rec.state_since.unwrap_or(0),
            }
        }
    };
    Ok(Trigger {
        id: rec.trigger_id,
        project: rec.project,
        name: rec.name,
        description: rec.description,
        signal_pattern: SignalPattern {
            signal_type: rec.signal_type,
            plugin_id: rec.plugin_id,
        },
        conditions,
        run_template_id: rec.run_template_id,
        state,
        rate_limit: RateLimitConfig {
            max_per_minute: rec.max_per_minute,
            max_burst: rec.max_burst,
        },
        max_chain_depth: rec.max_chain_depth,
        created_by: rec.created_by,
        created_at: rec.created_at,
        updated_at: rec.updated_at,
    })
}

fn run_template_from_record(
    rec: cairn_store::projections::RunTemplateRecord,
) -> Result<RunTemplate, TriggerError> {
    // Allowlists + required_fields + default_mode all round-trip
    // through serde_json. If the projection row is corrupted (or a
    // future RunMode variant is introduced that this binary can't
    // deserialize), surface as `TriggerError::Store` rather than
    // silently defaulting — a wrong RunMode would change which
    // orchestrator picks up the triggered run (PR #569 Copilot review).
    let plugin_allowlist: Option<Vec<String>> = rec
        .plugin_allowlist_json
        .as_deref()
        .map(serde_json::from_str)
        .transpose()
        .map_err(|e| {
            TriggerError::Store(format!(
                "template {} plugin_allowlist_json parse error: {e}",
                rec.template_id
            ))
        })?;
    let tool_allowlist: Option<Vec<String>> = rec
        .tool_allowlist_json
        .as_deref()
        .map(serde_json::from_str)
        .transpose()
        .map_err(|e| {
            TriggerError::Store(format!(
                "template {} tool_allowlist_json parse error: {e}",
                rec.template_id
            ))
        })?;
    let required_fields: Vec<String> =
        serde_json::from_str(&rec.required_fields_json).map_err(|e| {
            TriggerError::Store(format!(
                "template {} required_fields_json parse error: {e}",
                rec.template_id
            ))
        })?;
    // RunMode is an internally-tagged enum (`#[serde(tag = "type")]`) so
    // its serialised form is JSON like `{"type":"direct"}`. Parse it
    // back as JSON rather than wrapping the raw string, matching how
    // `enum_to_str` writes it into the `default_mode` TEXT column.
    let default_mode: RunMode = serde_json::from_str(&rec.default_mode).map_err(|e| {
        TriggerError::Store(format!(
            "template {} default_mode `{}` parse error: {e}",
            rec.template_id, rec.default_mode
        ))
    })?;
    Ok(RunTemplate {
        id: rec.template_id,
        project: rec.project,
        name: rec.name,
        description: rec.description,
        default_mode,
        system_prompt: rec.system_prompt,
        initial_user_message: rec.initial_user_message,
        plugin_allowlist,
        tool_allowlist,
        budget: TemplateBudget {
            max_tokens: rec.budget_max_tokens,
            max_wall_clock_ms: rec.budget_max_wall_clock_ms,
            max_iterations: rec.budget_max_iterations,
            exploration_budget_share: rec.budget_exploration_budget_share,
        },
        sandbox_hint: rec.sandbox_hint,
        required_fields,
        created_by: rec.created_by,
        created_at: rec.created_at,
        updated_at: rec.updated_at,
    })
}

// ── Pure-logic unit tests ───────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // ── Condition DSL ─────────────────────────────────────────────────

    #[test]
    fn condition_equals_matches() {
        let payload = json!({"action": "labeled"});
        let cond = TriggerCondition::Equals {
            path: "action".into(),
            value: json!("labeled"),
        };
        assert!(evaluate_condition(&cond, &payload));
    }

    #[test]
    fn condition_equals_mismatches() {
        let payload = json!({"action": "opened"});
        let cond = TriggerCondition::Equals {
            path: "action".into(),
            value: json!("labeled"),
        };
        assert!(!evaluate_condition(&cond, &payload));
    }

    #[test]
    fn condition_contains_array() {
        let payload = json!({
            "labels": [{"name": "bug"}, {"name": "cairn-ready"}]
        });
        let cond = TriggerCondition::Contains {
            path: "labels[].name".into(),
            value: json!("cairn-ready"),
        };
        assert!(evaluate_condition(&cond, &payload));
    }

    #[test]
    fn condition_contains_array_no_match() {
        let payload = json!({
            "labels": [{"name": "bug"}, {"name": "enhancement"}]
        });
        let cond = TriggerCondition::Contains {
            path: "labels[].name".into(),
            value: json!("cairn-ready"),
        };
        assert!(!evaluate_condition(&cond, &payload));
    }

    #[test]
    fn condition_exists() {
        let payload = json!({"issue": {"number": 42}});
        assert!(evaluate_condition(
            &TriggerCondition::Exists {
                path: "issue.number".into()
            },
            &payload
        ));
        assert!(!evaluate_condition(
            &TriggerCondition::Exists {
                path: "issue.title".into()
            },
            &payload
        ));
    }

    #[test]
    fn condition_not() {
        let payload = json!({"action": "opened"});
        let cond = TriggerCondition::Not(Box::new(TriggerCondition::Equals {
            path: "action".into(),
            value: json!("labeled"),
        }));
        assert!(evaluate_condition(&cond, &payload));
    }

    #[test]
    fn condition_serializes_and_roundtrips_with_nested_not() {
        let cond = TriggerCondition::Not(Box::new(TriggerCondition::Contains {
            path: "labels[].name".into(),
            value: json!("cairn-ready"),
        }));

        let json = serde_json::to_value(&cond).expect("trigger condition should serialize");
        assert_eq!(json["type"], json!("not"));
        assert_eq!(json["condition"]["type"], json!("contains"));

        let restored: TriggerCondition =
            serde_json::from_value(json).expect("trigger condition should deserialize");
        assert_eq!(restored, cond);
    }

    // ── Variable substitution ─────────────────────────────────────────

    #[test]
    fn substitution_replaces_scalars() {
        let payload = json!({
            "action": "labeled",
            "issue": {"number": 42, "title": "Fix login bug"},
            "repository": {"full_name": "org/dogfood"}
        });

        let template = "Issue #{{issue.number}} in {{repository.full_name}}: {{issue.title}}";
        let result = substitute_variables(template, &payload, &[]).unwrap();
        assert_eq!(result, "Issue #42 in org/dogfood: Fix login bug");
    }

    #[test]
    fn substitution_replaces_arrays() {
        let payload = json!({
            "issue": {
                "labels": [{"name": "bug"}, {"name": "cairn-ready"}]
            }
        });

        let template = "Labels: {{issue.labels[].name}}";
        let result = substitute_variables(template, &payload, &[]).unwrap();
        assert_eq!(result, "Labels: bug, cairn-ready");
    }

    #[test]
    fn substitution_missing_field_empty_string() {
        let payload = json!({"action": "labeled"});
        let result = substitute_variables("Value: {{nonexistent}}", &payload, &[]).unwrap();
        assert_eq!(result, "Value: ");
    }

    #[test]
    fn substitution_required_field_missing_errors() {
        let payload = json!({"action": "labeled"});
        let result =
            substitute_variables("{{issue.number}}", &payload, &["issue.number".to_string()]);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), vec!["issue.number".to_string()]);
    }
}
