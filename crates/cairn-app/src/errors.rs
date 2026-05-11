//! HTTP error response helpers and utility functions.

use axum::extract::rejection::JsonRejection;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use cairn_api::bootstrap::{BootstrapConfig, DeploymentMode, StorageBackend};
use cairn_api::http::ApiError;
use cairn_domain::tool_invocation::ToolInvocationState;
use cairn_domain::{
    DefaultFeatureGate, Entitlement, EntitlementSet, EventEnvelope, EventId, EventSource,
    FeatureGate, FeatureGateResult, OperatorId, ProductTier, ProjectId, RunState, RuntimeEvent,
    SessionState, TaskState, TenantId,
};
use cairn_evals::EvalRunService as ProductEvalRunService;
use uuid::Uuid;

#[derive(Clone, Debug)]
pub struct AppApiError {
    pub(crate) status: StatusCode,
    pub(crate) error: ApiError,
}

impl AppApiError {
    /// Build a canonical error envelope.
    ///
    /// Preferred constructor for every HTTP error response — goes
    /// through `runtime_error_response` / `store_error_response` for
    /// typed errors, or directly for hand-rolled validation messages.
    /// The resulting body shape is the canonical
    /// `{status_code, code, message, request_id}`.
    ///
    /// New handlers should never drift into hand-rolled
    /// `json!({"error": ..})`. Any remaining non-canonical sites are
    /// tracked in the api-design audit queue and should be migrated
    /// when touched.
    ///
    /// `pub` (not `pub(crate)`) so integration tests in
    /// `crates/cairn-app/tests/*.rs` can construct the same envelope
    /// they assert against without round-tripping through a full HTTP
    /// handler for every site.
    pub fn new(status: StatusCode, code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            status,
            error: ApiError {
                status_code: status.as_u16(),
                code: code.into(),
                message: message.into(),
                request_id: None,
            },
        }
    }
}

impl IntoResponse for AppApiError {
    fn into_response(self) -> Response {
        (self.status, Json(self.error)).into_response()
    }
}

pub(crate) fn unauthorized_api_error() -> AppApiError {
    AppApiError::new(StatusCode::UNAUTHORIZED, "unauthorized", "unauthorized")
}

pub(crate) fn tenant_scope_mismatch_error() -> AppApiError {
    AppApiError::new(
        StatusCode::FORBIDDEN,
        "tenant_scope_mismatch",
        "requested project does not belong to authenticated tenant",
    )
}

pub(crate) fn query_rejection_error(message: impl Into<String>) -> AppApiError {
    AppApiError::new(
        StatusCode::UNPROCESSABLE_ENTITY,
        "validation_error",
        message,
    )
}

pub(crate) fn forbidden_api_error(message: impl Into<String>) -> AppApiError {
    AppApiError::new(StatusCode::FORBIDDEN, "forbidden", message)
}

/// RFC 026 PR-A0: structured 403 body emitted when `TenantAdminGuard`
/// rejects a real operator principal (not god-token) that has no
/// `operator_tenant_roles` entry for the target tenant.
///
/// Carries the actionable hint so operators upgrading from pre-PR-A0
/// main can see the remediation path instead of a bare `forbidden`.
/// The body intentionally diverges from the canonical envelope — this
/// is what the UI `<AdminGate>` wrapper parses to distinguish
/// "regression upgrade, backfill missing" from "genuinely not admin."
pub(crate) fn tenant_role_missing_response(tenant_id: &str, operator_id: &str) -> Response {
    let body = serde_json::json!({
        "error_code": "tenant_role_missing",
        "tenant_id": tenant_id,
        "operator_id": operator_id,
        "hint": "Ask your deployment admin to run `cairn-app admin promote <op> --tenant <T> --role Admin` or set CAIRN_ADMIN_TOKEN and POST /v1/admin/operators/:id/tenant-roles/:tenant/promote",
    });
    (StatusCode::FORBIDDEN, Json(body)).into_response()
}

/// 422 Unprocessable Entity with the canonical `validation_error` code.
///
/// Use for requests that parse (syntactically valid JSON) but fail
/// business-rule validation — missing required fields, unknown enum
/// variants, values outside allowed ranges. Audit #483 removed the
/// `bad_request_response` alias which returned the same 422 + name,
/// naming-drift that misled callers into expecting 400. For genuinely
/// malformed requests that should surface as 400 Bad Request, use the
/// rejection-path helper `json_rejection_response` which honours the
/// axum extractor's native status.
pub(crate) fn validation_error_response(message: impl Into<String>) -> Response {
    AppApiError::new(
        StatusCode::UNPROCESSABLE_ENTITY,
        "validation_error",
        message,
    )
    .into_response()
}

/// #486: canonical 404 `run not found` envelope, extracted from 18+
/// hand-rolled call sites across the runs handler and its siblings.
/// Single choke point so a future rename of the \`not_found\` code or
/// the message text is a 1-file change.
pub(crate) fn run_not_found_response() -> Response {
    AppApiError::new(StatusCode::NOT_FOUND, "not_found", "run not found").into_response()
}

pub(crate) fn memory_api_error_response(err: String) -> Response {
    if err.starts_with("memory not found:") {
        return AppApiError::new(StatusCode::NOT_FOUND, "not_found", err).into_response();
    }

    if err.starts_with("invalid memory status:") {
        return validation_error_response(err);
    }

    AppApiError::new(StatusCode::INTERNAL_SERVER_ERROR, "internal_error", err).into_response()
}

/// Map a `RuntimeError` to the canonical HTTP error envelope.
///
/// Routing table:
///   * `NotFound` → 404 `not_found`
///   * `Conflict`, `DependencyConflict`, `InvalidTransition` → 409
///   * `PolicyDenied` → 403 `permission_denied`
///   * `QuotaExceeded` → 429 `quota_exceeded`
///   * `LeaseExpired` → 409 `lease_expired` (closes #464 — previously 422)
///   * `Validation` → 422 `validation_error`
///   * `Store(e)` → delegates to [`store_error_response`]
///   * `Internal(_)` → 500 `internal_error` with redacted message (SEC-007)
///
/// `pub` so integration tests can assert the mapping directly without
/// re-deriving it through a full handler flow.
pub fn runtime_error_response(err: cairn_runtime::RuntimeError) -> axum::response::Response {
    match err {
        cairn_runtime::RuntimeError::NotFound { .. } => {
            AppApiError::new(StatusCode::NOT_FOUND, "not_found", err.to_string()).into_response()
        }
        cairn_runtime::RuntimeError::Conflict { .. } => {
            AppApiError::new(StatusCode::CONFLICT, "conflict", err.to_string()).into_response()
        }
        cairn_runtime::RuntimeError::DependencyConflict { .. } => {
            AppApiError::new(StatusCode::CONFLICT, "dependency_conflict", err.to_string())
                .into_response()
        }
        cairn_runtime::RuntimeError::PolicyDenied { .. } => {
            AppApiError::new(StatusCode::FORBIDDEN, "permission_denied", err.to_string())
                .into_response()
        }
        cairn_runtime::RuntimeError::QuotaExceeded { .. } => AppApiError::new(
            StatusCode::TOO_MANY_REQUESTS,
            "quota_exceeded",
            err.to_string(),
        )
        .into_response(),
        // Invalid state transitions (e.g. pause on a non-running run,
        // resolve an already-decided approval, activate a draft prompt
        // release) are 409 Conflict, not 422: the request is well-formed
        // syntactically but the resource is in a state that cannot accept
        // the operation. Closes #216 — previously `pause` on a pending
        // run landed in a "fabric layer error" 500 because the FF
        // suspend-rejection codes were shadowed by `RuntimeError::Internal`;
        // they now round-trip as `InvalidTransition` and surface as 409.
        cairn_runtime::RuntimeError::InvalidTransition { .. } => AppApiError::new(
            StatusCode::CONFLICT,
            "invalid_state_transition",
            err.to_string(),
        )
        .into_response(),
        // Closes #464: LeaseExpired is 409 Conflict, not 422. The request
        // is syntactically and semantically well-formed; the resource is
        // in a state where the lease token can no longer be accepted.
        // Retry semantics differ: clients that retry on 422 ("please fix
        // your JSON") would loop forever; clients that retry on 409
        // re-claim the lease correctly.
        cairn_runtime::RuntimeError::LeaseExpired { .. } => {
            AppApiError::new(StatusCode::CONFLICT, "lease_expired", err.to_string()).into_response()
        }
        cairn_runtime::RuntimeError::Validation { .. } => {
            validation_error_response(err.to_string())
        }
        // #353: provider connection referenced for generation but with
        // no bound credential. Returns 422 (not 503) so callers that
        // retry-on-5xx do not spin — this is a configuration fix, not
        // an upstream outage. The `provider_credential_missing` code
        // names the remediation path (link a credential) distinct from
        // `provider_auth_failed` (rotate an existing credential).
        cairn_runtime::RuntimeError::CredentialMissing { .. } => AppApiError::new(
            StatusCode::UNPROCESSABLE_ENTITY,
            "provider_credential_missing",
            err.to_string(),
        )
        .into_response(),
        cairn_runtime::RuntimeError::Store(store_err) => store_error_response(store_err),
        // SEC-007 (#419): `RuntimeError::Internal(msg)` carries free-form
        // runtime detail that can include internal paths, IDs, or
        // third-party error fragments. Log server-side so operators can
        // correlate via `x-request-id`, and return a generic static
        // message to the caller. Closes the symmetric leak called out
        // alongside `store_error_response` in the audit finding.
        //
        // Bind `msg` explicitly so the tracing line carries the exact
        // internal string rather than `Display` which wraps it with
        // "internal runtime error: ". Makes grep-by-root-cause easier
        // for operators.
        cairn_runtime::RuntimeError::Internal(msg) => {
            tracing::error!(detail = %msg, "runtime internal error");
            AppApiError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal_error",
                "internal runtime error",
            )
            .into_response()
        }
    }
}

/// Map a `StoreError` to the canonical HTTP error envelope.
///
/// SEC-007 (#419): `Connection`, `Migration`, `Serialization`, and
/// `Internal` arms redact the raw driver string (host:port, SQL
/// fragments, credential-adjacent data). The full chain is logged at
/// `error` level so operators can correlate via `x-request-id`.
///
/// `pub` so integration tests can assert the redaction guarantee
/// directly.
pub fn store_error_response(err: cairn_store::StoreError) -> Response {
    match err {
        cairn_store::StoreError::NotFound { .. } => {
            AppApiError::new(StatusCode::NOT_FOUND, "not_found", err.to_string()).into_response()
        }
        cairn_store::StoreError::Conflict { .. } => {
            AppApiError::new(StatusCode::CONFLICT, "conflict", err.to_string()).into_response()
        }
        // SEC-007 (#419): driver/serde error strings can carry host:port,
        // role names, column names, SQL fragments, and depending on the
        // error, raw row data. Never forward these to the client. Log at
        // `error` level with the full detail so operators can correlate
        // via `x-request-id`, then return a stable generic message. This
        // matches the pattern used at evals.rs:475.
        //
        // Each arm binds the inner `String` explicitly so the tracing
        // line carries the exact driver detail (not the `Display`
        // wrapper prefix like "connection error: ").
        cairn_store::StoreError::Connection(detail) => {
            tracing::error!(%detail, "store connection error");
            AppApiError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal_error",
                "store temporarily unavailable",
            )
            .into_response()
        }
        cairn_store::StoreError::Migration(detail) => {
            tracing::error!(%detail, "store migration error");
            AppApiError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal_error",
                "internal store error",
            )
            .into_response()
        }
        cairn_store::StoreError::Serialization(detail) => {
            tracing::error!(%detail, "store serialization error");
            AppApiError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal_error",
                "internal store error",
            )
            .into_response()
        }
        cairn_store::StoreError::Internal(detail) => {
            tracing::error!(%detail, "store internal error");
            AppApiError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal_error",
                "internal store error",
            )
            .into_response()
        }
    }
}

/// Build a canonical error response with an additional `details` sidecar.
///
/// The top-level body keeps the canonical envelope shape
/// (`status_code`, `code`, `message`, `request_id`) so SDK parsers that
/// key on those fields continue to work. Extra structured context
/// (e.g. per-partition failure breakdown for a rotation operation, or
/// per-attempt provider diagnostics on `all_providers_exhausted`) is
/// emitted alongside under `details` rather than replacing the envelope.
///
/// Use sparingly — prefer `AppApiError::new(..).into_response()` when
/// the error can be fully described by `code` + `message`.
pub fn api_error_with_details(
    status: StatusCode,
    code: impl Into<String>,
    message: impl Into<String>,
    details: serde_json::Value,
) -> Response {
    // Build the base envelope by serializing the canonical `ApiError`
    // struct — ensures the shape never drifts from AppApiError/ApiError
    // even if fields are added/renamed. Per Copilot review on PR #540.
    let envelope = ApiError {
        status_code: status.as_u16(),
        code: code.into(),
        message: message.into(),
        request_id: None,
    };
    let mut body = match serde_json::to_value(&envelope) {
        Ok(serde_json::Value::Object(map)) => map,
        // serde derive on `ApiError` cannot produce a non-object value
        // and cannot fail, but keep a defensive fallback so this
        // helper never panics in production. The fallback carries the
        // minimum canonical shape so clients still parse it.
        _ => {
            let mut fallback = serde_json::Map::new();
            fallback.insert(
                "status_code".to_owned(),
                serde_json::Value::from(status.as_u16()),
            );
            fallback.insert(
                "code".to_owned(),
                serde_json::Value::String("internal_error".to_owned()),
            );
            fallback.insert(
                "message".to_owned(),
                serde_json::Value::String("failed to serialize api error".to_owned()),
            );
            fallback.insert("request_id".to_owned(), serde_json::Value::Null);
            fallback
        }
    };

    // Coerce non-object `details` to `{ "value": <original> }` so the
    // OpenAPI schema contract (`details: type: object`) is never
    // violated. Caller-supplied arrays/scalars/strings get wrapped;
    // objects pass through unchanged.
    let details = match details {
        serde_json::Value::Object(_) => details,
        other => {
            let mut wrapped = serde_json::Map::new();
            wrapped.insert("value".to_owned(), other);
            serde_json::Value::Object(wrapped)
        }
    };
    body.insert("details".to_owned(), details);

    (status, Json(serde_json::Value::Object(body))).into_response()
}

pub(crate) fn json_rejection_response(err: JsonRejection) -> Response {
    // Honour the rejection's native status — `JsonRejection::BytesRejection
    // → FailedToBufferBody::LengthLimitError` returns 413 PAYLOAD_TOO_LARGE
    // when a per-route `DefaultBodyLimit` rejects the body, and we must
    // preserve that so the client gets the right signal instead of a 422.
    // Other variants (JsonDataError / JsonSyntaxError / MissingJsonContentType)
    // all map to 4xx codes that are fine to surface verbatim. The response
    // body is still wrapped in the usual `AppApiError` shape so the error
    // envelope matches the rest of the admin surface. Copilot review on
    // PR #548, #493.
    let status = err.status();
    let code = if status == StatusCode::PAYLOAD_TOO_LARGE {
        "payload_too_large"
    } else {
        "validation_error"
    };
    AppApiError::new(status, code, err.body_text()).into_response()
}

pub(crate) fn parse_run_state(value: &str) -> Result<RunState, String> {
    serde_json::from_value::<RunState>(serde_json::Value::String(value.to_owned()))
        .map_err(|_| format!("invalid run status: {value}"))
}

pub(crate) fn parse_session_state(value: &str) -> Result<SessionState, String> {
    serde_json::from_value::<SessionState>(serde_json::Value::String(value.to_owned()))
        .map_err(|_| format!("invalid session status: {value}"))
}

pub(crate) fn parse_task_state(value: &str) -> Result<TaskState, String> {
    serde_json::from_value::<TaskState>(serde_json::Value::String(value.to_owned()))
        .map_err(|_| format!("invalid task state: {value}"))
}

pub(crate) fn parse_eval_subject_kind(
    value: &str,
) -> Result<cairn_domain::EvalSubjectKind, String> {
    serde_json::from_value::<cairn_domain::EvalSubjectKind>(serde_json::Value::String(
        value.to_owned(),
    ))
    .map_err(|_| format!("invalid eval subject_kind: {value}"))
}

pub(crate) fn parse_tool_invocation_state(value: &str) -> Result<ToolInvocationState, String> {
    serde_json::from_value::<ToolInvocationState>(serde_json::Value::String(value.to_owned()))
        .map_err(|_| format!("invalid tool invocation state: {value}"))
}

pub(crate) fn latest_eval_score_for_release(
    evals: &ProductEvalRunService,
    release: &cairn_store::projections::PromptReleaseRecord,
) -> Option<f64> {
    let mut runs = evals
        .list_by_project(&ProjectId::new(release.project.project_id.as_str()))
        .into_iter()
        .filter(|run| run.prompt_release_id.as_ref() == Some(&release.prompt_release_id))
        .collect::<Vec<_>>();
    runs.sort_by_key(|run| run.completed_at.unwrap_or(run.created_at));
    runs.into_iter()
        .rev()
        .find_map(|run| run.metrics.task_success_rate)
}

pub(crate) fn deployment_mode_tier(mode: DeploymentMode) -> ProductTier {
    match mode {
        DeploymentMode::Local => ProductTier::LocalEval,
        DeploymentMode::SelfHostedTeam => ProductTier::TeamSelfHosted,
    }
}

/// Build the active EntitlementSet for the current deployment config.
/// Self-hosted team mode gets DeploymentTier by default. Local in-memory dev
/// runs also get DeploymentTier when credentials are available so operator
/// flows can exercise credential management without a paid license.
pub(crate) fn local_dev_deployment_entitlements(config: &BootstrapConfig) -> bool {
    matches!(config.mode, DeploymentMode::Local)
        && matches!(config.storage, StorageBackend::InMemory)
        && config.credentials_available()
}

pub(crate) fn app_entitlements(config: &BootstrapConfig) -> EntitlementSet {
    let tier = deployment_mode_tier(config.mode);
    let base = EntitlementSet::new(TenantId::new("bootstrap"), tier);
    if local_dev_deployment_entitlements(config) {
        return base.with_entitlement(Entitlement::DeploymentTier);
    }
    match config.mode {
        DeploymentMode::SelfHostedTeam => base.with_entitlement(Entitlement::DeploymentTier),
        DeploymentMode::Local => base,
    }
}

/// Check a feature gate, returning a 403 response if the feature is not allowed.
///
/// Returns the canonical error envelope (`status_code`, `code`,
/// `message`, `request_id`) — matches the rest of the error surface
/// so UI parsers can key on `code` / `message` uniformly.
pub(crate) fn require_feature(config: &BootstrapConfig, feature: &str) -> Option<Response> {
    let gate = DefaultFeatureGate::v1_defaults();
    match gate.check(&app_entitlements(config), feature) {
        FeatureGateResult::Allowed => None,
        FeatureGateResult::Denied { reason } | FeatureGateResult::Degraded { reason } => Some(
            AppApiError::new(StatusCode::FORBIDDEN, "entitlement_required", reason).into_response(),
        ),
    }
}

pub(crate) fn deployment_mode_label(mode: DeploymentMode) -> &'static str {
    match mode {
        DeploymentMode::Local => "local",
        DeploymentMode::SelfHostedTeam => "self_hosted_team",
    }
}

pub(crate) fn storage_backend_label(storage: &StorageBackend) -> &'static str {
    match storage {
        StorageBackend::InMemory => "memory",
        StorageBackend::Sqlite { .. } => "sqlite",
        StorageBackend::Postgres { .. } => "postgres",
    }
}

pub(crate) fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

pub(crate) fn operator_event_envelope(payload: RuntimeEvent) -> EventEnvelope<RuntimeEvent> {
    EventEnvelope::for_runtime_event(
        EventId::new(format!("evt_operator_{}", Uuid::new_v4())),
        EventSource::Operator {
            operator_id: OperatorId::new("operator_api"),
        },
        payload,
    )
}
