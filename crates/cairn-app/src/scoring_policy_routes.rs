//! RFC 029 PR-B2: HTTP handler for configuring a project's scoring policy.
//!
//! `PUT /v1/projects/:project/scoring-policy`
//!
//! Body: JSON-serialized `cairn_memory::retrieval::ScoringPolicy`.
//! Auth: any authenticated operator scoped to the target tenant.
//! Validation: weights on provider-required dimensions the resolved
//! provider declared `not_supported` are rejected with a 400 listing
//! the offending dimensions.
//!
//! Storage: the policy is persisted as a project-scope default under
//! the key `scoring_policy_json`. The default already flows through the
//! existing `DefaultSettingSet` event + projection, so the rescorer can
//! read it back via the standard defaults resolver in a follow-up (B2
//! +1 threading) — today the rescorer uses `ScoringPolicy::default()`
//! regardless of what's stored. Storing the policy now is the first
//! half of the contract; reading it is the next step.

use std::sync::Arc;

use axum::extract::{Extension, Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use serde::Serialize;

use cairn_api::auth::AuthPrincipal;
use cairn_memory::event_log_resolver::{snapshot_for_provider_ref, EventLogProviderResolver};
use cairn_memory::multi_provider::ProviderResolver;
use cairn_memory::retrieval::ScoringPolicy;
use cairn_memory::scoring_policy_validator::{
    validate_scoring_policy, ScoringPolicyValidationError,
};

use crate::extractors::enforce_project_tenant;
use crate::marketplace_routes::{operator_id_from_principal, project_key_from_path};
use crate::AppState;

#[derive(Serialize)]
pub struct ConfigureScoringPolicyResponse {
    pub project: String,
    pub provider_id: Option<String>,
    /// Effective policy persisted. Returned as-is so callers can
    /// confirm the on-disk shape matches what they sent.
    pub policy: ScoringPolicy,
}

#[derive(Serialize)]
struct ErrorBody {
    error: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    unsupported_dimensions: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    provider_id: Option<String>,
}

/// PUT /v1/projects/:project/scoring-policy
pub async fn configure_scoring_policy_handler(
    State(state): State<Arc<AppState>>,
    Extension(principal): Extension<AuthPrincipal>,
    Path(project_id): Path<String>,
    Json(policy): Json<ScoringPolicy>,
) -> impl IntoResponse {
    let _operator = operator_id_from_principal(&principal);

    let project = match project_key_from_path(&project_id) {
        Ok(p) => p,
        Err(msg) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorBody {
                    error: msg,
                    unsupported_dimensions: None,
                    provider_id: None,
                }),
            )
                .into_response();
        }
    };

    if !enforce_project_tenant(&principal, &project) {
        return crate::errors::tenant_scope_mismatch_error().into_response();
    }

    // Resolve the project's current provider snapshot to know which
    // dimensions are actually surfaced. When the resolver errors or
    // the snapshot is None (e.g. newly configured plugin that hasn't
    // handshaked yet), validation passes: the handler doesn't block
    // operator intent on a transient resolver failure.
    let resolver = EventLogProviderResolver::new(state.runtime.store.clone());
    let pref = resolver.resolve(&project).await.ok();
    let snapshot = pref
        .as_ref()
        .and_then(|p| snapshot_for_provider_ref(p).map(|s| (p.as_str().to_owned(), s)));

    if let Some((provider_id, snap)) = &snapshot {
        if let Err(ScoringPolicyValidationError::UnavailableDimensions {
            provider_id: _,
            unsupported,
        }) = validate_scoring_policy(
            &policy,
            Some(provider_id),
            Some(&snap.scoring_dimensions_surfaced),
        ) {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorBody {
                    error: format!(
                        "scoring policy references dimension(s) not surfaced by provider {provider_id}"
                    ),
                    unsupported_dimensions: Some(unsupported),
                    provider_id: Some(provider_id.clone()),
                }),
            )
                .into_response();
        }
    }

    // Storage: persist via the existing project-scope defaults surface
    // under key `scoring_policy_json`. Threading the stored policy
    // into the rescorer is a follow-up (depends on a batched
    // `DefaultsResolver` read that doesn't exist yet); shipping the
    // write path first validates the operator-facing contract.
    let policy_json = match serde_json::to_string(&policy) {
        Ok(j) => j,
        Err(e) => {
            // Internal detail stays in the structured log (operator
            // visibility); response body carries a generic message so
            // the client surface doesn't leak implementation details
            // (SEC-007).
            tracing::error!(error = %e, "failed to serialise scoring policy");
            return internal_error_response("internal error while persisting scoring policy");
        }
    };

    if let Err(e) = persist_policy_default(state.as_ref(), &project, &policy_json).await {
        tracing::error!(
            error = %e,
            tenant = project.tenant_id.as_str(),
            workspace = project.workspace_id.as_str(),
            project = project.project_id.as_str(),
            "failed to persist scoring policy default"
        );
        return internal_error_response("internal error while persisting scoring policy");
    }

    (
        StatusCode::OK,
        Json(ConfigureScoringPolicyResponse {
            project: format!(
                "{}/{}/{}",
                project.tenant_id.as_str(),
                project.workspace_id.as_str(),
                project.project_id.as_str()
            ),
            provider_id: snapshot.map(|(pid, _)| pid),
            policy,
        }),
    )
        .into_response()
}

/// Shared 500 response shape. Keeps the public body identical across
/// every 5xx branch (SEC-007: no implementation detail leakage) while
/// the structured error continues to land in the logs.
fn internal_error_response(message: &str) -> axum::response::Response {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(ErrorBody {
            error: message.to_owned(),
            unsupported_dimensions: None,
            provider_id: None,
        }),
    )
        .into_response()
}

/// Persist the policy JSON as a project-scope default under the key
/// `scoring_policy_json`. Uses the existing `DefaultsService` surface
/// so storage lives in the same event log + projection as every other
/// per-project default.
async fn persist_policy_default(
    state: &AppState,
    project: &cairn_domain::tenancy::ProjectKey,
    policy_json: &str,
) -> Result<(), String> {
    use cairn_domain::Scope;
    use cairn_runtime::services::defaults_impl::DefaultsServiceImpl;
    use cairn_runtime::DefaultsService;

    let svc = DefaultsServiceImpl::new(state.runtime.store.clone());
    // `scope_id` for a project-scope default is the canonical
    // `tenant/workspace/project` triple — the same shape the existing
    // project-scope defaults (run_mode, default_brain_model, etc) use.
    let scope_id = format!(
        "{}/{}/{}",
        project.tenant_id.as_str(),
        project.workspace_id.as_str(),
        project.project_id.as_str()
    );
    svc.set(
        Scope::Project,
        scope_id,
        "scoring_policy_json".to_owned(),
        serde_json::Value::String(policy_json.to_owned()),
    )
    .await
    .map_err(|e| e.to_string())?;
    Ok(())
}
