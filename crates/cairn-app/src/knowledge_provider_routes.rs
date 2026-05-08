//! RFC 029: HTTP handler for configuring a project's knowledge provider.
//!
//! `PUT /v1/projects/:project/knowledge-provider`
//!
//! Body: `{ "provider_ref": "cairn-default" | "plugin:<id>" }`.
//! Auth: any authenticated operator scoped to the target tenant.
//! Emits: `KnowledgeProviderConfigured`.
//!
//! The `configured_by` actor is taken from the auth principal, not the
//! request body — callers cannot impersonate another operator even with
//! a valid token.

use std::sync::Arc;

use axum::extract::{Extension, Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use serde::{Deserialize, Serialize};

use cairn_api::auth::AuthPrincipal;
use cairn_domain::ProviderRef;
use cairn_runtime::services::KnowledgeProviderServiceImpl;

use crate::extractors::enforce_project_tenant;
use crate::marketplace_routes::{operator_id_from_principal, project_key_from_path};
use crate::AppState;

#[derive(Deserialize)]
pub struct ConfigureKnowledgeProviderRequest {
    /// Provider reference. Either `"cairn-default"` or `"plugin:<plugin_id>"`.
    /// Any other shape is accepted on write — the runtime resolver flags
    /// unknown refs at query time (per RFC 029 §"Runtime resolution").
    pub provider_ref: String,
}

#[derive(Serialize)]
pub struct ConfigureKnowledgeProviderResponse {
    pub project: String,
    pub provider_ref: String,
    pub configured_by: String,
}

#[derive(Serialize)]
struct ErrorResponse {
    error: String,
}

/// PUT /v1/projects/:project/knowledge-provider
pub async fn configure_knowledge_provider_handler(
    State(state): State<Arc<AppState>>,
    Extension(principal): Extension<AuthPrincipal>,
    Path(project_id): Path<String>,
    Json(body): Json<ConfigureKnowledgeProviderRequest>,
) -> impl IntoResponse {
    let operator = operator_id_from_principal(&principal);

    let project = match project_key_from_path(&project_id) {
        Ok(p) => p,
        Err(message) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse { error: message }),
            )
                .into_response();
        }
    };

    if !enforce_project_tenant(&principal, &project) {
        return crate::errors::tenant_scope_mismatch_error().into_response();
    }

    let trimmed = body.provider_ref.trim();
    if trimmed.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "provider_ref must not be empty".to_owned(),
            }),
        )
            .into_response();
    }

    let provider_ref = ProviderRef::new(trimmed);
    let svc = KnowledgeProviderServiceImpl::new(state.runtime.store.clone());

    match cairn_runtime::services::KnowledgeProviderService::configure(
        &svc,
        &project,
        &provider_ref,
        &operator,
    )
    .await
    {
        Ok(()) => (
            StatusCode::OK,
            Json(ConfigureKnowledgeProviderResponse {
                project: format!(
                    "{}/{}/{}",
                    project.tenant_id.as_str(),
                    project.workspace_id.as_str(),
                    project.project_id.as_str()
                ),
                provider_ref: provider_ref.as_str().to_owned(),
                configured_by: operator.as_str().to_owned(),
            }),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: e.to_string(),
            }),
        )
            .into_response(),
    }
}
