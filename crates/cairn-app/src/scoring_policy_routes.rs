//! RFC 029 PR-B2 + RFC 030 PR-E: HTTP surface for per-project scoring
//! policies.
//!
//! The single RFC 029 endpoint (`PUT /v1/projects/:project/scoring-policy`)
//! has been split by capability family under RFC 030:
//!
//! - `PUT /v1/projects/:project/memory-scoring-policy`
//! - `PUT /v1/projects/:project/knowledge-scoring-policy`
//! - `GET /v1/projects/:project/memory-scoring-policy`
//! - `GET /v1/projects/:project/knowledge-scoring-policy`
//! - `GET /v1/projects/:project/{memory,knowledge}-scoring-policy/valid-dimensions`
//!
//! The RFC 029 endpoint lives on as a **308 Permanent Redirect** to the
//! knowledge variant so in-flight operator CLIs from the pre-PR-E world
//! keep working through the rollout window (the RFC 029 single-slot world
//! was entirely knowledge-family). A 308 preserves the request body +
//! method, unlike 301/302 which historically allowed method downgrade.
//!
//! Storage: policies are persisted as project-scope `default_settings`
//! rows keyed by `{memory,knowledge}_scoring_policy_json`. The V072
//! migration renames the pre-RFC-030 `scoring_policy_json` rows to the
//! knowledge-family key. Memory-family policies start empty (absence =
//! `ScoringPolicy::default()` at query time).
//!
//! Validation: PUT bodies may include non-zero weights on dimensions the
//! resolved provider declared `not_supported`. The server does NOT reject
//! these — it persists them and returns a `warnings[]` array listing each
//! offending dimension. Operators explicitly opted in to the weight; the
//! weight has no effect (the rescorer only surfaces dimensions the
//! provider supplies) but the policy is preserved for when the provider
//! changes.

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

/// Capability family tag for the dispatch helpers below. We use a small
/// local enum rather than `cairn_plugin_proto::CapabilityFamily` so this
/// module stays independent of the plugin-proto types; the storage
/// keys are the only durable surface that matters.
#[derive(Clone, Copy, Debug)]
enum Family {
    Memory,
    Knowledge,
}

impl Family {
    fn storage_key(self) -> &'static str {
        match self {
            Family::Memory => "memory_scoring_policy_json",
            Family::Knowledge => "knowledge_scoring_policy_json",
        }
    }
}

#[derive(Serialize)]
pub struct ConfigureScoringPolicyResponse {
    pub project: String,
    pub provider_id: Option<String>,
    /// Effective policy persisted. Returned as-is so callers can confirm
    /// the on-disk shape matches what they sent.
    pub policy: ScoringPolicy,
    /// RFC 030: non-empty when the policy references dimensions the
    /// resolved provider declares `not_supported`. The policy still
    /// persists — warnings are informational.
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub warnings: Vec<String>,
}

#[derive(Serialize)]
pub struct GetScoringPolicyResponse {
    pub project: String,
    pub policy: ScoringPolicy,
    /// `true` when no policy is stored and the caller is seeing
    /// `ScoringPolicy::default()`. Lets operator CLIs distinguish
    /// "explicit default policy persisted" from "no policy configured".
    pub using_default: bool,
}

#[derive(Serialize)]
pub struct ValidDimensionsResponse {
    pub project: String,
    pub provider_id: Option<String>,
    /// Dimensions the resolved provider surfaces right now — the set the
    /// operator UI should enable in its weight editor.
    pub valid_dimensions: Vec<String>,
    /// `true` when the project has no resolved provider yet (cairn-default
    /// fallback). The UI should treat this as "all five dimensions
    /// available" per cairn-default's posture.
    pub no_provider_resolved: bool,
}

#[derive(Serialize)]
struct ErrorBody {
    error: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    provider_id: Option<String>,
}

// ─── shared handler logic ────────────────────────────────────────────────

async fn configure_scoring_policy_family(
    family: Family,
    state: Arc<AppState>,
    principal: AuthPrincipal,
    project_id: String,
    policy: ScoringPolicy,
) -> axum::response::Response {
    let _operator = operator_id_from_principal(&principal);

    let project = match project_key_from_path(&project_id) {
        Ok(p) => p,
        Err(msg) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorBody {
                    error: msg,
                    provider_id: None,
                }),
            )
                .into_response();
        }
    };

    if !enforce_project_tenant(&principal, &project) {
        return crate::errors::tenant_scope_mismatch_error().into_response();
    }

    // Resolve the project's current provider snapshot to compute
    // `warnings[]`. RFC 030 flips the RFC 029 rule: unsupported dimensions
    // are no longer rejected at write-time — operators explicitly opt in
    // by sending a non-zero weight; the policy persists and the rescorer
    // ignores the weight at read-time (dimensions the provider doesn't
    // surface never flow through). The warning is informational so the
    // operator CLI can surface it.
    //
    // Today the resolver only projects the knowledge-family slot. For
    // memory-family PUTs we still hit the same resolver — the snapshot
    // mostly gives us a provider_id to echo back. Once PR-G lands the
    // memory-family resolver the same function takes a per-family
    // resolver arg.
    let resolver = EventLogProviderResolver::new(state.runtime.store.clone());
    let pref = resolver.resolve(&project).await.ok();
    let snapshot = pref
        .as_ref()
        .and_then(|p| snapshot_for_provider_ref(p).map(|s| (p.as_str().to_owned(), s)));

    let warnings: Vec<String> = match &snapshot {
        Some((provider_id, snap)) => {
            match validate_scoring_policy(
                &policy,
                Some(provider_id),
                Some(&snap.scoring_dimensions_surfaced),
            ) {
                Ok(()) => vec![],
                Err(ScoringPolicyValidationError::UnavailableDimensions {
                    unsupported, ..
                }) => unsupported,
            }
        }
        None => vec![],
    };

    let policy_json = match serde_json::to_string(&policy) {
        Ok(j) => j,
        Err(e) => {
            tracing::error!(error = %e, "failed to serialise scoring policy");
            return internal_error_response("internal error while persisting scoring policy");
        }
    };

    if let Err(e) =
        persist_policy_default(state.as_ref(), &project, family.storage_key(), &policy_json).await
    {
        tracing::error!(
            error = %e,
            family = ?family,
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
            project: project_triple(&project),
            provider_id: snapshot.map(|(pid, _)| pid),
            policy,
            warnings,
        }),
    )
        .into_response()
}

async fn get_scoring_policy_family(
    family: Family,
    state: Arc<AppState>,
    principal: AuthPrincipal,
    project_id: String,
) -> axum::response::Response {
    let project = match project_key_from_path(&project_id) {
        Ok(p) => p,
        Err(msg) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorBody {
                    error: msg,
                    provider_id: None,
                }),
            )
                .into_response();
        }
    };

    if !enforce_project_tenant(&principal, &project) {
        return crate::errors::tenant_scope_mismatch_error().into_response();
    }

    let (policy, using_default) = match read_policy_default(
        state.as_ref(),
        &project,
        family.storage_key(),
    )
    .await
    {
        Ok(Some(json)) => match serde_json::from_str::<ScoringPolicy>(&json) {
            Ok(p) => (p, false),
            Err(e) => {
                // Corrupt stored JSON — return default and warn the
                // operator rather than 500 so the project stays usable.
                tracing::warn!(error = %e, "stored scoring policy JSON failed to parse; using default");
                (ScoringPolicy::default(), true)
            }
        },
        Ok(None) => (ScoringPolicy::default(), true),
        Err(e) => {
            tracing::error!(error = %e, "failed to read scoring policy default");
            return internal_error_response("internal error while reading scoring policy");
        }
    };

    (
        StatusCode::OK,
        Json(GetScoringPolicyResponse {
            project: project_triple(&project),
            policy,
            using_default,
        }),
    )
        .into_response()
}

async fn get_valid_dimensions_family(
    _family: Family,
    state: Arc<AppState>,
    principal: AuthPrincipal,
    project_id: String,
) -> axum::response::Response {
    let project = match project_key_from_path(&project_id) {
        Ok(p) => p,
        Err(msg) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorBody {
                    error: msg,
                    provider_id: None,
                }),
            )
                .into_response();
        }
    };

    if !enforce_project_tenant(&principal, &project) {
        return crate::errors::tenant_scope_mismatch_error().into_response();
    }

    // Today only the knowledge-family resolver is wired (RFC 029 event
    // log). PR-G will branch on family to hit the memory-family resolver
    // too; until then both endpoints project through the knowledge
    // resolver, which already returns cairn-default's full five-dimension
    // set for unset projects. Close enough for the rollout-window
    // semantics and the unit tests in PR-G will backfill the branch.
    let resolver = EventLogProviderResolver::new(state.runtime.store.clone());
    let snapshot = resolver
        .resolve(&project)
        .await
        .ok()
        .and_then(|p| snapshot_for_provider_ref(&p).map(|s| (p.as_str().to_owned(), s)));

    let (provider_id, valid_dimensions, no_provider_resolved) = match snapshot {
        Some((pid, snap)) => (Some(pid), snap.scoring_dimensions_surfaced.clone(), false),
        None => (
            None,
            // Fallback: all five provider-required dimensions (operator
            // UI displays them greyed-out / disabled until a provider is
            // configured, so we return the full set and let the client
            // render accordingly).
            vec![
                "semantic_relevance".to_owned(),
                "lexical_relevance".to_owned(),
                "freshness_decay".to_owned(),
                "staleness_penalty".to_owned(),
                "recency_of_use".to_owned(),
            ],
            true,
        ),
    };

    (
        StatusCode::OK,
        Json(ValidDimensionsResponse {
            project: project_triple(&project),
            provider_id,
            valid_dimensions,
            no_provider_resolved,
        }),
    )
        .into_response()
}

// ─── PUT handlers ─────────────────────────────────────────────────────────

/// PUT /v1/projects/:project/memory-scoring-policy
pub async fn configure_memory_scoring_policy_handler(
    State(state): State<Arc<AppState>>,
    Extension(principal): Extension<AuthPrincipal>,
    Path(project_id): Path<String>,
    Json(policy): Json<ScoringPolicy>,
) -> impl IntoResponse {
    configure_scoring_policy_family(Family::Memory, state, principal, project_id, policy).await
}

/// PUT /v1/projects/:project/knowledge-scoring-policy
pub async fn configure_knowledge_scoring_policy_handler(
    State(state): State<Arc<AppState>>,
    Extension(principal): Extension<AuthPrincipal>,
    Path(project_id): Path<String>,
    Json(policy): Json<ScoringPolicy>,
) -> impl IntoResponse {
    configure_scoring_policy_family(Family::Knowledge, state, principal, project_id, policy).await
}

/// PUT /v1/projects/:project/scoring-policy — **308 redirect** to the
/// knowledge variant. RFC 030 chose 308 over 410 so in-flight operator
/// CLIs from the pre-PR-E world keep working through the rollout window;
/// 308 preserves the request body and method on redirect (unlike 301/302
/// which historically permitted method downgrade to GET).
pub async fn legacy_scoring_policy_redirect_handler(
    Path(project_id): Path<String>,
) -> impl IntoResponse {
    let target = format!("/v1/projects/{project_id}/knowledge-scoring-policy");
    (
        StatusCode::PERMANENT_REDIRECT,
        [(axum::http::header::LOCATION, target)],
    )
}

// ─── GET handlers ─────────────────────────────────────────────────────────

pub async fn get_memory_scoring_policy_handler(
    State(state): State<Arc<AppState>>,
    Extension(principal): Extension<AuthPrincipal>,
    Path(project_id): Path<String>,
) -> impl IntoResponse {
    get_scoring_policy_family(Family::Memory, state, principal, project_id).await
}

pub async fn get_knowledge_scoring_policy_handler(
    State(state): State<Arc<AppState>>,
    Extension(principal): Extension<AuthPrincipal>,
    Path(project_id): Path<String>,
) -> impl IntoResponse {
    get_scoring_policy_family(Family::Knowledge, state, principal, project_id).await
}

pub async fn get_memory_valid_dimensions_handler(
    State(state): State<Arc<AppState>>,
    Extension(principal): Extension<AuthPrincipal>,
    Path(project_id): Path<String>,
) -> impl IntoResponse {
    get_valid_dimensions_family(Family::Memory, state, principal, project_id).await
}

pub async fn get_knowledge_valid_dimensions_handler(
    State(state): State<Arc<AppState>>,
    Extension(principal): Extension<AuthPrincipal>,
    Path(project_id): Path<String>,
) -> impl IntoResponse {
    get_valid_dimensions_family(Family::Knowledge, state, principal, project_id).await
}

// ─── Helpers ──────────────────────────────────────────────────────────────

fn internal_error_response(message: &str) -> axum::response::Response {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(ErrorBody {
            error: message.to_owned(),
            provider_id: None,
        }),
    )
        .into_response()
}

fn project_triple(project: &cairn_domain::tenancy::ProjectKey) -> String {
    format!(
        "{}/{}/{}",
        project.tenant_id.as_str(),
        project.workspace_id.as_str(),
        project.project_id.as_str()
    )
}

/// Persist a JSON value as a project-scope default.
///
/// `scope_id` is the bare `project_id` (not the tenant/workspace/project
/// triple). That matches what `DefaultsServiceImpl::resolve` reads back
/// under `Scope::Project` — the cascading resolver keys on
/// `project_key.project_id.as_str()` for the project layer. Storing under
/// the triple (as the RFC 029 PR-B2 shipment did) would persist cleanly
/// but never surface back through `resolve`, leaving the rescorer always
/// on `ScoringPolicy::default()`.
async fn persist_policy_default(
    state: &AppState,
    project: &cairn_domain::tenancy::ProjectKey,
    key: &str,
    policy_json: &str,
) -> Result<(), String> {
    use cairn_domain::Scope;
    use cairn_runtime::services::defaults_impl::DefaultsServiceImpl;
    use cairn_runtime::DefaultsService;

    let svc = DefaultsServiceImpl::new(state.runtime.store.clone());
    svc.set(
        Scope::Project,
        project.project_id.as_str().to_owned(),
        key.to_owned(),
        serde_json::Value::String(policy_json.to_owned()),
    )
    .await
    .map_err(|e| e.to_string())?;
    Ok(())
}

/// Read the project-scope scoring policy by key. Uses the standard
/// defaults resolver so the scope-chain fallback (project → workspace →
/// tenant → system) applies consistently. Returns `None` when nothing
/// is set anywhere in the chain — the handler then renders
/// `ScoringPolicy::default()`.
async fn read_policy_default(
    state: &AppState,
    project: &cairn_domain::tenancy::ProjectKey,
    key: &str,
) -> Result<Option<String>, String> {
    use cairn_runtime::services::defaults_impl::DefaultsServiceImpl;
    use cairn_runtime::DefaultsService;

    let svc = DefaultsServiceImpl::new(state.runtime.store.clone());
    match svc.resolve(project, key).await {
        Ok(Some(v)) => Ok(Some(match v {
            serde_json::Value::String(s) => s,
            other => other.to_string(),
        })),
        Ok(None) => Ok(None),
        Err(e) => Err(e.to_string()),
    }
}

// ─── RFC 029 compat: retained single-endpoint handler for tests that
// still hit it through the old path. New code should use the split
// endpoints above. ──────────────────────────────────────────────────────

/// PUT /v1/projects/:project/scoring-policy — retained for compile-time
/// compatibility of existing callers during the RFC 030 rollout. Routes
/// to the knowledge-family handler. The router wires the legacy path to
/// `legacy_scoring_policy_redirect_handler` above, which 308s to the new
/// knowledge path, so this helper is unused at runtime.
pub async fn configure_scoring_policy_handler(
    State(state): State<Arc<AppState>>,
    Extension(principal): Extension<AuthPrincipal>,
    Path(project_id): Path<String>,
    Json(policy): Json<ScoringPolicy>,
) -> impl IntoResponse {
    configure_scoring_policy_family(Family::Knowledge, state, principal, project_id, policy).await
}
