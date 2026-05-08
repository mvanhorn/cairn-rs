//! RFC 030 PR-E: atomic reads for per-project provider state.
//!
//! - `GET /v1/projects/:project/providers` — both slots + resolved
//!   snapshots in one roundtrip. The dual-PUT sequence during project
//!   bootstrap is inherently split across two writes (one per family);
//!   operators use this endpoint to confirm the post-write state
//!   atomically.
//! - `GET /v1/projects/:project/ingest-jobs?family=memory|knowledge|all`
//!   — unified listing backed by the V072 `v_all_ingest_jobs` view.
//!   Required for cross-family audit queries.

use std::sync::Arc;

use axum::extract::{Extension, Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use serde::{Deserialize, Serialize};

use cairn_api::auth::AuthPrincipal;
use cairn_domain::events::ResolvedProviderSnapshot;
use cairn_memory::event_log_resolver::{snapshot_for_provider_ref, EventLogProviderResolver};
use cairn_memory::multi_provider::ProviderResolver;

use crate::extractors::enforce_project_tenant;
use crate::marketplace_routes::project_key_from_path;
use crate::AppState;

#[derive(Serialize)]
pub struct ProvidersResponse {
    pub project: String,
    pub memory: ProviderSlot,
    pub knowledge: ProviderSlot,
}

#[derive(Serialize)]
pub struct ProviderSlot {
    /// `"cairn-default"` when the project has never been configured for
    /// this family (matches the RFC 030 §"cairn-default TODO" rule).
    pub provider_ref: String,
    /// `true` when no explicit configuration exists yet — the slot is
    /// reporting the cairn-default fallback rather than an operator-set
    /// value. The event-log resolver returns the same `cairn-default`
    /// ref in both cases, so the flag is informational for operator UI.
    pub using_default_fallback: bool,
    /// Capability snapshot the plugin host had at last handshake. `None`
    /// for provider_refs the host hasn't handshaked with yet.
    pub resolved_snapshot: Option<ResolvedProviderSnapshot>,
}

#[derive(Deserialize, Debug, Clone, Copy, Default)]
#[serde(rename_all = "snake_case")]
pub enum FamilyQuery {
    Memory,
    Knowledge,
    #[default]
    All,
}

#[derive(Deserialize)]
pub struct IngestJobsParams {
    #[serde(default)]
    pub family: FamilyQuery,
}

#[derive(Serialize)]
pub struct IngestJobsResponse {
    pub project: String,
    pub family: String,
    pub jobs: Vec<IngestJob>,
}

#[derive(Serialize)]
pub struct IngestJob {
    /// `"memory"` or `"knowledge"` — matches the `family` column from
    /// `v_all_ingest_jobs`.
    pub family: String,
    pub document_id: String,
    pub provider_ref: String,
    pub status: String,
    pub source_type: Option<String>,
    pub reason: Option<String>,
    pub submitted_at_ms: i64,
    pub updated_at_ms: i64,
}

#[derive(Serialize)]
struct ErrorBody {
    error: String,
}

fn project_triple(project: &cairn_domain::tenancy::ProjectKey) -> String {
    format!(
        "{}/{}/{}",
        project.tenant_id.as_str(),
        project.workspace_id.as_str(),
        project.project_id.as_str()
    )
}

// ─── GET /v1/projects/:project/providers ─────────────────────────────────

pub async fn get_providers_handler(
    State(state): State<Arc<AppState>>,
    Extension(principal): Extension<AuthPrincipal>,
    Path(project_id): Path<String>,
) -> impl IntoResponse {
    let project = match project_key_from_path(&project_id) {
        Ok(p) => p,
        Err(msg) => {
            return (StatusCode::BAD_REQUEST, Json(ErrorBody { error: msg })).into_response();
        }
    };

    if !enforce_project_tenant(&principal, &project) {
        return crate::errors::tenant_scope_mismatch_error().into_response();
    }

    // Today the resolver only projects the knowledge-family slot. Under
    // the RFC 030 rollout rule (memory slot absent → falls back to
    // knowledge snapshot, cairn-default serves both families), both
    // slots surface the same resolved snapshot. PR-G will introduce the
    // memory-family resolver and branch this code accordingly; the
    // response shape is already the one the future PR-G wiring will
    // populate distinctly.
    let resolver = EventLogProviderResolver::new(state.runtime.store.clone());
    let pref = match resolver.resolve(&project).await {
        Ok(p) => p,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorBody {
                    error: e.to_string(),
                }),
            )
                .into_response();
        }
    };
    let snapshot = snapshot_for_provider_ref(&pref);
    let is_default = pref.as_str() == cairn_memory::multi_provider::CAIRN_DEFAULT_PROVIDER_REF;

    let slot = ProviderSlot {
        provider_ref: pref.as_str().to_owned(),
        using_default_fallback: is_default,
        resolved_snapshot: snapshot,
    };

    (
        StatusCode::OK,
        Json(ProvidersResponse {
            project: project_triple(&project),
            memory: slot.clone_for_response(),
            knowledge: slot,
        }),
    )
        .into_response()
}

impl ProviderSlot {
    fn clone_for_response(&self) -> Self {
        Self {
            provider_ref: self.provider_ref.clone(),
            using_default_fallback: self.using_default_fallback,
            resolved_snapshot: self.resolved_snapshot.clone(),
        }
    }
}

// ─── GET /v1/projects/:project/ingest-jobs?family=… ──────────────────────

pub async fn get_ingest_jobs_handler(
    State(state): State<Arc<AppState>>,
    Extension(principal): Extension<AuthPrincipal>,
    Path(project_id): Path<String>,
    Query(params): Query<IngestJobsParams>,
) -> impl IntoResponse {
    let project = match project_key_from_path(&project_id) {
        Ok(p) => p,
        Err(msg) => {
            return (StatusCode::BAD_REQUEST, Json(ErrorBody { error: msg })).into_response();
        }
    };

    if !enforce_project_tenant(&principal, &project) {
        return crate::errors::tenant_scope_mismatch_error().into_response();
    }

    let jobs = match load_ingest_jobs(state.as_ref(), &project, params.family).await {
        Ok(jobs) => jobs,
        Err(e) => {
            tracing::error!(error = %e, "ingest jobs query failed");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorBody {
                    error: "internal error while listing ingest jobs".to_owned(),
                }),
            )
                .into_response();
        }
    };

    let family_label = match params.family {
        FamilyQuery::Memory => "memory",
        FamilyQuery::Knowledge => "knowledge",
        FamilyQuery::All => "all",
    };

    (
        StatusCode::OK,
        Json(IngestJobsResponse {
            project: project_triple(&project),
            family: family_label.to_owned(),
            jobs,
        }),
    )
        .into_response()
}

/// Load ingest jobs by replaying the project's `{Memory,Knowledge}Ingest*`
/// event stream from the event log and folding it into the projection
/// shape the V072 `v_all_ingest_jobs` view exposes.
///
/// Why replay instead of hitting pg/sqlite directly: the lib-level
/// `AppState` doesn't carry the backend pool handles (bin_state does).
/// The event-log query path works identically across InMemoryStore,
/// PgEventLog, and SqliteEventLog, so operator CLIs get a consistent
/// answer regardless of backend. The projected tables from V072 remain
/// the canonical read model for bulk queries; this handler serves the
/// per-project-scope case where replaying a few hundred events is cheap
/// enough to avoid threading backend pools through the lib AppState.
async fn load_ingest_jobs(
    state: &AppState,
    project: &cairn_domain::tenancy::ProjectKey,
    family: FamilyQuery,
) -> Result<Vec<IngestJob>, String> {
    use cairn_domain::RuntimeEvent;
    use cairn_store::EventLog;
    use std::collections::HashMap;

    // Stream the full event log. For projects with thousands of ingest
    // events this is O(N); a future enhancement is to project the
    // memory/knowledge ingest views into InMemoryStore (currently
    // no-ops — see `InMemoryStore::append` in cairn-store). Until then
    // replay is the correct answer for small-to-medium scopes and
    // matches the pg/sqlite view row-for-row.
    let events = state
        .runtime
        .store
        .read_stream(None, usize::MAX)
        .await
        .map_err(|e| e.to_string())?;

    // (family, document_id) → latest job row. Rejected/Submitted/StatusUpdated
    // all key on (project, document_id) in the projection; we collapse here
    // the same way, respecting the collision-avoidance trick for rejected
    // events (synthetic document_id including envelope event_id — mirrors
    // the pg/sqlite projection in cairn-store).
    let mut jobs: HashMap<(String, String), IngestJob> = HashMap::new();

    for stored in &events {
        let env = &stored.envelope;
        let filter_matches = |family_tag: &str| {
            matches!(
                (family, family_tag),
                (FamilyQuery::All, _)
                    | (FamilyQuery::Memory, "memory")
                    | (FamilyQuery::Knowledge, "knowledge")
            )
        };
        match &env.payload {
            RuntimeEvent::KnowledgeIngestSubmitted(e) if &e.project == project => {
                if !filter_matches("knowledge") {
                    continue;
                }
                let key = ("knowledge".to_owned(), e.document_id.as_str().to_owned());
                jobs.insert(
                    key,
                    IngestJob {
                        family: "knowledge".to_owned(),
                        document_id: e.document_id.as_str().to_owned(),
                        provider_ref: e.provider_ref.as_str().to_owned(),
                        status: "submitted".to_owned(),
                        source_type: Some(e.source_type.clone()),
                        reason: None,
                        submitted_at_ms: e.at_ms as i64,
                        updated_at_ms: e.at_ms as i64,
                    },
                );
            }
            RuntimeEvent::KnowledgeIngestRejected(e) if &e.project == project => {
                if !filter_matches("knowledge") {
                    continue;
                }
                let doc_id = format!(
                    "rejected:{}:{}",
                    e.provider_ref.as_str(),
                    env.event_id.as_str()
                );
                let key = ("knowledge".to_owned(), doc_id.clone());
                jobs.insert(
                    key,
                    IngestJob {
                        family: "knowledge".to_owned(),
                        document_id: doc_id,
                        provider_ref: e.provider_ref.as_str().to_owned(),
                        status: "rejected".to_owned(),
                        source_type: None,
                        reason: Some(e.reason.clone()),
                        submitted_at_ms: e.at_ms as i64,
                        updated_at_ms: e.at_ms as i64,
                    },
                );
            }
            RuntimeEvent::KnowledgeIngestStatusUpdated(e) if &e.project == project => {
                if !filter_matches("knowledge") {
                    continue;
                }
                let key = ("knowledge".to_owned(), e.document_id.as_str().to_owned());
                if let Some(job) = jobs.get_mut(&key) {
                    job.status = e.status.clone();
                    job.updated_at_ms = e.at_ms as i64;
                }
            }
            RuntimeEvent::MemoryIngestSubmitted(e) if &e.project == project => {
                if !filter_matches("memory") {
                    continue;
                }
                let key = ("memory".to_owned(), e.document_id.as_str().to_owned());
                jobs.insert(
                    key,
                    IngestJob {
                        family: "memory".to_owned(),
                        document_id: e.document_id.as_str().to_owned(),
                        provider_ref: e.provider_ref.as_str().to_owned(),
                        status: "submitted".to_owned(),
                        source_type: Some(e.source_type.clone()),
                        reason: None,
                        submitted_at_ms: e.at_ms as i64,
                        updated_at_ms: e.at_ms as i64,
                    },
                );
            }
            RuntimeEvent::MemoryIngestRejected(e) if &e.project == project => {
                if !filter_matches("memory") {
                    continue;
                }
                let doc_id = format!(
                    "rejected:{}:{}",
                    e.provider_ref.as_str(),
                    env.event_id.as_str()
                );
                let key = ("memory".to_owned(), doc_id.clone());
                jobs.insert(
                    key,
                    IngestJob {
                        family: "memory".to_owned(),
                        document_id: doc_id,
                        provider_ref: e.provider_ref.as_str().to_owned(),
                        status: "rejected".to_owned(),
                        source_type: None,
                        reason: Some(e.reason.clone()),
                        submitted_at_ms: e.at_ms as i64,
                        updated_at_ms: e.at_ms as i64,
                    },
                );
            }
            RuntimeEvent::MemoryIngestStatusUpdated(e) if &e.project == project => {
                if !filter_matches("memory") {
                    continue;
                }
                let key = ("memory".to_owned(), e.document_id.as_str().to_owned());
                if let Some(job) = jobs.get_mut(&key) {
                    job.status = e.status.clone();
                    job.updated_at_ms = e.at_ms as i64;
                }
            }
            _ => {}
        }
    }

    let mut out: Vec<IngestJob> = jobs.into_values().collect();
    out.sort_by(|a, b| {
        b.submitted_at_ms
            .cmp(&a.submitted_at_ms)
            .then_with(|| a.document_id.cmp(&b.document_id))
    });
    Ok(out)
}
