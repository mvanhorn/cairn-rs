use async_trait::async_trait;
use cairn_domain::{IngestJobId, IngestJobRecord, IngestJobState, ProjectKey};

use crate::error::StoreError;

/// Read-model for ingest job current state.
#[async_trait]
pub trait IngestJobReadModel: Send + Sync {
    async fn get(&self, job_id: &IngestJobId) -> Result<Option<IngestJobRecord>, StoreError>;

    async fn list_by_project(
        &self,
        project: &ProjectKey,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<IngestJobRecord>, StoreError>;
}

/// Stable TEXT encoding of `IngestJobState` for the `ingest_jobs.state`
/// column. Kept in lockstep with the domain `#[serde(rename_all =
/// "snake_case")]` contract so pg/sqlite/in-memory all agree on the
/// on-disk form and `rehydrate_ingest_job_state` is the single inverse.
pub fn ingest_job_state_str(state: IngestJobState) -> &'static str {
    match state {
        IngestJobState::Pending => "pending",
        IngestJobState::Processing => "processing",
        IngestJobState::Completed => "completed",
        IngestJobState::Failed => "failed",
    }
}

/// Inverse of [`ingest_job_state_str`]. Unknown values are treated as
/// a projection-corruption bug — callers surface `StoreError::Internal`.
pub fn rehydrate_ingest_job_state(raw: &str) -> Result<IngestJobState, StoreError> {
    match raw {
        "pending" => Ok(IngestJobState::Pending),
        "processing" => Ok(IngestJobState::Processing),
        "completed" => Ok(IngestJobState::Completed),
        "failed" => Ok(IngestJobState::Failed),
        other => Err(StoreError::Internal(format!(
            "ingest_jobs.state = {other:?} is not a known IngestJobState"
        ))),
    }
}
