use async_trait::async_trait;
use cairn_domain::policy::{GuardrailDecisionKind, GuardrailPolicy, GuardrailSubjectType};
use cairn_domain::TenantId;
use serde::{Deserialize, Serialize};

use crate::error::StoreError;

#[async_trait]
pub trait GuardrailReadModel: Send + Sync {
    async fn get_policy(&self, policy_id: &str) -> Result<Option<GuardrailPolicy>, StoreError>;

    async fn list_policies(
        &self,
        tenant_id: &TenantId,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<GuardrailPolicy>, StoreError>;
}

/// Audit record for a single `GuardrailPolicyEvaluated` event.
///
/// One row per evaluation action; identical (tenant_id, policy_id,
/// subject_type, subject_id, action, evaluated_at_ms) tuples are
/// deduped via the composite primary key on pg/sqlite and an
/// equivalent check-before-insert on the in-memory applier. `tenant_id`
/// leads so shared runtime-emitted `policy_id`s (e.g. "implicit_allow")
/// do not collapse evaluations across tenants.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GuardrailEvaluationRecord {
    pub policy_id: String,
    pub tenant_id: TenantId,
    pub subject_type: GuardrailSubjectType,
    /// `None` when the evaluation did not carry a concrete subject_id.
    /// Backed on pg/sqlite by an empty-string sentinel so the composite
    /// PK stays pure-NOT-NULL; the read-model converts back on query.
    pub subject_id: Option<String>,
    pub action: String,
    pub decision: GuardrailDecisionKind,
    pub reason: Option<String>,
    pub evaluated_at_ms: u64,
}

#[async_trait]
pub trait GuardrailEvaluationReadModel: Send + Sync {
    /// List evaluation audit rows for a tenant, most-recent first.
    async fn list_evaluations(
        &self,
        tenant_id: &TenantId,
        limit: usize,
    ) -> Result<Vec<GuardrailEvaluationRecord>, StoreError>;
}
