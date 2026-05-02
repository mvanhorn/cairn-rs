//! RFC 026 PR-A0: tenant-admin role service impl.

use std::sync::Arc;

use async_trait::async_trait;
use cairn_domain::tenancy::TenantRole;
use cairn_domain::{OperatorId, RuntimeEvent, TenantId, TenantRoleGranted, TenantRoleRevoked};
use cairn_store::projections::{OperatorTenantRoleReadModel, OperatorTenantRoleRecord};
use cairn_store::EventLog;

use super::event_helpers::make_envelope;
use crate::error::RuntimeError;
use crate::tenant_roles::TenantRoleService;

pub struct TenantRoleServiceImpl<S> {
    store: Arc<S>,
}

impl<S> TenantRoleServiceImpl<S> {
    pub fn new(store: Arc<S>) -> Self {
        Self { store }
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[async_trait]
impl<S> TenantRoleService for TenantRoleServiceImpl<S>
where
    S: EventLog + OperatorTenantRoleReadModel + Send + Sync + 'static,
{
    async fn grant(
        &self,
        tenant_id: TenantId,
        operator_id: OperatorId,
        role: TenantRole,
        granted_by: String,
    ) -> Result<OperatorTenantRoleRecord, RuntimeError> {
        let event = make_envelope(RuntimeEvent::TenantRoleGranted(TenantRoleGranted {
            tenant_id: tenant_id.clone(),
            operator_id: operator_id.clone(),
            role,
            granted_by,
            at_ms: now_ms(),
        }));
        self.store.append(&[event]).await?;

        OperatorTenantRoleReadModel::get(self.store.as_ref(), &tenant_id, &operator_id)
            .await?
            .ok_or_else(|| {
                RuntimeError::Internal(
                    "operator_tenant_role not found after grant — projection drift".into(),
                )
            })
    }

    async fn revoke(
        &self,
        tenant_id: TenantId,
        operator_id: OperatorId,
        revoked_by: String,
    ) -> Result<Option<OperatorTenantRoleRecord>, RuntimeError> {
        // A revoke with no prior grant is a no-op — mirrors the
        // projection applier's "update-if-exists" semantics. Callers
        // that need "must have been granted" semantics should check
        // `get` first and surface a 404.
        if OperatorTenantRoleReadModel::get(self.store.as_ref(), &tenant_id, &operator_id)
            .await?
            .is_none()
        {
            return Ok(None);
        }

        let event = make_envelope(RuntimeEvent::TenantRoleRevoked(TenantRoleRevoked {
            tenant_id: tenant_id.clone(),
            operator_id: operator_id.clone(),
            revoked_by,
            at_ms: now_ms(),
        }));
        self.store.append(&[event]).await?;

        OperatorTenantRoleReadModel::get(self.store.as_ref(), &tenant_id, &operator_id)
            .await
            .map_err(Into::into)
    }

    async fn get(
        &self,
        tenant_id: &TenantId,
        operator_id: &OperatorId,
    ) -> Result<Option<OperatorTenantRoleRecord>, RuntimeError> {
        OperatorTenantRoleReadModel::get(self.store.as_ref(), tenant_id, operator_id)
            .await
            .map_err(Into::into)
    }

    async fn list_by_operator(
        &self,
        operator_id: &OperatorId,
    ) -> Result<Vec<OperatorTenantRoleRecord>, RuntimeError> {
        OperatorTenantRoleReadModel::list_by_operator(self.store.as_ref(), operator_id)
            .await
            .map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use cairn_domain::tenancy::TenantRole;
    use cairn_domain::{OperatorId, TenantId};
    use cairn_store::InMemoryStore;

    use crate::services::TenantRoleServiceImpl;
    use crate::tenant_roles::TenantRoleService;

    #[tokio::test]
    async fn grant_then_get_round_trip() {
        let store = Arc::new(InMemoryStore::new());
        let svc = TenantRoleServiceImpl::new(store);
        let tenant = TenantId::new("t_alpha");
        let operator = OperatorId::new("op_a");

        let granted = svc
            .grant(
                tenant.clone(),
                operator.clone(),
                TenantRole::Admin,
                "system".into(),
            )
            .await
            .unwrap();
        assert_eq!(granted.role, TenantRole::Admin);
        assert!(granted.is_active());

        let fetched = svc.get(&tenant, &operator).await.unwrap().unwrap();
        assert_eq!(fetched.role, TenantRole::Admin);
        assert_eq!(fetched.granted_by, "system");
    }

    #[tokio::test]
    async fn revoke_preserves_row_and_marks_inactive() {
        let store = Arc::new(InMemoryStore::new());
        let svc = TenantRoleServiceImpl::new(store);
        let tenant = TenantId::new("t_rev");
        let operator = OperatorId::new("op_rev");

        svc.grant(
            tenant.clone(),
            operator.clone(),
            TenantRole::Admin,
            "system".into(),
        )
        .await
        .unwrap();

        let revoked = svc
            .revoke(tenant.clone(), operator.clone(), "op_admin".into())
            .await
            .unwrap()
            .expect("revoke returns row");
        assert!(!revoked.is_active());
        assert_eq!(revoked.revoked_by.as_deref(), Some("op_admin"));
    }

    #[tokio::test]
    async fn revoke_unknown_pair_is_noop() {
        let store = Arc::new(InMemoryStore::new());
        let svc = TenantRoleServiceImpl::new(store);
        let tenant = TenantId::new("t_none");
        let operator = OperatorId::new("op_none");

        let result = svc
            .revoke(tenant, operator, "op_admin".into())
            .await
            .unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn regrant_after_revoke_clears_revocation() {
        let store = Arc::new(InMemoryStore::new());
        let svc = TenantRoleServiceImpl::new(store);
        let tenant = TenantId::new("t_cycle");
        let operator = OperatorId::new("op_cycle");

        svc.grant(
            tenant.clone(),
            operator.clone(),
            TenantRole::Admin,
            "system".into(),
        )
        .await
        .unwrap();
        svc.revoke(tenant.clone(), operator.clone(), "op_admin".into())
            .await
            .unwrap();
        let regrant = svc
            .grant(
                tenant.clone(),
                operator.clone(),
                TenantRole::Member,
                "op_admin".into(),
            )
            .await
            .unwrap();
        assert_eq!(regrant.role, TenantRole::Member);
        assert!(regrant.is_active());
        assert!(regrant.revoked_at_ms.is_none());
    }

    #[tokio::test]
    async fn list_by_operator_returns_every_tenant() {
        let store = Arc::new(InMemoryStore::new());
        let svc = TenantRoleServiceImpl::new(store);
        let operator = OperatorId::new("op_multi");

        svc.grant(
            TenantId::new("t_1"),
            operator.clone(),
            TenantRole::Admin,
            "system".into(),
        )
        .await
        .unwrap();
        svc.grant(
            TenantId::new("t_2"),
            operator.clone(),
            TenantRole::Member,
            "system".into(),
        )
        .await
        .unwrap();

        let list = svc.list_by_operator(&operator).await.unwrap();
        assert_eq!(list.len(), 2);
    }
}
