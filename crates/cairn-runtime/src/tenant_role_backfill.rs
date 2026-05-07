//! RFC 026 PR-A0: upgrade backfill for `operator_tenant_roles`.
//!
//! Self-hosted cairn deployments upgrading from pre-A0 main already
//! have `operator_profiles` + `workspace_members` rows. Without a
//! backfill, every operator loses admin-UI access the moment the
//! upgrade deploys (`TenantAdminGuard` rejects non-god-token operators
//! with no `operator_tenant_roles` grant).
//!
//! Strategy (Layer A — authoritative):
//!
//! - Scan `workspace_members` rows and map workspace → tenant via the
//!   `workspaces` read model, then emit one `TenantRoleGranted` event per
//!   `(tenant, operator)` pair with `role = TenantRole::Member`.
//! - The backfill intentionally does not infer tenant-admin authority
//!   from workspace-scoped roles. Tenant-wide admin grants must be
//!   explicit and auditable as operator-authored actions.
//! - Idempotent: rows already present in `operator_tenant_roles` are
//!   skipped, so re-running the backfill at every boot is a no-op on
//!   steady-state deployments.
//! - WARN-on-boot: the number + sample of backfilled pairs is logged
//!   to stderr so operators see exactly what the upgrade attached. RFC
//!   Open Q#4.
//!
//! The emission goes through `TenantRoleService::grant` so events land
//! in the durable event log, projections reconstruct correctly across
//! backends, and the audit trail lives in the same place every other
//! tenant-role grant lives.

use std::collections::BTreeSet;

use cairn_domain::tenancy::TenantRole;
use cairn_domain::OperatorId;
use cairn_store::projections::{
    OperatorTenantRoleReadModel, TenantReadModel, WorkspaceMembershipReadModel, WorkspaceReadModel,
};

use crate::error::RuntimeError;
use crate::tenant_roles::TenantRoleService;

/// Result summary of a backfill run. Returned to the caller so the
/// boot sequence can log the count + per-pair breakdown at WARN.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TenantRoleBackfillReport {
    /// Pairs newly granted by this backfill run. Format:
    /// `(tenant_id, operator_id, role)`.
    pub granted: Vec<(String, String, TenantRole)>,
    /// Pairs that already had a projected row; skipped for
    /// idempotency.
    pub skipped_already_present: usize,
}

impl TenantRoleBackfillReport {
    pub fn total_emitted(&self) -> usize {
        self.granted.len()
    }
}

/// Source-of-truth contract: the backfill needs three projections
/// (tenants, workspaces, workspace_members) plus the tenant-role
/// service. Named so the store trait bounds stay explicit at the
/// call site.
pub async fn run_tenant_role_backfill<S, R>(
    store: &S,
    service: &R,
    tenant_page_size: usize,
) -> Result<TenantRoleBackfillReport, RuntimeError>
where
    S: TenantReadModel
        + WorkspaceReadModel
        + WorkspaceMembershipReadModel
        + OperatorTenantRoleReadModel
        + Send
        + Sync,
    R: TenantRoleService + ?Sized,
{
    let mut report = TenantRoleBackfillReport::default();

    // Walk every tenant. `TenantReadModel::list` pages; take a
    // reasonable per-call size so the backfill bounds memory on
    // deployments with thousands of tenants.
    let mut offset = 0usize;
    loop {
        let tenants = TenantReadModel::list(store, tenant_page_size, offset)
            .await
            .map_err(RuntimeError::from)?;
        if tenants.is_empty() {
            break;
        }
        let page_len = tenants.len();

        for tenant in &tenants {
            let tenant_id = tenant.tenant_id.clone();

            // Per-tenant: walk every workspace's members and collect
            // the unique set of operators with any membership. A
            // BTreeSet gives us deterministic iteration for the WARN
            // log + test assertions.
            //
            // Paginate workspaces so a tenant with >page_size workspaces
            // doesn't get a partial backfill that locks legitimate
            // admins out of the admin UI. Gemini PR #609 comment.
            const WORKSPACE_PAGE: usize = 500;
            let mut workspaces: Vec<cairn_domain::org::WorkspaceRecord> = Vec::new();
            let mut ws_offset = 0usize;
            loop {
                let page = WorkspaceReadModel::list_by_tenant(
                    store,
                    &tenant_id,
                    WORKSPACE_PAGE,
                    ws_offset,
                )
                .await
                .map_err(RuntimeError::from)?;
                if page.is_empty() {
                    break;
                }
                let page_len = page.len();
                workspaces.extend(page);
                ws_offset += page_len;
                if page_len < WORKSPACE_PAGE {
                    break;
                }
            }

            let mut operators: BTreeSet<String> = BTreeSet::new();
            for workspace in &workspaces {
                let members = WorkspaceMembershipReadModel::list_workspace_members(
                    store,
                    workspace.workspace_id.as_str(),
                )
                .await
                .map_err(RuntimeError::from)?;
                for member in &members {
                    // Backfill preserves tenant presence only. It does
                    // NOT promote workspace-scoped authority to
                    // tenant-scoped Admin — every operator with any
                    // workspace membership lands as `Member` and any
                    // tenant-admin grant must be an explicit, audited
                    // operator-authored action.
                    operators.insert(member.operator_id.clone());
                }
            }

            // Emit one Member grant per operator. Skip pairs that
            // already have a row in `operator_tenant_roles` — covers
            // re-boot idempotency + cases where a PR-A0-deployed
            // tenant already has live grants.
            for op_str in operators {
                let operator_id = OperatorId::new(op_str.clone());
                let existing = OperatorTenantRoleReadModel::get(store, &tenant_id, &operator_id)
                    .await
                    .map_err(RuntimeError::from)?;
                if existing.is_some() {
                    report.skipped_already_present += 1;
                    continue;
                }
                service
                    .grant(
                        tenant_id.clone(),
                        operator_id,
                        TenantRole::Member,
                        "upgrade-backfill".to_owned(),
                    )
                    .await?;
                report
                    .granted
                    .push((tenant_id.as_str().to_owned(), op_str, TenantRole::Member));
            }
        }

        offset += page_len;
        if page_len < tenant_page_size {
            break;
        }
    }

    Ok(report)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use cairn_domain::tenancy::WorkspaceRole;
    use cairn_domain::{TenantId, WorkspaceId};
    use cairn_store::InMemoryStore;

    use crate::services::{
        TenantRoleServiceImpl, TenantServiceImpl, WorkspaceMembershipServiceImpl,
        WorkspaceServiceImpl,
    };
    use crate::tenants::TenantService;
    use crate::workspace_memberships::WorkspaceMembershipService;
    use crate::workspaces::WorkspaceService;

    use super::run_tenant_role_backfill;

    /// Seeds two tenants × workspaces × members mirroring a realistic
    /// pre-A0 deployment, then asserts the backfill emits one grant
    /// per operator with the correct role.
    #[tokio::test]
    async fn backfill_maps_workspace_memberships_to_tenant_member() {
        let store = Arc::new(InMemoryStore::new());
        let tenants = TenantServiceImpl::new(store.clone());
        let workspaces = WorkspaceServiceImpl::new(store.clone());
        let memberships = WorkspaceMembershipServiceImpl::new(store.clone());
        let roles = TenantRoleServiceImpl::new(store.clone());

        tenants
            .create(TenantId::new("t_alpha"), "Alpha".into())
            .await
            .unwrap();
        tenants
            .create(TenantId::new("t_beta"), "Beta".into())
            .await
            .unwrap();

        let _ws_a = workspaces
            .create(
                TenantId::new("t_alpha"),
                WorkspaceId::new("ws_a"),
                "Workspace A".into(),
            )
            .await
            .unwrap();
        let _ws_b = workspaces
            .create(
                TenantId::new("t_beta"),
                WorkspaceId::new("ws_b"),
                "Workspace B".into(),
            )
            .await
            .unwrap();

        // op_admin: Admin on ws_a (tenant t_alpha) still maps to Member at tenant scope.
        memberships
            .add_member(
                cairn_domain::tenancy::WorkspaceKey::new("t_alpha", "ws_a"),
                "op_admin".into(),
                WorkspaceRole::Admin,
            )
            .await
            .unwrap();
        // op_member: Member on ws_b (tenant t_beta) → expect TenantRole::Member on t_beta.
        memberships
            .add_member(
                cairn_domain::tenancy::WorkspaceKey::new("t_beta", "ws_b"),
                "op_member".into(),
                WorkspaceRole::Member,
            )
            .await
            .unwrap();

        let report = run_tenant_role_backfill(store.as_ref(), &roles, 100)
            .await
            .unwrap();

        assert_eq!(report.granted.len(), 2, "one grant per operator pair");
        let granted: std::collections::HashSet<(
            String,
            String,
            cairn_domain::tenancy::TenantRole,
        )> = report.granted.iter().cloned().collect();
        assert!(granted.contains(&(
            "t_alpha".into(),
            "op_admin".into(),
            cairn_domain::tenancy::TenantRole::Member
        )));
        assert!(granted.contains(&(
            "t_beta".into(),
            "op_member".into(),
            cairn_domain::tenancy::TenantRole::Member
        )));
    }

    /// Idempotency: running backfill twice emits events only on the
    /// first pass. A re-boot must not double-grant.
    #[tokio::test]
    async fn backfill_is_idempotent_on_second_boot() {
        let store = Arc::new(InMemoryStore::new());
        let tenants = TenantServiceImpl::new(store.clone());
        let workspaces = WorkspaceServiceImpl::new(store.clone());
        let memberships = WorkspaceMembershipServiceImpl::new(store.clone());
        let roles = TenantRoleServiceImpl::new(store.clone());

        tenants
            .create(TenantId::new("t_idem"), "Idem".into())
            .await
            .unwrap();
        workspaces
            .create(
                TenantId::new("t_idem"),
                WorkspaceId::new("ws_i"),
                "Workspace I".into(),
            )
            .await
            .unwrap();
        memberships
            .add_member(
                cairn_domain::tenancy::WorkspaceKey::new("t_idem", "ws_i"),
                "op_i".into(),
                WorkspaceRole::Admin,
            )
            .await
            .unwrap();

        let first = run_tenant_role_backfill(store.as_ref(), &roles, 100)
            .await
            .unwrap();
        assert_eq!(first.granted.len(), 1, "first run grants 1 pair");

        let second = run_tenant_role_backfill(store.as_ref(), &roles, 100)
            .await
            .unwrap();
        assert_eq!(second.granted.len(), 0, "second run grants nothing");
        assert_eq!(second.skipped_already_present, 1);
    }

    /// Multiple memberships on the same tenant collapse to one Member grant.
    #[tokio::test]
    async fn multiple_memberships_yield_single_member_grant() {
        let store = Arc::new(InMemoryStore::new());
        let tenants = TenantServiceImpl::new(store.clone());
        let workspaces = WorkspaceServiceImpl::new(store.clone());
        let memberships = WorkspaceMembershipServiceImpl::new(store.clone());
        let roles = TenantRoleServiceImpl::new(store.clone());

        tenants
            .create(TenantId::new("t_multi"), "Multi".into())
            .await
            .unwrap();
        for ws_id in ["ws_a", "ws_b"] {
            workspaces
                .create(
                    TenantId::new("t_multi"),
                    WorkspaceId::new(ws_id),
                    format!("Workspace {ws_id}"),
                )
                .await
                .unwrap();
            memberships
                .add_member(
                    cairn_domain::tenancy::WorkspaceKey::new("t_multi", ws_id),
                    "op_shared".into(),
                    WorkspaceRole::Admin,
                )
                .await
                .unwrap();
        }

        let report = run_tenant_role_backfill(store.as_ref(), &roles, 100)
            .await
            .unwrap();
        assert_eq!(report.granted.len(), 1, "single Member grant per tenant");
        assert_eq!(
            report.granted[0].2,
            cairn_domain::tenancy::TenantRole::Member
        );
    }
}
