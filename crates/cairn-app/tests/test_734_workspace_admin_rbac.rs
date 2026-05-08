//! #734: workspace-admin RBAC gaps.
//!
//! Pre-fix, three list endpoints under `/v1/admin/workspaces/:id/...`
//! lacked any tenant-scope or role guard:
//!
//!   * `GET /v1/admin/workspaces/:id/members`
//!   * `GET /v1/admin/workspaces/:id/shares`
//!   * `GET /v1/admin/workspaces/:id/projects`
//!
//! Plus one mutation:
//!
//!   * `DELETE /v1/admin/workspaces/:id/members/:member_id`
//!
//! The mutation is covered by the admin/operator-403 matrix (added
//! the row in #734 commit). The three reads return **404** (per
//! the existing `list_workspaces_handler` pattern) when a non-admin
//! operator asks for a foreign-tenant workspace — 404, not 403, so
//! the endpoint can't be used as an enumeration oracle. This file
//! pins the 404 contract for those three reads.

mod support;

use serde_json::json;
use support::live_fabric::LiveHarness;

/// Mint a non-admin operator token bound to the harness's per-uuid
/// tenant. Used as the "foreign tenant" caller — the LiveHarness
/// boots with a different per-test tenant, so an operator scoped to
/// `op_matrix_734_$tenant` is not a member of `default_tenant`.
async fn mint_operator_token(h: &LiveHarness, operator_id: &str, tenant_id: &str) -> String {
    let r = h
        .client()
        .post(format!("{}/v1/auth/tokens", h.base_url))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "operator_id": operator_id,
            "tenant_id": tenant_id,
            "name": format!("rbac-734-{operator_id}"),
        }))
        .send()
        .await
        .expect("POST /v1/auth/tokens reaches server");
    assert_eq!(
        r.status().as_u16(),
        201,
        "operator-token mint: {}",
        r.text().await.unwrap_or_default(),
    );
    let body: serde_json::Value = r.json().await.expect("mint body json");
    body["token"]
        .as_str()
        .expect("mint body must carry `token`")
        .to_owned()
}

/// Create a workspace under the harness's per-uuid tenant. The
/// admin token is god-token-equivalent so this just provisions the
/// fixture.
async fn create_workspace(h: &LiveHarness, workspace_id: &str) {
    let r = h
        .client()
        .post(format!(
            "{}/v1/admin/tenants/{}/workspaces",
            h.base_url, h.tenant,
        ))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "workspace_id": workspace_id,
            "name": format!("ws-{workspace_id}"),
        }))
        .send()
        .await
        .expect("create workspace reaches server");
    let status = r.status().as_u16();
    assert!(
        matches!(status, 200 | 201),
        "create workspace status {status}: {}",
        r.text().await.unwrap_or_default(),
    );
}

/// `GET /v1/admin/workspaces/:id/members` must 404 a non-admin
/// operator from a foreign tenant. Pre-#734 fix it was a 200 with
/// the workspace's full membership list — an enumeration oracle.
#[tokio::test]
async fn list_workspace_members_rejects_foreign_tenant_operator() {
    let h = LiveHarness::setup().await;
    // Workspace lives in the harness's per-uuid tenant.
    create_workspace(&h, "ws_734_members").await;
    // Operator token scoped to a *different* tenant (`default_tenant`),
    // not the harness tenant. Not a member of the workspace's tenant.
    let foreign_op = mint_operator_token(&h, "op_734_members", "default_tenant").await;

    let r = h
        .client()
        .get(format!(
            "{}/v1/admin/workspaces/ws_734_members/members",
            h.base_url,
        ))
        .bearer_auth(foreign_op)
        .send()
        .await
        .expect("list members reaches server");
    let status = r.status().as_u16();
    let body = r.text().await.unwrap_or_default();
    assert_eq!(
        status, 404,
        "foreign-tenant operator must get 404 (not 403, not 200) — \
         enumeration-safe per the existing list_workspaces pattern. \
         body={body}",
    );
}

/// `GET /v1/admin/workspaces/:id/projects` must 404 a non-admin
/// operator from a foreign tenant. Pre-#734 fix it was a 200 list
/// of every project in the workspace.
#[tokio::test]
async fn list_projects_rejects_foreign_tenant_operator() {
    let h = LiveHarness::setup().await;
    create_workspace(&h, "ws_734_projects").await;
    let foreign_op = mint_operator_token(&h, "op_734_projects", "default_tenant").await;

    let r = h
        .client()
        .get(format!(
            "{}/v1/admin/workspaces/ws_734_projects/projects",
            h.base_url,
        ))
        .bearer_auth(foreign_op)
        .send()
        .await
        .expect("list projects reaches server");
    let status = r.status().as_u16();
    let body = r.text().await.unwrap_or_default();
    assert_eq!(
        status, 404,
        "foreign-tenant operator must get 404 listing projects of a \
         foreign workspace; body={body}",
    );
}

/// `GET /v1/admin/workspaces/:id/shares?tenant_id=X` must 404 a
/// non-admin operator from a foreign tenant — the query-supplied
/// `tenant_id` is not trustworthy on its own (same body-tenant
/// trust class as #722).
#[tokio::test]
async fn list_workspace_shares_rejects_foreign_tenant_operator() {
    let h = LiveHarness::setup().await;
    create_workspace(&h, "ws_734_shares").await;
    let foreign_op = mint_operator_token(&h, "op_734_shares", "default_tenant").await;

    // Foreign operator passes the workspace's actual tenant in the
    // query — without the auth-derived TenantScope check this
    // would 200 a list of the harness tenant's shares. Post-fix
    // the auth-derived scope mismatches and we 404.
    let r = h
        .client()
        .get(format!(
            "{}/v1/admin/workspaces/ws_734_shares/shares?tenant_id={}",
            h.base_url, h.tenant,
        ))
        .bearer_auth(foreign_op)
        .send()
        .await
        .expect("list shares reaches server");
    let status = r.status().as_u16();
    let body = r.text().await.unwrap_or_default();
    assert_eq!(
        status, 404,
        "foreign-tenant operator must get 404 even when supplying \
         the workspace's real tenant_id in the query — the \
         auth-derived TenantScope is the load-bearing check. \
         body={body}",
    );
}

/// Counter-test: an admin token still gets 200 with the actual
/// list. Pin the legitimate-path behaviour so a future tightening
/// that returns 403/404 to admin tokens is caught.
#[tokio::test]
async fn list_workspace_members_admin_still_succeeds() {
    let h = LiveHarness::setup().await;
    create_workspace(&h, "ws_734_admin_ok").await;

    let r = h
        .client()
        .get(format!(
            "{}/v1/admin/workspaces/ws_734_admin_ok/members",
            h.base_url,
        ))
        .bearer_auth(&h.admin_token)
        .send()
        .await
        .expect("list members reaches server");
    let status = r.status().as_u16();
    let body = r.text().await.unwrap_or_default();
    assert_eq!(
        status, 200,
        "admin token must still get 200 listing workspace members; body={body}",
    );
}
