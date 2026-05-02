//! RFC 026 PR-A0: integration tests for the tenant-admin role endpoints.
//!
//! Covers the three behaviours the admin-UI series depends on:
//!
//! 1. **God-token (CAIRN_ADMIN_TOKEN) can promote + revoke** an
//!    operator on any tenant — the bootstrap path.
//! 2. **TenantAdminGuard rejects real operators** that have no
//!    `operator_tenant_roles` entry, with the structured
//!    `tenant_role_missing` body (not a bare 403).
//! 3. **A promoted operator can authorize further promotes** on their
//!    own tenant (tenant-admin delegation) but **fails on a foreign
//!    tenant** — cross-tenant isolation.
//!
//! These tests protect the A1..A6 UI surface: if this matrix breaks,
//! the admin pages silently fail 403 against real operators.

mod support;

use serde_json::json;
use support::live_fabric::LiveHarness;

/// Mint a non-admin operator token scoped to `tenant_id`. Same shape
/// as `test_admin_operator_403_matrix.rs` — the service-account
/// authenticator fills in the operator principal with the supplied
/// operator_id + tenant_id.
async fn mint_operator_token(h: &LiveHarness, operator_id: &str, tenant_id: &str) -> String {
    let r = h
        .client()
        .post(format!("{}/v1/auth/tokens", h.base_url))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "operator_id": operator_id,
            "tenant_id": tenant_id,
            "name": format!("tenant-role-test-{operator_id}"),
        }))
        .send()
        .await
        .expect("POST /v1/auth/tokens");
    assert_eq!(r.status().as_u16(), 201, "operator-token mint");
    let body: serde_json::Value = r.json().await.unwrap();
    body["token"].as_str().unwrap().to_owned()
}

/// God-token path: deployment admin promotes an operator on any
/// tenant, the projection row materializes, and the same token can
/// revoke the grant (soft delete — row retained with revocation
/// fields set).
#[tokio::test]
async fn god_token_can_promote_and_revoke() {
    let h = LiveHarness::setup().await;
    let tenant = h.tenant.clone();
    let operator = "op_admin_gold";

    // Promote
    let promote = h
        .client()
        .post(format!(
            "{}/v1/admin/operators/{operator}/tenant-roles/{tenant}/promote",
            h.base_url
        ))
        .bearer_auth(&h.admin_token)
        .json(&json!({ "role": "admin" }))
        .send()
        .await
        .expect("promote reaches server");
    assert_eq!(
        promote.status().as_u16(),
        201,
        "god-token promote must 201: body={}",
        promote.text().await.unwrap_or_default()
    );

    let body: serde_json::Value = h
        .client()
        .post(format!(
            "{}/v1/admin/operators/{operator}/tenant-roles/{tenant}/promote",
            h.base_url
        ))
        .bearer_auth(&h.admin_token)
        .json(&json!({ "role": "member" }))
        .send()
        .await
        .expect("re-promote reaches server")
        .json()
        .await
        .expect("json");
    assert_eq!(
        body["role"].as_str(),
        Some("member"),
        "re-promote updates the role; body={body}"
    );
    assert!(
        body["revoked_at_ms"].is_null(),
        "re-promote clears revocation; body={body}"
    );

    // Revoke
    let revoke = h
        .client()
        .delete(format!(
            "{}/v1/admin/operators/{operator}/tenant-roles/{tenant}",
            h.base_url
        ))
        .bearer_auth(&h.admin_token)
        .send()
        .await
        .expect("revoke reaches server");
    assert_eq!(revoke.status().as_u16(), 200, "god-token revoke must 200");
    let revoke_body: serde_json::Value = revoke.json().await.unwrap();
    assert!(
        !revoke_body["revoked_at_ms"].is_null(),
        "revoke populates revoked_at_ms; body={revoke_body}"
    );
    assert_eq!(
        revoke_body["revoked_by"].as_str(),
        Some("admin"),
        "revoked_by audit hook records who authorized; body={revoke_body}"
    );
}

/// Revoking a pair that was never granted returns 404, not 200 with
/// a stale-ish "soft delete" row. Distinct from the god-token revoke
/// of an existing pair (which returns 200).
#[tokio::test]
async fn revoke_unknown_pair_returns_404() {
    let h = LiveHarness::setup().await;
    let res = h
        .client()
        .delete(format!(
            "{}/v1/admin/operators/op_nonexistent/tenant-roles/{}",
            h.base_url, h.tenant
        ))
        .bearer_auth(&h.admin_token)
        .send()
        .await
        .expect("revoke reaches server");
    assert_eq!(res.status().as_u16(), 404, "unknown pair must 404");
}

/// Non-admin operator with no `operator_tenant_roles` entry hitting
/// the promote endpoint receives the structured `tenant_role_missing`
/// envelope (NOT the canonical `{status_code, code, message,
/// request_id}` shape). The UI `<AdminGate>` wrapper distinguishes
/// this body from a generic 403 to render an upgrade-regression
/// banner.
#[tokio::test]
async fn operator_without_grant_gets_structured_403() {
    let h = LiveHarness::setup().await;
    let op_token = mint_operator_token(&h, "op_no_grant", &h.tenant).await;

    let res = h
        .client()
        .post(format!(
            "{}/v1/admin/operators/op_target/tenant-roles/{}/promote",
            h.base_url, h.tenant
        ))
        .bearer_auth(&op_token)
        .json(&json!({ "role": "admin" }))
        .send()
        .await
        .expect("request reaches server");
    assert_eq!(
        res.status().as_u16(),
        403,
        "operator without grant must 403"
    );
    let body: serde_json::Value = res.json().await.expect("403 body is JSON");
    assert_eq!(
        body["error_code"].as_str(),
        Some("tenant_role_missing"),
        "403 body must carry structured `error_code=tenant_role_missing`; body={body}"
    );
    assert!(
        body["tenant_id"].as_str().is_some(),
        "403 body must echo tenant_id; body={body}"
    );
    assert!(
        body["operator_id"].as_str().is_some(),
        "403 body must echo operator_id; body={body}"
    );
    assert!(
        body["hint"]
            .as_str()
            .map(|h| h.contains("promote"))
            .unwrap_or(false),
        "hint must mention the remediation path; body={body}"
    );
}

/// Operator promoted to TenantRole::Admin on tenant T can then
/// promote a second operator on T, but is rejected on a foreign
/// tenant T'. The structured error envelope is what the UI uses to
/// render a clear error — asserting both the status code and the
/// envelope keeps the contract explicit.
#[tokio::test]
async fn promoted_admin_can_delegate_and_fails_cross_tenant() {
    let h = LiveHarness::setup().await;
    let tenant_t = h.tenant.clone();
    let tenant_prime = format!("{tenant_t}_prime");

    // Bootstrap: god-token promotes op_delegator on tenant T.
    let r = h
        .client()
        .post(format!(
            "{}/v1/admin/operators/op_delegator/tenant-roles/{tenant_t}/promote",
            h.base_url
        ))
        .bearer_auth(&h.admin_token)
        .json(&json!({ "role": "admin" }))
        .send()
        .await
        .expect("bootstrap promote");
    assert_eq!(r.status().as_u16(), 201, "bootstrap promote must 201");

    // Now mint an operator token for op_delegator on tenant T.
    let delegator_token = mint_operator_token(&h, "op_delegator", &tenant_t).await;

    // op_delegator can promote a second operator on the SAME tenant.
    let r = h
        .client()
        .post(format!(
            "{}/v1/admin/operators/op_child/tenant-roles/{tenant_t}/promote",
            h.base_url
        ))
        .bearer_auth(&delegator_token)
        .json(&json!({ "role": "member" }))
        .send()
        .await
        .expect("same-tenant delegate promote");
    assert_eq!(
        r.status().as_u16(),
        201,
        "tenant-admin delegation on own tenant must 201; body={}",
        r.text().await.unwrap_or_default()
    );

    // Foreign-tenant promote fails with structured `tenant_role_missing`.
    let r = h
        .client()
        .post(format!(
            "{}/v1/admin/operators/op_foreign/tenant-roles/{tenant_prime}/promote",
            h.base_url
        ))
        .bearer_auth(&delegator_token)
        .json(&json!({ "role": "admin" }))
        .send()
        .await
        .expect("cross-tenant promote");
    assert_eq!(r.status().as_u16(), 403, "cross-tenant promote must 403");
    let body: serde_json::Value = r.json().await.expect("403 body json");
    assert_eq!(
        body["error_code"].as_str(),
        Some("tenant_role_missing"),
        "cross-tenant 403 must use structured envelope; body={body}"
    );
}

/// A revoked grant must NOT authorize further promotes. The middleware
/// `attach_tenant_role` filters on `is_active()` — once the operator's
/// row carries `revoked_at_ms`, TenantAdminGuard rejects them.
#[tokio::test]
async fn revoked_admin_loses_authority() {
    let h = LiveHarness::setup().await;
    let tenant = h.tenant.clone();

    // Bootstrap + revoke via god-token.
    h.client()
        .post(format!(
            "{}/v1/admin/operators/op_once_admin/tenant-roles/{tenant}/promote",
            h.base_url
        ))
        .bearer_auth(&h.admin_token)
        .json(&json!({ "role": "admin" }))
        .send()
        .await
        .expect("bootstrap promote");
    h.client()
        .delete(format!(
            "{}/v1/admin/operators/op_once_admin/tenant-roles/{tenant}",
            h.base_url
        ))
        .bearer_auth(&h.admin_token)
        .send()
        .await
        .expect("bootstrap revoke");

    let revoked_token = mint_operator_token(&h, "op_once_admin", &tenant).await;

    // Now attempt a promote with the revoked operator's token — must
    // 403 with `tenant_role_missing`, same as a never-granted operator.
    let r = h
        .client()
        .post(format!(
            "{}/v1/admin/operators/op_another/tenant-roles/{tenant}/promote",
            h.base_url
        ))
        .bearer_auth(&revoked_token)
        .json(&json!({ "role": "member" }))
        .send()
        .await
        .expect("revoked-admin promote");
    assert_eq!(r.status().as_u16(), 403, "revoked admin must 403");
    let body: serde_json::Value = r.json().await.expect("403 body");
    assert_eq!(
        body["error_code"].as_str(),
        Some("tenant_role_missing"),
        "revoked-admin 403 body shape; body={body}"
    );
}
