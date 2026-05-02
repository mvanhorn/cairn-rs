//! RFC 026 PR-A4: integration tests for
//! `GET /v1/admin/tenants/:tenant_id/operators/:operator_id/tenant-roles`.
//!
//! Proves, through the real HTTP surface:
//!
//!   1. **God-token happy path.** After two promotes across different
//!      tenants, the list endpoint returns both grants with the projected
//!      shape.
//!   2. **Soft-revoked rows stay in the response.** A revoke leaves an
//!      audit row with `revoked_at_ms` populated; the list endpoint keeps
//!      emitting it so the UI can render revocation history.
//!   3. **Tenant-admin delegation.** An operator promoted to
//!      `TenantRole::Admin` on `T` sees the list for any operator whose
//!      home tenant is `T`. The same caller fails with the structured
//!      `tenant_role_missing` 403 when they target `T'`.
//!   4. **Cross-tenant operator id → 404.** A tenant-admin on `T` hitting
//!      the list path with an operator id that belongs to `T'` gets 404
//!      "operator profile not found for this tenant" — presence of the
//!      foreign-tenant operator is never revealed.

mod support;

use serde_json::json;
use support::live_fabric::LiveHarness;

async fn mint_operator_token(h: &LiveHarness, operator_id: &str, tenant_id: &str) -> String {
    let r = h
        .client()
        .post(format!("{}/v1/auth/tokens", h.base_url))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "operator_id": operator_id,
            "tenant_id": tenant_id,
            "name": format!("pr-a4-list-{operator_id}"),
        }))
        .send()
        .await
        .expect("POST /v1/auth/tokens");
    assert_eq!(
        r.status().as_u16(),
        201,
        "operator-token mint body: {}",
        r.text().await.unwrap_or_default()
    );
    let body: serde_json::Value = r.json().await.unwrap();
    body["token"].as_str().unwrap().to_owned()
}

async fn ensure_tenant(h: &LiveHarness, tenant_id: &str, name: &str) {
    let r = h
        .client()
        .post(format!("{}/v1/admin/tenants", h.base_url))
        .bearer_auth(&h.admin_token)
        .json(&json!({ "tenant_id": tenant_id, "name": name }))
        .send()
        .await
        .expect("create tenant reaches server");
    let status = r.status().as_u16();
    assert!(
        status == 201 || status == 409,
        "create tenant must 201 or 409; got {status}: {}",
        r.text().await.unwrap_or_default()
    );
}

async fn promote_tenant_admin(h: &LiveHarness, operator_id: &str, tenant_id: &str) {
    let r = h
        .client()
        .post(format!(
            "{}/v1/admin/operators/{operator_id}/tenant-roles/{tenant_id}/promote",
            h.base_url
        ))
        .bearer_auth(&h.admin_token)
        .json(&json!({ "role": "admin" }))
        .send()
        .await
        .expect("promote reaches server");
    assert_eq!(
        r.status().as_u16(),
        201,
        "bootstrap promote must 201: {}",
        r.text().await.unwrap_or_default()
    );
}

async fn promote_member(h: &LiveHarness, operator_id: &str, tenant_id: &str) {
    let r = h
        .client()
        .post(format!(
            "{}/v1/admin/operators/{operator_id}/tenant-roles/{tenant_id}/promote",
            h.base_url
        ))
        .bearer_auth(&h.admin_token)
        .json(&json!({ "role": "member" }))
        .send()
        .await
        .expect("promote member reaches server");
    assert_eq!(
        r.status().as_u16(),
        201,
        "member promote must 201: {}",
        r.text().await.unwrap_or_default()
    );
}

async fn revoke_tenant_role(h: &LiveHarness, operator_id: &str, tenant_id: &str) {
    let r = h
        .client()
        .delete(format!(
            "{}/v1/admin/operators/{operator_id}/tenant-roles/{tenant_id}",
            h.base_url
        ))
        .bearer_auth(&h.admin_token)
        .send()
        .await
        .expect("revoke reaches server");
    assert_eq!(
        r.status().as_u16(),
        200,
        "revoke must 200: {}",
        r.text().await.unwrap_or_default()
    );
}

async fn create_operator_profile(
    h: &LiveHarness,
    tenant_id: &str,
    display_name: &str,
    email: &str,
    role: &str,
) -> String {
    let r = h
        .client()
        .post(format!(
            "{}/v1/admin/tenants/{tenant_id}/operator-profiles",
            h.base_url
        ))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "display_name": display_name,
            "email": email,
            "role": role,
        }))
        .send()
        .await
        .expect("create operator profile");
    assert_eq!(
        r.status().as_u16(),
        201,
        "operator profile create: {}",
        r.text().await.unwrap_or_default()
    );
    let body: serde_json::Value = r.json().await.unwrap();
    body["operator_id"]
        .as_str()
        .expect("operator_id in response")
        .to_owned()
}

// ── Tests ────────────────────────────────────────────────────────────

#[tokio::test]
async fn god_token_lists_grants_across_tenants() {
    let h = LiveHarness::setup().await;
    let tenant = h.tenant.clone();
    let other = format!("{tenant}_partner");
    ensure_tenant(&h, &tenant, "Home").await;
    ensure_tenant(&h, &other, "Partner").await;

    let op_id =
        create_operator_profile(&h, &tenant, "Multi Tenant", "multi@example.com", "admin").await;

    // Grant on two different tenants.
    promote_tenant_admin(&h, &op_id, &tenant).await;
    promote_member(&h, &op_id, &other).await;

    let r = h
        .client()
        .get(format!(
            "{}/v1/admin/tenants/{tenant}/operators/{op_id}/tenant-roles",
            h.base_url
        ))
        .bearer_auth(&h.admin_token)
        .send()
        .await
        .expect("list reaches server");
    assert_eq!(
        r.status().as_u16(),
        200,
        "god-token list must 200: {}",
        r.text().await.unwrap_or_default()
    );
    let body: serde_json::Value = r.json().await.unwrap();
    let items = body["items"].as_array().expect("items array");
    assert_eq!(items.len(), 2, "both grants surfaced; body={body}");

    // hasMore is always false for this endpoint (no pagination — each
    // operator's grant set is O(tenants)). Wire shape is camelCase per
    // `ListResponse`'s `#[serde(rename_all = "camelCase")]` contract
    // (see PR #611).
    assert_eq!(body["hasMore"].as_bool(), Some(false));

    // Both rows carry the projected shape (role, granted_by, granted_at_ms).
    for row in items {
        assert!(row["tenant_id"].is_string());
        assert!(row["operator_id"].is_string());
        assert!(row["role"].is_string());
        assert!(row["granted_at_ms"].is_number());
        assert!(row["granted_by"].is_string());
    }
}

#[tokio::test]
async fn revoked_grants_remain_in_list_with_audit_fields() {
    let h = LiveHarness::setup().await;
    let tenant = h.tenant.clone();
    ensure_tenant(&h, &tenant, "Home").await;
    let op_id =
        create_operator_profile(&h, &tenant, "Revoked Op", "rev@example.com", "member").await;

    promote_tenant_admin(&h, &op_id, &tenant).await;
    revoke_tenant_role(&h, &op_id, &tenant).await;

    let r = h
        .client()
        .get(format!(
            "{}/v1/admin/tenants/{tenant}/operators/{op_id}/tenant-roles",
            h.base_url
        ))
        .bearer_auth(&h.admin_token)
        .send()
        .await
        .expect("list reaches server");
    assert_eq!(r.status().as_u16(), 200);
    let body: serde_json::Value = r.json().await.unwrap();
    let items = body["items"].as_array().expect("items array");
    assert_eq!(items.len(), 1, "soft-revoked row must survive; body={body}");
    let row = &items[0];
    assert!(
        row["revoked_at_ms"].as_u64().is_some(),
        "revoked_at_ms populated; row={row}"
    );
    assert!(
        row["revoked_by"].as_str().is_some(),
        "revoked_by populated; row={row}"
    );
}

#[tokio::test]
async fn tenant_admin_can_list_on_own_tenant_and_fails_cross_tenant() {
    let h = LiveHarness::setup().await;
    let tenant_t = h.tenant.clone();
    let tenant_prime = format!("{tenant_t}_prime");

    ensure_tenant(&h, &tenant_t, "Home").await;
    ensure_tenant(&h, &tenant_prime, "Foreign").await;

    // Operator whose home tenant is T.
    let op_id =
        create_operator_profile(&h, &tenant_t, "Local Op", "local@example.com", "member").await;
    promote_tenant_admin(&h, &op_id, &tenant_t).await;

    // Caller: tenant-admin on T, not T'.
    promote_tenant_admin(&h, "op_caller", &tenant_t).await;
    let caller_token = mint_operator_token(&h, "op_caller", &tenant_t).await;

    // Own tenant → 200.
    let own = h
        .client()
        .get(format!(
            "{}/v1/admin/tenants/{tenant_t}/operators/{op_id}/tenant-roles",
            h.base_url
        ))
        .bearer_auth(&caller_token)
        .send()
        .await
        .expect("same-tenant list");
    assert_eq!(
        own.status().as_u16(),
        200,
        "tenant-admin list of own tenant must 200: {}",
        own.text().await.unwrap_or_default()
    );

    // Foreign tenant → 403 with `tenant_role_missing` envelope.
    let foreign = h
        .client()
        .get(format!(
            "{}/v1/admin/tenants/{tenant_prime}/operators/{op_id}/tenant-roles",
            h.base_url
        ))
        .bearer_auth(&caller_token)
        .send()
        .await
        .expect("cross-tenant list");
    assert_eq!(foreign.status().as_u16(), 403, "cross-tenant list must 403");
    let body: serde_json::Value = foreign.json().await.unwrap();
    assert_eq!(
        body["error_code"].as_str(),
        Some("tenant_role_missing"),
        "cross-tenant 403 uses structured envelope; body={body}"
    );
}

#[tokio::test]
async fn list_cross_tenant_operator_id_returns_404() {
    let h = LiveHarness::setup().await;
    let tenant_t = h.tenant.clone();
    let tenant_prime = format!("{tenant_t}_prime");

    ensure_tenant(&h, &tenant_t, "Home").await;
    ensure_tenant(&h, &tenant_prime, "Foreign").await;

    // Operator belongs to tenant_prime.
    let foreign_op = create_operator_profile(
        &h,
        &tenant_prime,
        "Foreign",
        "foreign@example.com",
        "member",
    )
    .await;

    // Tenant-admin on tenant_t hits the list with tenant_t in the URL
    // but a foreign operator id. Must 404 so the caller cannot
    // enumerate foreign operator ids.
    promote_tenant_admin(&h, "op_enum_test", &tenant_t).await;
    let caller_token = mint_operator_token(&h, "op_enum_test", &tenant_t).await;

    let r = h
        .client()
        .get(format!(
            "{}/v1/admin/tenants/{tenant_t}/operators/{foreign_op}/tenant-roles",
            h.base_url
        ))
        .bearer_auth(&caller_token)
        .send()
        .await
        .expect("cross-tenant operator id list");
    assert_eq!(
        r.status().as_u16(),
        404,
        "cross-tenant operator id must 404; body={}",
        r.text().await.unwrap_or_default()
    );
}

#[tokio::test]
async fn list_unknown_operator_returns_404() {
    let h = LiveHarness::setup().await;
    let tenant = h.tenant.clone();
    ensure_tenant(&h, &tenant, "Home").await;

    let r = h
        .client()
        .get(format!(
            "{}/v1/admin/tenants/{tenant}/operators/op_ghost/tenant-roles",
            h.base_url
        ))
        .bearer_auth(&h.admin_token)
        .send()
        .await
        .expect("ghost operator list");
    assert_eq!(r.status().as_u16(), 404, "unknown operator must 404");
}
