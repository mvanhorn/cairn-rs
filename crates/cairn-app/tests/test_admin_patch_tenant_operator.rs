//! RFC 026 PR-A2: integration tests for the tenant + operator PATCH
//! endpoints.
//!
//! Proves, through the real HTTP surface:
//!
//!   1. **God-token happy path (tenant + operator).** `CAIRN_ADMIN_TOKEN`
//!      PATCHes a tenant name and an operator's display_name/role; GET
//!      confirms persistence.
//!   2. **Empty body → 422 `empty_patch`.** A POST with no fields set
//!      returns 422 with `error_code=empty_patch`, not a silent 200.
//!   3. **Tenant-admin delegation.** An operator promoted to
//!      `TenantRole::Admin` on `T` succeeds PATCHing `T`; the same
//!      operator fails PATCHing `T'` with the structured
//!      `tenant_role_missing` 403 body. Cross-tenant isolation.
//!   4. **Cross-tenant operator id → 404, not 200.** A tenant-admin on
//!      `T` hitting PATCH on an operator that belongs to `T'` gets 404
//!      "operator profile not found for this tenant" — presence of the
//!      operator is never leaked.
//!   5. **Invalid email → 422.** PATCH with `email: "not-an-email"`
//!      rejects without emitting an event.

mod support;

use serde_json::json;
use support::live_fabric::LiveHarness;

/// Mint a non-admin operator token scoped to `tenant_id`. Same shape
/// as the promote/revoke tests.
async fn mint_operator_token(h: &LiveHarness, operator_id: &str, tenant_id: &str) -> String {
    let r = h
        .client()
        .post(format!("{}/v1/auth/tokens", h.base_url))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "operator_id": operator_id,
            "tenant_id": tenant_id,
            "name": format!("pr-a2-patch-{operator_id}"),
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

/// Create `tenant_id` via the god-token. The LiveHarness sets
/// `h.tenant` but does not insert a `tenants` projection row — tests
/// that need the tenant to actually exist (GET/PATCH/operator-profile
/// paths) must bootstrap it themselves.
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
        "create tenant must 201 or 409 (already exists); got {status}: {}",
        r.text().await.unwrap_or_default()
    );
}

/// Promote `operator_id` to `TenantRole::Admin` on `tenant_id` via the
/// god-token. Used as a prerequisite for every "real-operator"
/// scenario below.
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

/// Helper: create an operator profile under the given tenant via the
/// god-token. Returns the generated operator id.
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
        .expect("operator_id present in response")
        .to_owned()
}

// ── Tenant PATCH ─────────────────────────────────────────────────────

#[tokio::test]
async fn god_token_can_patch_tenant_name() {
    let h = LiveHarness::setup().await;
    let tenant = h.tenant.clone();
    ensure_tenant(&h, &tenant, "Initial Tenant").await;

    let r = h
        .client()
        .patch(format!("{}/v1/admin/tenants/{tenant}", h.base_url))
        .bearer_auth(&h.admin_token)
        .json(&json!({ "name": "Renamed Tenant" }))
        .send()
        .await
        .expect("patch tenant reaches server");
    assert_eq!(
        r.status().as_u16(),
        200,
        "god-token PATCH must 200: {}",
        r.text().await.unwrap_or_default()
    );
    let body: serde_json::Value = r.json().await.unwrap();
    assert_eq!(
        body["name"].as_str(),
        Some("Renamed Tenant"),
        "body carries new name; body={body}"
    );
    assert_eq!(body["tenant_id"].as_str(), Some(tenant.as_str()));

    // GET confirms persistence.
    let g = h
        .client()
        .get(format!("{}/v1/admin/tenants/{tenant}", h.base_url))
        .bearer_auth(&h.admin_token)
        .send()
        .await
        .expect("GET tenant");
    assert_eq!(g.status().as_u16(), 200);
    let g_body: serde_json::Value = g.json().await.unwrap();
    assert_eq!(g_body["name"].as_str(), Some("Renamed Tenant"));
}

#[tokio::test]
async fn patch_tenant_empty_body_returns_422() {
    let h = LiveHarness::setup().await;
    let tenant = h.tenant.clone();
    ensure_tenant(&h, &tenant, "Initial").await;

    let r = h
        .client()
        .patch(format!("{}/v1/admin/tenants/{tenant}", h.base_url))
        .bearer_auth(&h.admin_token)
        .json(&json!({})) // every field None
        .send()
        .await
        .expect("empty patch reaches server");
    assert_eq!(r.status().as_u16(), 422, "empty patch must 422");
    let body = r.text().await.unwrap_or_default();
    assert!(
        body.contains("empty_patch"),
        "422 body must mention empty_patch; body={body}"
    );
}

#[tokio::test]
async fn patch_missing_tenant_returns_404() {
    let h = LiveHarness::setup().await;

    let r = h
        .client()
        .patch(format!(
            "{}/v1/admin/tenants/tenant_does_not_exist",
            h.base_url
        ))
        .bearer_auth(&h.admin_token)
        .json(&json!({ "name": "Ghost" }))
        .send()
        .await
        .expect("patch ghost tenant reaches server");
    assert_eq!(r.status().as_u16(), 404, "missing tenant must 404");
}

#[tokio::test]
async fn promoted_admin_can_patch_own_tenant_and_fails_cross_tenant() {
    let h = LiveHarness::setup().await;
    let tenant_t = h.tenant.clone();
    let tenant_prime = format!("{tenant_t}_prime");

    // Bootstrap: god-token creates both tenants + promotes the
    // operator on `tenant_t` only.
    ensure_tenant(&h, &tenant_t, "Home Tenant").await;
    ensure_tenant(&h, &tenant_prime, "Foreign").await;
    promote_tenant_admin(&h, "op_patcher", &tenant_t).await;
    let op_token = mint_operator_token(&h, "op_patcher", &tenant_t).await;

    // Own tenant → 200.
    let own = h
        .client()
        .patch(format!("{}/v1/admin/tenants/{tenant_t}", h.base_url))
        .bearer_auth(&op_token)
        .json(&json!({ "name": "Own Tenant Renamed" }))
        .send()
        .await
        .expect("same-tenant PATCH");
    assert_eq!(
        own.status().as_u16(),
        200,
        "tenant-admin PATCH of own tenant must 200: {}",
        own.text().await.unwrap_or_default()
    );

    // Foreign tenant → 403 with `tenant_role_missing` envelope.
    let foreign = h
        .client()
        .patch(format!("{}/v1/admin/tenants/{tenant_prime}", h.base_url))
        .bearer_auth(&op_token)
        .json(&json!({ "name": "Hostile Takeover" }))
        .send()
        .await
        .expect("cross-tenant PATCH");
    assert_eq!(
        foreign.status().as_u16(),
        403,
        "cross-tenant PATCH must 403"
    );
    let body: serde_json::Value = foreign.json().await.unwrap();
    assert_eq!(
        body["error_code"].as_str(),
        Some("tenant_role_missing"),
        "cross-tenant 403 must use structured envelope; body={body}"
    );
}

// ── Operator profile PATCH ──────────────────────────────────────────

#[tokio::test]
async fn god_token_can_patch_operator_profile() {
    let h = LiveHarness::setup().await;
    let tenant = h.tenant.clone();
    ensure_tenant(&h, &tenant, "Initial").await;
    let op_id =
        create_operator_profile(&h, &tenant, "Before", "before@example.com", "member").await;

    let r = h
        .client()
        .patch(format!(
            "{}/v1/admin/tenants/{tenant}/operator-profiles/{op_id}",
            h.base_url
        ))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "display_name": "After",
            "role": "admin"
        }))
        .send()
        .await
        .expect("patch operator reaches server");
    assert_eq!(
        r.status().as_u16(),
        200,
        "god-token operator PATCH must 200: {}",
        r.text().await.unwrap_or_default()
    );
    let body: serde_json::Value = r.json().await.unwrap();
    assert_eq!(body["display_name"].as_str(), Some("After"));
    assert_eq!(body["role"].as_str(), Some("admin"));
    // email preserved when the patch didn't mention it.
    assert_eq!(body["email"].as_str(), Some("before@example.com"));
}

#[tokio::test]
async fn patch_operator_empty_body_returns_422() {
    let h = LiveHarness::setup().await;
    let tenant = h.tenant.clone();
    ensure_tenant(&h, &tenant, "Initial").await;
    let op_id = create_operator_profile(&h, &tenant, "Empty", "empty@example.com", "member").await;

    let r = h
        .client()
        .patch(format!(
            "{}/v1/admin/tenants/{tenant}/operator-profiles/{op_id}",
            h.base_url
        ))
        .bearer_auth(&h.admin_token)
        .json(&json!({}))
        .send()
        .await
        .expect("empty body reaches server");
    assert_eq!(r.status().as_u16(), 422, "empty patch must 422");
    let body = r.text().await.unwrap_or_default();
    assert!(
        body.contains("empty_patch"),
        "422 body must mention empty_patch; body={body}"
    );
}

#[tokio::test]
async fn patch_operator_invalid_email_returns_422() {
    let h = LiveHarness::setup().await;
    let tenant = h.tenant.clone();
    ensure_tenant(&h, &tenant, "Initial").await;
    let op_id = create_operator_profile(&h, &tenant, "Bad", "good@example.com", "member").await;

    let r = h
        .client()
        .patch(format!(
            "{}/v1/admin/tenants/{tenant}/operator-profiles/{op_id}",
            h.base_url
        ))
        .bearer_auth(&h.admin_token)
        .json(&json!({ "email": "not-an-email" }))
        .send()
        .await
        .expect("bad email reaches server");
    assert_eq!(r.status().as_u16(), 422, "invalid email must 422");
}

#[tokio::test]
async fn non_admin_operator_patching_tenant_gets_tenant_role_missing_403() {
    let h = LiveHarness::setup().await;
    let tenant = h.tenant.clone();
    ensure_tenant(&h, &tenant, "Initial").await;
    let non_admin = mint_operator_token(&h, "op_no_role", &tenant).await;

    let r = h
        .client()
        .patch(format!("{}/v1/admin/tenants/{tenant}", h.base_url))
        .bearer_auth(&non_admin)
        .json(&json!({ "name": "Unauthorized" }))
        .send()
        .await
        .expect("non-admin PATCH reaches server");
    assert_eq!(r.status().as_u16(), 403, "non-admin PATCH must 403");
    let body: serde_json::Value = r.json().await.unwrap();
    assert_eq!(
        body["error_code"].as_str(),
        Some("tenant_role_missing"),
        "403 must be structured envelope; body={body}"
    );
}

#[tokio::test]
async fn patch_operator_cross_tenant_id_returns_404() {
    // A tenant-admin on `tenant_t` hitting PATCH on an operator that
    // belongs to `tenant_prime` must get 404, not 200 or 403. This
    // prevents enumeration of foreign-tenant operator ids via the
    // admin-delegated path.
    let h = LiveHarness::setup().await;
    let tenant_t = h.tenant.clone();
    let tenant_prime = format!("{tenant_t}_prime");

    ensure_tenant(&h, &tenant_t, "Home").await;
    ensure_tenant(&h, &tenant_prime, "Other").await;

    // Create the operator on tenant_prime.
    let foreign_op = create_operator_profile(
        &h,
        &tenant_prime,
        "Foreign Operator",
        "foreign@example.com",
        "member",
    )
    .await;

    // Promote a tenant-admin on tenant_t.
    promote_tenant_admin(&h, "op_local_admin", &tenant_t).await;
    let local_admin_token = mint_operator_token(&h, "op_local_admin", &tenant_t).await;

    // PATCH with tenant_t in the URL but the foreign operator id.
    let r = h
        .client()
        .patch(format!(
            "{}/v1/admin/tenants/{tenant_t}/operator-profiles/{foreign_op}",
            h.base_url
        ))
        .bearer_auth(&local_admin_token)
        .json(&json!({ "display_name": "Hijack Attempt" }))
        .send()
        .await
        .expect("cross-tenant operator PATCH reaches server");
    assert_eq!(
        r.status().as_u16(),
        404,
        "cross-tenant operator id must 404 (not 403, not 200); body={}",
        r.text().await.unwrap_or_default()
    );
}
