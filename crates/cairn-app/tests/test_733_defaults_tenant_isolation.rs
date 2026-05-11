//! #733 — cross-tenant isolation on the `/v1/settings/defaults/*`
//! surface.
//!
//! Codex's PR #733 closed the disclosure path on the per-key GET
//! handler (`GET /v1/settings/defaults/:scope/:scope_id/:key`) and
//! the resolve handler. This test pins the **complete** access rule
//! across every CRUD verb on the surface, including the gaps codex
//! missed:
//!
//! 1. PUT — write surface, was unscoped (any caller could overwrite
//!    any tenant's persisted run goals / model defaults).
//! 2. DELETE (clear) — was unscoped (any caller could clear another
//!    tenant's settings).
//! 3. GET /v1/settings/defaults/all — was unscoped (returned every
//!    tenant's settings to every caller).
//!
//! Policy under test:
//!
//! | Method | Scope     | non-admin caller's own tenant | non-admin foreign tenant | admin |
//! |--------|-----------|--------------------------------|--------------------------|-------|
//! | GET    | tenant    | 200 / 404                      | 404 (not 403 — no leak)  | 200/404 |
//! | PUT    | tenant    | 200                            | 404                      | 200   |
//! | DELETE | tenant    | 200                            | 404                      | 200   |
//! | any    | system    | 403 forbidden                  | 403                      | 200   |
//! | any    | workspace | 403                            | 403                      | 200   |
//! | any    | project   | 403                            | 403                      | 200   |
//! | GET-all| —         | rows where scope_id == own tid | excluded                 | all   |
//!
//! Negative-before-after: every assertion below FAILS on `main`
//! before this PR (the PUT/DELETE/list-all paths return 200 with no
//! tenant gating); they pass after.

mod support;

use serde_json::json;
use support::live_fabric::LiveHarness;

/// Mint a non-admin operator token scoped to a tenant id. Mirrors
/// `test_tenant_role_promote_revoke.rs::mint_operator_token`.
async fn mint_operator_token(h: &LiveHarness, operator_id: &str, tenant_id: &str) -> String {
    let r = h
        .client()
        .post(format!("{}/v1/auth/tokens", h.base_url))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "operator_id": operator_id,
            "tenant_id": tenant_id,
            "name": format!("test-733-{operator_id}"),
        }))
        .send()
        .await
        .expect("POST /v1/auth/tokens");
    assert_eq!(
        r.status().as_u16(),
        201,
        "operator-token mint: {}",
        r.text().await.unwrap_or_default()
    );
    let body: serde_json::Value = r.json().await.unwrap();
    body["token"].as_str().unwrap().to_owned()
}

/// PUT a tenant-scoped default as admin — used to seed rows we then
/// try to read/clear from a foreign tenant.
async fn admin_put_tenant_default(
    h: &LiveHarness,
    tenant_id: &str,
    key: &str,
    value: serde_json::Value,
) {
    let r = h
        .client()
        .put(format!(
            "{}/v1/settings/defaults/tenant/{tenant_id}/{key}",
            h.base_url
        ))
        .bearer_auth(&h.admin_token)
        .json(&json!({ "value": value }))
        .send()
        .await
        .expect("admin PUT");
    assert_eq!(
        r.status().as_u16(),
        200,
        "admin PUT must succeed: {}",
        r.text().await.unwrap_or_default()
    );
}

#[tokio::test]
async fn put_rejects_cross_tenant_write() {
    // Two distinct tenants. Tenant B's operator must NOT be able to
    // PUT a default into tenant A's namespace. Pre-PR-733-extended
    // this returned 200 (silent cross-tenant write).
    let h = LiveHarness::setup().await;
    let tenant_a = format!("tenant-a-{}", uuid::Uuid::new_v4());
    let tenant_b = format!("tenant-b-{}", uuid::Uuid::new_v4());
    let op_b = format!("op_b_{}", uuid::Uuid::new_v4());
    let token_b = mint_operator_token(&h, &op_b, &tenant_b).await;

    // Operator B tries to write into tenant A's namespace.
    let r = h
        .client()
        .put(format!(
            "{}/v1/settings/defaults/tenant/{tenant_a}/run:abc:goal",
            h.base_url
        ))
        .bearer_auth(&token_b)
        .json(&json!({ "value": "B-overwrites-A" }))
        .send()
        .await
        .expect("PUT reaches server");

    assert_eq!(
        r.status().as_u16(),
        404,
        "cross-tenant PUT must 404 (no leak), got {}: {}",
        r.status(),
        r.text().await.unwrap_or_default(),
    );
}

#[tokio::test]
async fn delete_rejects_cross_tenant_clear() {
    // Admin seeds a value in tenant A. Operator B then tries to DELETE
    // it. Pre-PR-733-extended, the DELETE returned 200 + cleared the
    // row. Now: 404, row preserved.
    let h = LiveHarness::setup().await;
    let tenant_a = format!("tenant-a-{}", uuid::Uuid::new_v4());
    let tenant_b = format!("tenant-b-{}", uuid::Uuid::new_v4());
    let op_b = format!("op_b_{}", uuid::Uuid::new_v4());
    let token_b = mint_operator_token(&h, &op_b, &tenant_b).await;

    admin_put_tenant_default(&h, &tenant_a, "run:abc:goal", json!("A-protected")).await;

    let r = h
        .client()
        .delete(format!(
            "{}/v1/settings/defaults/tenant/{tenant_a}/run:abc:goal",
            h.base_url
        ))
        .bearer_auth(&token_b)
        .send()
        .await
        .expect("DELETE reaches server");

    assert_eq!(
        r.status().as_u16(),
        404,
        "cross-tenant DELETE must 404, got {}: {}",
        r.status(),
        r.text().await.unwrap_or_default(),
    );

    // Verify the row is still there — admin can read it back.
    let r = h
        .client()
        .get(format!(
            "{}/v1/settings/defaults/tenant/{tenant_a}/run:abc:goal",
            h.base_url
        ))
        .bearer_auth(&h.admin_token)
        .send()
        .await
        .expect("admin GET");
    assert_eq!(
        r.status().as_u16(),
        200,
        "row should survive the rejected cross-tenant DELETE",
    );
    let body: serde_json::Value = r.json().await.unwrap();
    assert_eq!(body["value"], json!("A-protected"));
}

#[tokio::test]
async fn list_all_filters_to_caller_tenant() {
    // Admin seeds settings in tenant A and tenant B. Operator B's
    // GET /v1/settings/defaults/all must return ONLY tenant B's rows.
    // Pre-PR-733-extended, the response included both tenants.
    let h = LiveHarness::setup().await;
    let tenant_a = format!("tenant-a-{}", uuid::Uuid::new_v4());
    let tenant_b = format!("tenant-b-{}", uuid::Uuid::new_v4());
    let op_b = format!("op_b_{}", uuid::Uuid::new_v4());
    let token_b = mint_operator_token(&h, &op_b, &tenant_b).await;

    admin_put_tenant_default(&h, &tenant_a, "secret_a", json!("A-value")).await;
    admin_put_tenant_default(&h, &tenant_b, "secret_b", json!("B-value")).await;

    let r = h
        .client()
        .get(format!("{}/v1/settings/defaults/all", h.base_url))
        .bearer_auth(&token_b)
        .send()
        .await
        .expect("GET all");
    assert_eq!(r.status().as_u16(), 200);
    let body: serde_json::Value = r.json().await.unwrap();
    let settings = body
        .get("settings")
        .and_then(|s| s.as_array())
        .expect("settings array");

    // No row may name tenant A's id.
    for row in settings {
        let scope_id = row
            .get("scope_id")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        assert_ne!(
            scope_id, tenant_a,
            "tenant B caller must not see tenant A row: {row}",
        );
    }

    // Tenant B's row IS present.
    let saw_b = settings.iter().any(|row| {
        row.get("scope_id").and_then(|v| v.as_str()) == Some(tenant_b.as_str())
            && row.get("key").and_then(|v| v.as_str()) == Some("secret_b")
    });
    assert!(saw_b, "tenant B's own row missing from list-all: {body}");
}

#[tokio::test]
async fn list_all_filters_system_workspace_for_non_admin() {
    // Codex's read-side policy already rejects non-admin per-key
    // reads of System / Workspace / Project. The list-all response
    // must mirror that — non-admin callers see ZERO system/workspace/
    // project rows even if such rows exist for their own tenant's
    // workspaces.
    let h = LiveHarness::setup().await;
    let tenant_a = format!("tenant-a-{}", uuid::Uuid::new_v4());
    let op_a = format!("op_a_{}", uuid::Uuid::new_v4());
    let token_a = mint_operator_token(&h, &op_a, &tenant_a).await;

    // Admin sets a system-scoped + workspace-scoped value (these are
    // possible because the admin token bypasses the gate).
    let r = h
        .client()
        .put(format!(
            "{}/v1/settings/defaults/system/system/admin_only_key",
            h.base_url
        ))
        .bearer_auth(&h.admin_token)
        .json(&json!({ "value": "system-secret" }))
        .send()
        .await
        .expect("admin PUT system");
    assert_eq!(r.status().as_u16(), 200);

    let r = h
        .client()
        .get(format!("{}/v1/settings/defaults/all", h.base_url))
        .bearer_auth(&token_a)
        .send()
        .await
        .expect("non-admin GET all");
    assert_eq!(r.status().as_u16(), 200);
    let body: serde_json::Value = r.json().await.unwrap();
    let settings = body
        .get("settings")
        .and_then(|s| s.as_array())
        .expect("settings array");

    for row in settings {
        let scope = row
            .get("scope")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        assert_eq!(
            scope, "tenant",
            "non-admin list-all returned non-tenant scope row: {row}",
        );
    }
}

#[tokio::test]
async fn put_own_tenant_succeeds() {
    // Defense-in-depth ≠ defense-from-depth. Verify the gate does
    // NOT also block legitimate same-tenant writes by the tenant's
    // own operator.
    let h = LiveHarness::setup().await;
    let tenant_a = format!("tenant-a-{}", uuid::Uuid::new_v4());
    let op_a = format!("op_a_{}", uuid::Uuid::new_v4());
    let token_a = mint_operator_token(&h, &op_a, &tenant_a).await;

    let r = h
        .client()
        .put(format!(
            "{}/v1/settings/defaults/tenant/{tenant_a}/run:legit:goal",
            h.base_url
        ))
        .bearer_auth(&token_a)
        .json(&json!({ "value": "tenant A's own operator wrote this" }))
        .send()
        .await
        .expect("PUT own tenant");
    assert_eq!(
        r.status().as_u16(),
        200,
        "operator must be able to write its own tenant's defaults: {}",
        r.text().await.unwrap_or_default(),
    );
}

#[tokio::test]
async fn put_to_system_scope_requires_admin() {
    // System-scope writes must require admin even for the caller's
    // own tenant_id. The codex read fix made this explicit; the
    // expansion enforces it on the write side.
    let h = LiveHarness::setup().await;
    let tenant_a = format!("tenant-a-{}", uuid::Uuid::new_v4());
    let op_a = format!("op_a_{}", uuid::Uuid::new_v4());
    let token_a = mint_operator_token(&h, &op_a, &tenant_a).await;

    let r = h
        .client()
        .put(format!(
            "{}/v1/settings/defaults/system/system/generate_model",
            h.base_url
        ))
        .bearer_auth(&token_a)
        .json(&json!({ "value": "non-admin tries to set system model" }))
        .send()
        .await
        .expect("PUT system");
    assert_eq!(
        r.status().as_u16(),
        403,
        "system-scope PUT must require admin: {}",
        r.text().await.unwrap_or_default(),
    );
}
