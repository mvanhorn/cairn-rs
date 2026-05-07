//! PR #719: `GET /v1/approvals/pending` requires explicit project scope.
//!
//! The legacy bin-router endpoint used to fall through to a global
//! cross-tenant scan when no `(tenant_id, workspace_id, project_id)`
//! triple was supplied — letting any authenticated caller enumerate
//! pending approvals across tenants. After #719 the endpoint hard-
//! returns 400 when any of the three is missing; cross-tenant admin
//! inboxes are served by the unified `/v1/approvals` route via
//! `TenantScope`.
//!
//! These tests run against a real `LiveHarness` subprocess because
//! the route lives in `bin_router` and is NOT mounted on the lib-
//! level `build_catalog_routes` used by the unit-test router.

mod support;

use support::live_fabric::LiveHarness;

#[tokio::test]
async fn pending_route_rejects_missing_scope() {
    let h = LiveHarness::setup().await;
    let res = h
        .client()
        .get(format!("{}/v1/approvals/pending", h.base_url))
        .bearer_auth(&h.admin_token)
        .send()
        .await
        .expect("pending reaches server");
    let status = res.status().as_u16();
    let body = res.text().await.unwrap_or_default();
    assert_eq!(
        status, 400,
        "expected 400 without scope, got {status}: {body}"
    );
    let parsed: serde_json::Value = serde_json::from_str(&body).expect("json error envelope");
    assert_eq!(
        parsed.get("code").and_then(|v| v.as_str()),
        Some("bad_request"),
        "code: {body}"
    );
    let msg = parsed
        .get("message")
        .and_then(|v| v.as_str())
        .unwrap_or_default();
    assert!(
        msg.contains("tenant_id, workspace_id, and project_id are required"),
        "message: {body}"
    );
}

#[tokio::test]
async fn pending_route_accepts_full_scope() {
    let h = LiveHarness::setup().await;
    let url = format!(
        "{}/v1/approvals/pending?tenant_id={}&workspace_id={}&project_id={}",
        h.base_url, h.tenant, h.workspace, h.project,
    );
    let res = h
        .client()
        .get(&url)
        .bearer_auth(&h.admin_token)
        .send()
        .await
        .expect("pending reaches server");
    let status = res.status().as_u16();
    let body = res.text().await.unwrap_or_default();
    assert_eq!(
        status, 200,
        "expected 200 with full scope, got {status}: {body}"
    );
    let parsed: serde_json::Value = serde_json::from_str(&body).expect("json array");
    assert!(parsed.is_array(), "expected array, got {body}");
}

#[tokio::test]
async fn pending_route_rejects_partial_scope() {
    let h = LiveHarness::setup().await;
    // tenant_id only — workspace_id and project_id missing.
    let url = format!("{}/v1/approvals/pending?tenant_id={}", h.base_url, h.tenant,);
    let res = h
        .client()
        .get(&url)
        .bearer_auth(&h.admin_token)
        .send()
        .await
        .expect("pending reaches server");
    let status = res.status().as_u16();
    let body = res.text().await.unwrap_or_default();
    assert_eq!(
        status, 400,
        "partial scope (tenant_id only) must still 400, got {status}: {body}"
    );
}
