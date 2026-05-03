//! RFC 026 PR-A7b: integration tests for the tenant-scoped admin GET
//! handlers' new authorization gates.
//!
//! Before this PR, six tenant-scoped GETs on `/v1/admin/tenants/:id/*`
//! had no role guard at all — any authenticated operator could read
//! any tenant's name, overview, quota, retention, snapshots, and
//! workspaces. That was an oracle (tenant-existence leak) + an over-
//! broad read surface for admin-sensitive data.
//!
//! Two categories after the fix:
//!
//!   * **Tenant-scoped read** (admin OR same-tenant): `get_tenant`,
//!     `get_tenant_overview`, `list_workspaces`. Needed for basic UI
//!     navigation — a plain operator on tenant T can read T's own
//!     identity/structure. Foreign-tenant access → 404 (no oracle).
//!
//!   * **Strict admin-only** (`TenantAdminGuard`): `get_tenant_quota`,
//!     `get_retention_policy`, `list_snapshots`. Admin-sensitive data
//!     (limits, purge windows, backup catalogue). Foreign-tenant or
//!     no-role → 403 `tenant_role_missing`.
//!
//! Matrix axes:
//!
//!   1. god-token on any tenant → NOT 4xx (regression guard)
//!   2. tenant-admin on own tenant → NOT 4xx (new/existing capability)
//!   3. tenant-admin on FOREIGN tenant → 403 (admin-only) or 404 (read)
//!   4. plain operator on own tenant → 2xx (read) or 403 (admin-only)
//!   5. plain operator on FOREIGN tenant → 404 (both categories)

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
            "name": format!("pr-a7b-{operator_id}"),
        }))
        .send()
        .await
        .expect("POST /v1/auth/tokens");
    assert_eq!(
        r.status().as_u16(),
        201,
        "mint: {}",
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
        .expect("create tenant");
    let status = r.status().as_u16();
    assert!(
        status == 201 || status == 409,
        "create tenant must 201/409; got {status}: {}",
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
        .expect("promote");
    assert_eq!(
        r.status().as_u16(),
        201,
        "promote: {}",
        r.text().await.unwrap_or_default()
    );
}

/// Gate category: what the handler uses to authorize.
#[derive(Copy, Clone, Debug)]
enum Gate {
    /// `TenantScope` admin-or-same-tenant check. Foreign-tenant → 404.
    /// Plain operator on own tenant → 2xx.
    TenantScoped,
    /// `TenantAdminGuard`. Foreign-tenant → 403 `tenant_role_missing`.
    /// Plain operator on own tenant → 403 `tenant_role_missing`.
    StrictAdmin,
}

struct Row {
    path_tpl: &'static str,
    label: &'static str,
    gate: Gate,
}

const ROWS: &[Row] = &[
    Row {
        path_tpl: "/v1/admin/tenants/{T}",
        label: "GET tenant record",
        gate: Gate::TenantScoped,
    },
    Row {
        path_tpl: "/v1/admin/tenants/{T}/overview",
        label: "GET tenant overview",
        gate: Gate::TenantScoped,
    },
    Row {
        path_tpl: "/v1/admin/tenants/{T}/workspaces",
        label: "GET tenant workspaces",
        gate: Gate::TenantScoped,
    },
    Row {
        path_tpl: "/v1/admin/tenants/{T}/quota",
        label: "GET tenant quota",
        gate: Gate::StrictAdmin,
    },
    Row {
        path_tpl: "/v1/admin/tenants/{T}/retention-policy",
        label: "GET retention policy",
        gate: Gate::StrictAdmin,
    },
    Row {
        path_tpl: "/v1/admin/tenants/{T}/snapshots",
        label: "GET snapshots",
        gate: Gate::StrictAdmin,
    },
];

async fn get_status_body(h: &LiveHarness, path: &str, token: &str) -> (u16, String) {
    let res = h
        .client()
        .get(format!("{}{}", h.base_url, path))
        .bearer_auth(token)
        .send()
        .await
        .expect("request");
    let status = res.status().as_u16();
    let body = res.text().await.unwrap_or_default();
    (status, body)
}

#[tokio::test]
async fn tenant_get_gates_matrix() {
    let h = LiveHarness::setup().await;
    let t_own = h.tenant.clone();
    let t_foreign = format!("{t_own}_foreign");

    ensure_tenant(&h, &t_own, "Own").await;
    ensure_tenant(&h, &t_foreign, "Foreign").await;
    promote_tenant_admin(&h, "op_admin_get", &t_own).await;
    let admin_token = mint_operator_token(&h, "op_admin_get", &t_own).await;
    let plain_token = mint_operator_token(&h, "op_plain_get", &t_own).await;

    let mut failures: Vec<String> = Vec::new();

    for row in ROWS {
        let own_path = row.path_tpl.replace("{T}", &t_own);
        let foreign_path = row.path_tpl.replace("{T}", &t_foreign);

        // (1) god-token on own → any non-4xx/5xx-free outcome is fine.
        // Specifically, must NOT be 403 (regression canary).
        let (status, body) = get_status_body(&h, &own_path, &h.admin_token).await;
        if status == 403 {
            failures.push(format!(
                "[god/own] {label}: unexpected 403; body={body:.160}",
                label = row.label
            ));
        }

        // (2) tenant-admin on own → gate must NOT fire. We distinguish
        // "guard rejected" (403 with `tenant_role_missing`) from
        // "handler 404 because data-not-set" (quota/retention may
        // legitimately be absent on a fresh tenant). A plain 404 is
        // a handler-level not-found, not a guard rejection — accept.
        let (status, body) = get_status_body(&h, &own_path, &admin_token).await;
        if status == 403 && body.contains("tenant_role_missing") {
            failures.push(format!(
                "[tenant-admin/own] {label}: guard rejected tenant-admin on own tenant; \
                 body={body:.160}",
                label = row.label,
            ));
        }

        // (3) tenant-admin on FOREIGN → gate-dependent.
        let (status, body) = get_status_body(&h, &foreign_path, &admin_token).await;
        match row.gate {
            Gate::TenantScoped => {
                if status != 404 {
                    failures.push(format!(
                        "[tenant-admin/foreign tenant-scoped] {label}: expected 404, got {status}; \
                         body={body:.160}",
                        label = row.label,
                    ));
                }
            }
            Gate::StrictAdmin => {
                if status != 403 {
                    failures.push(format!(
                        "[tenant-admin/foreign strict] {label}: expected 403, got {status}; \
                         body={body:.160}",
                        label = row.label,
                    ));
                } else if !body.contains("tenant_role_missing") {
                    failures.push(format!(
                        "[tenant-admin/foreign strict] {label}: 403 envelope missing \
                         `tenant_role_missing`; body={body:.160}",
                        label = row.label,
                    ));
                }
            }
        }

        // (4) plain operator on OWN → gate-dependent.
        let (status, body) = get_status_body(&h, &own_path, &plain_token).await;
        match row.gate {
            Gate::TenantScoped => {
                // Same-tenant plain operator reads OK.
                if status == 403 || status == 404 {
                    failures.push(format!(
                        "[plain/own tenant-scoped] {label}: unexpected {status}; \
                         body={body:.160}",
                        label = row.label,
                    ));
                }
            }
            Gate::StrictAdmin => {
                if status != 403 {
                    failures.push(format!(
                        "[plain/own strict] {label}: expected 403, got {status}; body={body:.160}",
                        label = row.label,
                    ));
                } else if !body.contains("tenant_role_missing") {
                    failures.push(format!(
                        "[plain/own strict] {label}: 403 envelope missing `tenant_role_missing`; \
                         body={body:.160}",
                        label = row.label,
                    ));
                }
            }
        }

        // (5) plain operator on FOREIGN → 404 in both gate shapes.
        //     `TenantScoped` returns 404 explicitly; `StrictAdmin`
        //     returns 403 with envelope (still-structured, still-no-
        //     oracle — but NOT a 404). Accept either, just verify no
        //     200/data-leak.
        let (status, body) = get_status_body(&h, &foreign_path, &plain_token).await;
        if (200..300).contains(&status) {
            failures.push(format!(
                "[plain/foreign] {label}: DATA LEAK — expected 4xx, got {status}; \
                 body={body:.160}",
                label = row.label,
            ));
        }
    }

    assert!(
        failures.is_empty(),
        "tenant-get-gates matrix found {} violations:\n  {}",
        failures.len(),
        failures.join("\n  ")
    );
}

/// Tighter per-handler regression: plain same-tenant operator CAN read
/// the three tenant-scoped reads (navigation data). This is the
/// capability PR-A7b adds — prove it actually works end-to-end.
#[tokio::test]
async fn plain_same_tenant_operator_can_read_navigation_data() {
    let h = LiveHarness::setup().await;
    let t = h.tenant.clone();
    ensure_tenant(&h, &t, "Plain-Read").await;

    let plain_token = mint_operator_token(&h, "op_plain_reads", &t).await;

    // tenant record
    let (status, body) = get_status_body(&h, &format!("/v1/admin/tenants/{t}"), &plain_token).await;
    assert_eq!(status, 200, "tenant GET 200: {body:.200}");
    let parsed: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(parsed["tenant_id"].as_str(), Some(t.as_str()));

    // tenant overview
    let (status, _) =
        get_status_body(&h, &format!("/v1/admin/tenants/{t}/overview"), &plain_token).await;
    assert_eq!(status, 200, "overview GET must be 200");

    // tenant workspaces
    let (status, _) = get_status_body(
        &h,
        &format!("/v1/admin/tenants/{t}/workspaces"),
        &plain_token,
    )
    .await;
    assert_eq!(status, 200, "workspaces GET must be 200");
}
