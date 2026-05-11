//! RFC 026 PR-A7: integration matrix for the `AdminRoleGuard` →
//! `TenantAdminGuard` flip across the tenant-scoped admin surface.
//!
//! The flip widens who can authenticate: where previously only the
//! god-token (`CAIRN_ADMIN_TOKEN`) could hit these routes, now any
//! operator holding `TenantRole::Admin` on the URL's target tenant
//! clears the guard. The god-token path must still work (backward
//! compat), and cross-tenant access must still be denied.
//!
//! Matrix axes:
//!
//!   1. **God-token on own tenant** — must NOT 403 (regression guard).
//!   2. **Tenant-admin on own tenant** — must NOT 403 (new capability).
//!   3. **Tenant-admin on foreign tenant** — must 403 with
//!      `error_code == "tenant_role_missing"` (isolation).
//!   4. **Plain operator (no role) on own tenant** — must 403 with
//!      `error_code == "tenant_role_missing"` (no-escalation).
//!
//! We probe a representative subset of the flipped routes (one per
//! handler shape). Per-handler smoke is covered by the individual
//! handler tests; this file is the "did the flip land uniformly?"
//! canary.

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
            "name": format!("pr-a7-flip-{operator_id}"),
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

/// One row of the flip matrix. We fire the same method + path template
/// four times (god / own-role / foreign-role / no-role) and assert the
/// matching outcome for each principal.
#[derive(Clone, Copy)]
enum M {
    Get,
    Post,
    Delete,
}

/// Template uses `{T}` as the tenant-id placeholder so we can retarget
/// each row at "own" vs "foreign" for the cross-tenant axis.
struct Row {
    method: M,
    /// Path template with `{T}` substituted for the target tenant id.
    path_tpl: &'static str,
    label: &'static str,
}

const MATRIX: &[Row] = &[
    Row {
        method: M::Post,
        path_tpl: "/v1/admin/tenants/{T}/compact-event-log",
        label: "POST compact-event-log",
    },
    Row {
        method: M::Post,
        path_tpl: "/v1/admin/tenants/{T}/snapshot",
        label: "POST snapshot",
    },
    Row {
        method: M::Post,
        path_tpl: "/v1/admin/tenants/{T}/workspaces",
        label: "POST workspaces",
    },
    Row {
        method: M::Post,
        path_tpl: "/v1/admin/tenants/{T}/credentials",
        label: "POST credentials",
    },
    Row {
        method: M::Post,
        path_tpl: "/v1/admin/tenants/{T}/operator-profiles",
        label: "POST operator-profiles",
    },
    Row {
        method: M::Get,
        path_tpl: "/v1/admin/tenants/{T}/operator-profiles",
        label: "GET operator-profiles",
    },
    Row {
        method: M::Post,
        path_tpl: "/v1/admin/tenants/{T}/quota",
        label: "POST quota",
    },
    Row {
        method: M::Post,
        path_tpl: "/v1/admin/tenants/{T}/retention-policy",
        label: "POST retention-policy",
    },
    Row {
        method: M::Post,
        path_tpl: "/v1/admin/tenants/{T}/apply-retention",
        label: "POST apply-retention",
    },
    Row {
        method: M::Post,
        path_tpl: "/v1/admin/tenants/{T}/credentials/rotate-key",
        label: "POST credentials/rotate-key",
    },
    Row {
        method: M::Delete,
        path_tpl: "/v1/admin/tenants/{T}/sessions/sess_never_existed",
        label: "DELETE sessions/:id",
    },
    Row {
        method: M::Delete,
        path_tpl: "/v1/admin/tenants/{T}/credentials/cred_never_existed",
        label: "DELETE credentials/:id",
    },
    Row {
        method: M::Delete,
        path_tpl: "/v1/admin/tenants/{T}/workspaces/ws_never_existed",
        label: "DELETE workspaces/:id",
    },
];

/// Stub body broad enough that deserialization doesn't 400 before the
/// guard fires. The guard fires first in every case — we only need to
/// distinguish 403 from non-403, not verify handler semantics.
fn body_stub() -> serde_json::Value {
    json!({
        "name": "flip-matrix",
        "display_name": "flip",
        "email": "flip@example.com",
        "role": "member",
        "workspace_id": "flip_ws",
        "provider_id": "flip-provider",
        "plaintext_value": "flip-value",
        "new_key": "flip-new-key-long-enough-32-bytes-minimum",
        "up_to": 0,
        "max_total_tasks": 1,
        "max_concurrent_runs": 1,
        "retention_days": 30,
        "dry_run": true,
    })
}

async fn fire(
    h: &LiveHarness,
    method: M,
    path: &str,
    token: &str,
    body: &serde_json::Value,
) -> (u16, String) {
    let url = format!("{}{}", h.base_url, path);
    let req = match method {
        M::Get => h.client().get(&url),
        M::Post => h.client().post(&url).json(body),
        M::Delete => h.client().delete(&url),
    };
    let res = req
        .bearer_auth(token)
        .send()
        .await
        .expect("request reaches server");
    let status = res.status().as_u16();
    let body = res.text().await.unwrap_or_default();
    (status, body)
}

/// Full four-axis matrix: god / own-role / foreign-role / no-role
/// across every flipped route.
#[tokio::test]
async fn tenant_admin_flip_matrix() {
    let h = LiveHarness::setup().await;
    let tenant_t = h.tenant.clone();
    let tenant_prime = format!("{tenant_t}_prime");

    ensure_tenant(&h, &tenant_t, "Flip Own").await;
    ensure_tenant(&h, &tenant_prime, "Flip Foreign").await;

    // `op_admin` holds TenantRole::Admin on tenant_t only.
    promote_tenant_admin(&h, "op_admin_flip", &tenant_t).await;
    let admin_role_token = mint_operator_token(&h, "op_admin_flip", &tenant_t).await;

    // `op_plain` has no tenant-role grant anywhere.
    let plain_token = mint_operator_token(&h, "op_plain_flip", &tenant_t).await;

    let body = body_stub();
    let mut failures: Vec<String> = Vec::new();

    for row in MATRIX {
        let own_path = row.path_tpl.replace("{T}", &tenant_t);
        let foreign_path = row.path_tpl.replace("{T}", &tenant_prime);

        // (1) god-token on own → NOT 403.
        let (status, resp_body) = fire(&h, row.method, &own_path, &h.admin_token, &body).await;
        if status == 403 {
            failures.push(format!(
                "[god-token/own] {label}: unexpected 403; body={resp_body:.160}",
                label = row.label,
            ));
        }

        // (2) tenant-admin on own → NOT 403 (this is the new capability).
        let (status, resp_body) = fire(&h, row.method, &own_path, &admin_role_token, &body).await;
        if status == 403 {
            failures.push(format!(
                "[tenant-admin/own] {label}: unexpected 403 (flip didn't land); \
                 body={resp_body:.160}",
                label = row.label,
            ));
        }

        // (3) tenant-admin on FOREIGN → 403 with structured envelope.
        let (status, resp_body) =
            fire(&h, row.method, &foreign_path, &admin_role_token, &body).await;
        if status != 403 {
            failures.push(format!(
                "[tenant-admin/foreign] {label}: expected 403, got {status}; \
                 body={resp_body:.160}",
                label = row.label,
            ));
        } else if !resp_body.contains("tenant_role_missing") {
            failures.push(format!(
                "[tenant-admin/foreign] {label}: 403 envelope missing `tenant_role_missing`; \
                 body={resp_body:.160}",
                label = row.label,
            ));
        }

        // (4) plain operator (no role) on own → 403 with envelope.
        let (status, resp_body) = fire(&h, row.method, &own_path, &plain_token, &body).await;
        if status != 403 {
            failures.push(format!(
                "[plain-operator/own] {label}: expected 403, got {status}; \
                 body={resp_body:.160}",
                label = row.label,
            ));
        } else if !resp_body.contains("tenant_role_missing") {
            failures.push(format!(
                "[plain-operator/own] {label}: 403 envelope missing `tenant_role_missing`; \
                 body={resp_body:.160}",
                label = row.label,
            ));
        }
    }

    assert!(
        failures.is_empty(),
        "tenant-admin flip matrix found {} violations:\n  {}",
        failures.len(),
        failures.join("\n  ")
    );
}
