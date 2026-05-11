//! Admin vs operator 403 discrimination matrix — closes #404.
//!
//! # Why this matrix exists
//!
//! Audit finding (2026-04-28) grep for `403|Forbidden` across
//! `crates/cairn-app/tests/` yielded 3 hits total. `mint_operator_token`
//! appeared in one test file (`test_get_task_admin_bypass.rs`). The
//! admin surface therefore had essentially ZERO negative-path
//! coverage of the operator-token → 403 shape. The audit brief's
//! explicit rule was: "Any admin endpoint should have: (a) admin
//! token → 200, (b) operator token → 403."
//!
//! # What this test does
//!
//! For a representative set of `AdminRoleGuard`-protected endpoints,
//! we mint a non-admin operator token and hit each route, asserting
//! the response is 403 Forbidden — NOT 404 / 401 / 200.
//!
//! We intentionally pick routes that differ in:
//!   * method (GET, POST, PUT, DELETE)
//!   * path shape (collection vs singleton, nested vs flat)
//!   * handler crate (admin.rs, auth_tokens.rs, bin_admin.rs, debug.rs)
//!
//! A new admin endpoint added without AdminRoleGuard will typically
//! slip past this matrix, but every AdminRoleGuard-protected handler
//! already shares the same `WorkspaceRoleGuard<3>` implementation —
//! so this matrix is the integration-level canary: any accidental
//! guard-drop surfaces here.
//!
//! # What this test does NOT do
//!
//! It does NOT attempt to exhaustively enumerate every admin route
//! (there are ~60+). That would duplicate extractor-level unit tests.
//! It picks ~10 representative routes spanning the axes above.
//! A per-endpoint 403 assertion is the right job for the handler's
//! own test file; this matrix is the "did anyone forget the guard?"
//! canary.

mod support;

use serde_json::json;
use support::live_fabric::LiveHarness;

/// Mint a non-admin operator token scoped to `tenant_id`.
async fn mint_operator_token(h: &LiveHarness, operator_id: &str, tenant_id: &str) -> String {
    let r = h
        .client()
        .post(format!("{}/v1/auth/tokens", h.base_url))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "operator_id": operator_id,
            "tenant_id": tenant_id,
            "name": format!("admin-403-matrix-{operator_id}"),
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

/// HTTP methods we care about for the matrix. (No `Put` row yet —
/// no existing admin-only PUT route is in the matrix. Add a variant
/// here if one is added above.)
#[derive(Clone, Copy, Debug)]
enum M {
    Get,
    Post,
    Delete,
}

/// One row of the matrix: an admin-only route and what method to hit
/// it with. Body is empty for GET/DELETE; POST/PUT send a benign
/// JSON stub (the request never reaches the handler body — the
/// AdminRoleGuard fails the extractor chain first).
struct Row {
    method: M,
    path: &'static str,
    label: &'static str,
}

const MATRIX: &[Row] = &[
    // admin.rs tenant + workspace surface
    Row {
        method: M::Post,
        path: "/v1/admin/tenants",
        label: "POST /v1/admin/tenants (create tenant)",
    },
    Row {
        method: M::Get,
        path: "/v1/admin/tenants",
        label: "GET /v1/admin/tenants (list tenants)",
    },
    Row {
        method: M::Post,
        path: "/v1/admin/tenants/default_tenant/compact-event-log",
        label: "POST /v1/admin/tenants/:id/compact-event-log",
    },
    Row {
        method: M::Post,
        path: "/v1/admin/tenants/default_tenant/snapshot",
        label: "POST /v1/admin/tenants/:id/snapshot",
    },
    // NOTE: GET /v1/admin/tenants/:t/credentials is intentionally
    // NOT in this matrix. Its handler (#447 lands a `TenantScope`
    // check that returns 404 — not 403 — for a non-admin operator
    // asking for a foreign tenant. The matrix tests the
    // "admin-only" shape (403 on operator), so a 404 here is a
    // different pattern; same-tenant operators see their own
    // credentials list (per #447's design).
    Row {
        method: M::Post,
        path: "/v1/admin/tenants/default_tenant/credentials",
        label: "POST /v1/admin/tenants/:t/credentials (store)",
    },
    // admin.rs operator profile surface
    Row {
        method: M::Post,
        path: "/v1/admin/tenants/default_tenant/operator-profiles",
        label: "POST /v1/admin/tenants/:t/operator-profiles",
    },
    Row {
        method: M::Get,
        path: "/v1/admin/tenants/default_tenant/operator-profiles",
        label: "GET /v1/admin/tenants/:t/operator-profiles",
    },
    // NOTE: /v1/admin/audit-log is intentionally NOT in this
    // matrix. Its handler (`list_audit_log_handler`) distinguishes
    // admin (sees all tenants) from operator (sees own tenant
    // only) — 200 for an operator on their own tenant is the
    // documented behaviour, not a guard-drop.
    //
    // /v1/admin/logs (request logs) follows the same pattern in
    // practice (operator sees their own tenant's logs), so it
    // would also 200 for the matrix's operator — excluding.
    //
    // If we ever flip audit-log / request-logs to strict admin-only
    // (no operator read), add back here.
    // bin_admin.rs / auth
    Row {
        method: M::Post,
        path: "/v1/admin/rotate-token",
        label: "POST /v1/admin/rotate-token",
    },
    // auth_tokens — operator trying to mint another operator token.
    Row {
        method: M::Post,
        path: "/v1/auth/tokens",
        label: "POST /v1/auth/tokens",
    },
    // Delete surface — credential revoke. Uses a bogus credential id;
    // an operator must be 403 regardless of whether the id exists.
    Row {
        method: M::Delete,
        path: "/v1/admin/tenants/default_tenant/credentials/cred_does_not_exist",
        label: "DELETE /v1/admin/tenants/:t/credentials/:id",
    },
    // Delete surface — workspace session delete (admin tool).
    Row {
        method: M::Delete,
        path: "/v1/admin/tenants/default_tenant/sessions/sess_does_not_exist",
        label: "DELETE /v1/admin/tenants/:t/sessions/:session_id",
    },
    // #734: workspace member removal. Pre-fix this handler had no
    // role guard at all — any authenticated bearer token could
    // delete a member from any workspace. Now gated by
    // `AdminRoleGuard`.
    Row {
        method: M::Delete,
        path: "/v1/admin/workspaces/ws_does_not_exist/members/op_does_not_exist",
        label: "DELETE /v1/admin/workspaces/:id/members/:member_id (#734)",
    },
];

#[tokio::test]
async fn operator_token_is_403_across_admin_endpoints() {
    let h = LiveHarness::setup().await;
    let op_token = mint_operator_token(&h, "op_matrix", &h.tenant).await;

    let body_stub = json!({
        // Plausible fields so deserialization doesn't 400 before the
        // guard fires (the guard still fails closed either way, but
        // we want to unambiguously see 403, not 400).
        "tenant_id": "matrix_tenant",
        "workspace_id": "matrix_workspace",
        "project_id": "matrix_project",
        "operator_id": "matrix_operator",
        "name": "matrix",
        "display_name": "matrix",
        "provider_id": "matrix-provider",
        "plaintext_value": "matrix-value",
        "new_token": "matrix-new-token",
    });

    // Collect failures for a single panic at the end — lets us see
    // the full matrix instead of stopping at the first miss.
    let mut failures: Vec<String> = Vec::new();

    for row in MATRIX {
        let url = format!("{}{}", h.base_url, row.path);
        let req = match row.method {
            M::Get => h.client().get(&url),
            M::Post => h.client().post(&url).json(&body_stub),
            M::Delete => h.client().delete(&url),
        };
        let res = req
            .bearer_auth(&op_token)
            .send()
            .await
            .expect("request reaches server");
        let status = res.status().as_u16();
        if status != 403 {
            let body = res.text().await.unwrap_or_default();
            failures.push(format!(
                "{label}: expected 403, got {status}; body={body:.160}",
                label = row.label,
            ));
        }
    }

    assert!(
        failures.is_empty(),
        "admin-role-guard matrix found {} violations:\n  {}",
        failures.len(),
        failures.join("\n  ")
    );
}

/// Admin token on the SAME routes must NOT return 403 — the positive
/// half of the matrix. We accept any non-403 response (200/201/
/// 400/404/etc. are all fine here; we're only proving the guard
/// lets admins through). This catches a regression where an
/// AdminRoleGuard is accidentally replaced with a broader gate that
/// locks out admins too.
///
/// Excludes `POST /v1/admin/rotate-token` from the admin-positive
/// half because it has a side effect (rotates the admin token) that
/// would break the rest of the loop. The operator-negative half
/// still exercises it — the guard fires before rotation runs, so
/// the test's session stays intact.
#[tokio::test]
async fn admin_token_is_not_403_across_admin_endpoints() {
    let h = LiveHarness::setup().await;

    let body_stub = json!({
        "tenant_id": "matrix_tenant_admin",
        "workspace_id": "matrix_workspace_admin",
        "project_id": "matrix_project_admin",
        "operator_id": "matrix_operator_admin",
        "name": "matrix_admin",
        "display_name": "matrix_admin",
        "provider_id": "matrix-provider-admin",
        "plaintext_value": "matrix-value-admin",
        // Not actually sent to rotate-token (skipped below); included
        // for completeness in case a future row expects it.
        "new_token": "unused-placeholder-token-long-enough",
    });

    let mut failures: Vec<String> = Vec::new();

    for row in MATRIX {
        // Skip rotate-token in the admin-positive half: if admin
        // successfully rotates, subsequent admin requests in this
        // loop would fail auth and spurious-fail the matrix. The
        // operator-negative half still covers the guard itself.
        if row.path == "/v1/admin/rotate-token" {
            continue;
        }
        let url = format!("{}{}", h.base_url, row.path);
        let req = match row.method {
            M::Get => h.client().get(&url),
            M::Post => h.client().post(&url).json(&body_stub),
            M::Delete => h.client().delete(&url),
        };
        let res = req
            .bearer_auth(&h.admin_token)
            .send()
            .await
            .expect("request reaches server");
        let status = res.status().as_u16();
        if status == 403 {
            let body = res.text().await.unwrap_or_default();
            failures.push(format!(
                "{label}: admin was 403 (regression — admins must bypass); body={body:.160}",
                label = row.label,
            ));
        }
    }

    assert!(
        failures.is_empty(),
        "admin-role-guard regression — admin was denied on {} endpoints:\n  {}",
        failures.len(),
        failures.join("\n  ")
    );
}
