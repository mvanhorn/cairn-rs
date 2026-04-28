//! Regression tests for #453 / #454 — `POST /v1/plugins` and
//! `DELETE /v1/plugins/:id` MUST refuse non-admin callers with a 403.
//!
//! Pre-fix: `create_plugin_handler` and `delete_plugin_handler` accepted
//! any authenticated bearer token. A tenant-A operator could register a
//! plugin manifest pointing at an attacker-controlled binary; tenant B's
//! agent dispatching to that plugin name would execute attacker code in
//! the cairn-app server's process context. Full server compromise from a
//! low-privilege account.
//!
//! Post-fix: `AdminRoleGuard` fails closed with 403 for non-admin
//! principals on both mutation routes. These tests drive the real
//! cairn-app binary via `LiveHarness` and cover the three principal
//! classes:
//!
//!   * Admin token — bypass, 201 / 200.
//!   * Operator token scoped to a tenant — no workspace admin role, 403.
//!   * No token — 401 via bearer middleware (baseline, not the fix).
//!
//! Test fixtures build the minimal `PluginManifest` JSON shape the
//! handler deserializes (see `crates/cairn-tools/src/plugins.rs`). We
//! don't need a real binary on disk — the registry check happens before
//! the plugin host tries to spawn, so a fake command is fine for the
//! authz assertion. If the registration path ever requires a real
//! binary before the authz check runs, that's a defense-in-depth
//! regression worth failing on.

mod support;

use serde_json::{json, Value};
use support::live_fabric::LiveHarness;

/// Mint an operator token scoped to a specific tenant. This is the
/// exact helper shape used by `test_get_task_admin_bypass.rs`; the
/// token it returns is NOT admin, so any endpoint guarded by
/// `AdminRoleGuard` must refuse it.
async fn mint_operator_token(h: &LiveHarness, operator_id: &str, tenant_id: &str) -> String {
    let res = h
        .client()
        .post(format!("{}/v1/auth/tokens", h.base_url))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "operator_id": operator_id,
            "tenant_id": tenant_id,
            "name": format!("plugins-rbac-test-{operator_id}"),
        }))
        .send()
        .await
        .expect("POST /v1/auth/tokens reaches server");
    assert_eq!(
        res.status().as_u16(),
        201,
        "auth token create: {}",
        res.text().await.unwrap_or_default()
    );
    let body: Value = res.json().await.expect("auth token json");
    body["token"]
        .as_str()
        .expect("auth token response has `token`")
        .to_owned()
}

/// Minimal plugin manifest JSON. The authz refusal must fire before
/// any manifest semantics do, so the exact execution class doesn't
/// matter for the 403 assertion — we use `supervised_process` because
/// it's the more permissive variant (less chance of a downstream
/// validator rejecting the manifest on the admin path before the test
/// can assert).
fn manifest(id: &str) -> Value {
    json!({
        "id": id,
        "name": "RBAC Test Plugin",
        "version": "0.0.1",
        "command": ["/does/not/need/to/exist"],
        "capabilities": [
            { "type": "tool_provider", "tools": ["dummy.tool"] }
        ],
        "permissions": { "permissions": [] },
        "limits": null,
        "execution_class": "supervised_process",
        "description": "Regression fixture for #453/#454"
    })
}

// ── #453: POST /v1/plugins ───────────────────────────────────────────────────

/// Operator token (authenticated, NOT admin) must get 403. Before the
/// fix this silently 201'd the attacker's manifest into the global
/// plugin registry.
#[tokio::test]
async fn create_plugin_refused_for_non_admin_operator() {
    let h = LiveHarness::setup().await;
    let op_token = mint_operator_token(&h, "op_plugin_rbac", &h.tenant).await;

    let res = h
        .client()
        .post(format!("{}/v1/plugins", h.base_url))
        .bearer_auth(&op_token)
        .json(&manifest("rbac-test.create-denied"))
        .send()
        .await
        .expect("POST /v1/plugins reaches server");
    assert_eq!(
        res.status().as_u16(),
        403,
        "non-admin create must be 403 (pre-fix: 201). body: {}",
        res.text().await.unwrap_or_default(),
    );
}

/// Baseline: no bearer token at all must 401. This is the auth
/// middleware, not the role guard — we assert it so a future refactor
/// can't silently drop the auth layer and have only the role guard
/// catching unauthenticated callers.
#[tokio::test]
async fn create_plugin_refused_without_bearer() {
    let h = LiveHarness::setup().await;

    let res = h
        .client()
        .post(format!("{}/v1/plugins", h.base_url))
        .json(&manifest("rbac-test.create-noauth"))
        .send()
        .await
        .expect("POST /v1/plugins reaches server");
    assert_eq!(
        res.status().as_u16(),
        401,
        "no-bearer create must be 401. body: {}",
        res.text().await.unwrap_or_default(),
    );
}

/// Admin token continues to work — the fix must not break legitimate
/// admin installs.
#[tokio::test]
async fn create_plugin_accepted_for_admin() {
    let h = LiveHarness::setup().await;

    let res = h
        .client()
        .post(format!("{}/v1/plugins", h.base_url))
        .bearer_auth(&h.admin_token)
        .json(&manifest("rbac-test.create-ok"))
        .send()
        .await
        .expect("POST /v1/plugins reaches server");
    // Admin path: the manifest is valid JSON; registry accepts it and
    // returns 201 with the echoed manifest. If the plugin host rejects
    // it (400) that's still proof the admin guard let the request
    // through — the failure would come from a downstream layer, not
    // from `AdminRoleGuard`.
    let status = res.status().as_u16();
    let body = res.text().await.unwrap_or_default();
    assert!(
        matches!(status, 201 | 400),
        "admin create must pass the role guard (201 ok, 400 only acceptable for manifest \
         semantics failure); got {status} body={body}",
    );
}

// ── #454: DELETE /v1/plugins/:id ────────────────────────────────────────────

/// Operator token must get 403 on delete. Pre-fix, any authenticated
/// user could DoS a tenant by uninstalling a plugin their tenant
/// depended on.
#[tokio::test]
async fn delete_plugin_refused_for_non_admin_operator() {
    let h = LiveHarness::setup().await;
    // Seed a plugin as admin first so the id exists (the delete path
    // returns 404 for unknown ids BEFORE the role guard in some
    // handler shapes; we want the guard to fire first, so we make the
    // id real).
    let create = h
        .client()
        .post(format!("{}/v1/plugins", h.base_url))
        .bearer_auth(&h.admin_token)
        .json(&manifest("rbac-test.delete-denied"))
        .send()
        .await
        .expect("admin create reaches server");
    // Tolerate 201 or 400 — see the create-for-admin test for the
    // rationale. Either way the registry entry is present OR the admin
    // guard path worked and we can still assert operator 403 against
    // the id (404 is NOT an acceptable answer — that would confirm the
    // guard ran too late).
    let _ = create.status();

    let op_token = mint_operator_token(&h, "op_plugin_delete", &h.tenant).await;
    let res = h
        .client()
        .delete(format!(
            "{}/v1/plugins/{}",
            h.base_url, "rbac-test.delete-denied"
        ))
        .bearer_auth(&op_token)
        .send()
        .await
        .expect("DELETE /v1/plugins/:id reaches server");
    assert_eq!(
        res.status().as_u16(),
        403,
        "non-admin delete must be 403. body: {}",
        res.text().await.unwrap_or_default(),
    );
}

/// Baseline: missing bearer → 401.
#[tokio::test]
async fn delete_plugin_refused_without_bearer() {
    let h = LiveHarness::setup().await;

    let res = h
        .client()
        .delete(format!(
            "{}/v1/plugins/{}",
            h.base_url, "rbac-test.delete-noauth"
        ))
        .send()
        .await
        .expect("DELETE /v1/plugins/:id reaches server");
    assert_eq!(
        res.status().as_u16(),
        401,
        "no-bearer delete must be 401. body: {}",
        res.text().await.unwrap_or_default(),
    );
}

/// Admin delete still works on a seeded id.
#[tokio::test]
async fn delete_plugin_accepted_for_admin() {
    let h = LiveHarness::setup().await;
    let plugin_id = "rbac-test.delete-ok";

    // Seed via admin so there's something to delete.
    let _ = h
        .client()
        .post(format!("{}/v1/plugins", h.base_url))
        .bearer_auth(&h.admin_token)
        .json(&manifest(plugin_id))
        .send()
        .await;

    let res = h
        .client()
        .delete(format!("{}/v1/plugins/{plugin_id}", h.base_url))
        .bearer_auth(&h.admin_token)
        .send()
        .await
        .expect("DELETE /v1/plugins/:id reaches server");
    // 200/204 for happy path, 404 if the earlier create didn't land —
    // either proves the admin guard let the request through.
    let status = res.status().as_u16();
    assert!(
        matches!(status, 200 | 204 | 404),
        "admin delete must pass the role guard; got {status} body={}",
        res.text().await.unwrap_or_default(),
    );
}
