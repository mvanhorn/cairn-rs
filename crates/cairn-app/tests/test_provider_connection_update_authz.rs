//! Authorization regression coverage for the URL-path-id provider
//! connection family (PR #717 sweep).
//!
//! Eight handlers in `crates/cairn-app/src/handlers/providers.rs` and
//! `crates/cairn-app/src/bin_main/bin_providers.rs` previously trusted
//! whatever `provider_connection_id` showed up in the URL without
//! verifying the caller's tenant. PR #717's first cut fixed only
//! `update`; this sweep applies the shared
//! `load_connection_owned_by_scope` helper to every sibling.
//!
//! Per Gemini SEC-007, **cross-tenant hits collapse to 404** (not 403)
//! to avoid id enumeration: a non-admin caller cannot tell "doesn't
//! exist anywhere" apart from "exists but not yours". The body's
//! `code` field is `not_found` in both cases.
//!
//! Test matrix per handler:
//! - cross-tenant operator → **404** with `code = "not_found"`
//! - same-tenant operator → success-path status (handler-specific)
//! - admin token         → success-path status (handler-specific)
//!
//! Plus one shared `unknown_id_returns_404` test covering all six
//! mutated handlers in lockstep.

mod support;

use serde_json::{json, Value};
use support::live_fabric::LiveHarness;

// ── Helpers ────────────────────────────────────────────────────────────────

/// Mint an operator token bound to `tenant_id`. Admin-only endpoint, so
/// each test seeds via the harness's admin token before flipping to
/// the operator token for the actual mutation.
async fn mint_operator_token(h: &LiveHarness, operator_id: &str, tenant_id: &str) -> String {
    let r = h
        .client()
        .post(format!("{}/v1/auth/tokens", h.base_url))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "operator_id": operator_id,
            "tenant_id": tenant_id,
            "name": format!("provider-authz-sweep-{operator_id}"),
        }))
        .send()
        .await
        .expect("mint-operator-token reaches server");
    assert_eq!(
        r.status().as_u16(),
        201,
        "mint-operator-token must return 201, got {}: {}",
        r.status(),
        r.text().await.unwrap_or_default(),
    );
    let body: Value = r.json().await.expect("mint body json");
    body["token"]
        .as_str()
        .expect("token field present")
        .to_owned()
}

/// Ensure `h.tenant` exists in the tenants projection. LiveHarness
/// allocates the id but does not insert the row; mutating endpoints
/// like `POST /credentials` 404 until the tenant is created.
async fn ensure_tenant(h: &LiveHarness) {
    let r = h
        .client()
        .post(format!("{}/v1/admin/tenants", h.base_url))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "tenant_id": h.tenant,
            "name": format!("test-tenant-{}", h.project),
        }))
        .send()
        .await
        .expect("ensure_tenant reaches server");
    let status = r.status().as_u16();
    assert!(
        status == 201 || status == 409,
        "ensure_tenant must 201 or 409, got {status}: {}",
        r.text().await.unwrap_or_default(),
    );
}

/// Seed a credential + provider connection in `h.tenant`. Returns the
/// `provider_connection_id` so the caller can drive subsequent
/// authz-bearing requests against it.
async fn seed_connection(h: &LiveHarness, suffix: &str) -> String {
    ensure_tenant(h).await;
    let r = h
        .client()
        .post(format!(
            "{}/v1/admin/tenants/{}/credentials",
            h.base_url, h.tenant
        ))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "provider_id": "openrouter",
            "plaintext_value": format!("sk-authz-{suffix}-{}", h.project),
        }))
        .send()
        .await
        .expect("credential create reaches server");
    let status = r.status().as_u16();
    let body_str = r.text().await.unwrap_or_default();
    assert_eq!(status, 201, "credential create: {body_str}");
    let credential_id = serde_json::from_str::<Value>(&body_str)
        .expect("credential body json")
        .get("id")
        .and_then(|v| v.as_str())
        .unwrap()
        .to_owned();

    let connection_id = format!("authz_{suffix}_{}", h.project);
    let r = h
        .client()
        .post(format!("{}/v1/providers/connections", h.base_url))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "tenant_id": h.tenant,
            "provider_connection_id": connection_id,
            "provider_family": "openrouter",
            "adapter_type": "openrouter",
            "supported_models": [format!("openrouter/authz-{suffix}")],
            "credential_id": credential_id,
            "endpoint_url": "http://127.0.0.1:1",
        }))
        .send()
        .await
        .expect("create reaches server");
    assert_eq!(
        r.status().as_u16(),
        201,
        "seed connection must 201: {}",
        r.text().await.unwrap_or_default()
    );

    connection_id
}

/// Assert a response is the helper's canonical 404 — both the status
/// code and the `code` body field. The combination is what makes the
/// path safe against id enumeration: a 200/204 here would tell the
/// attacker "this id exists somewhere" even when the body is empty.
async fn assert_helper_404(label: &str, response: reqwest::Response) {
    assert_eq!(
        response.status().as_u16(),
        404,
        "{label}: expected 404 not_found from helper, got {}",
        response.status(),
    );
    let body: Value = response
        .json()
        .await
        .unwrap_or_else(|e| panic!("{label}: response body not JSON: {e}"));
    let code = body
        .get("code")
        .and_then(|v| v.as_str())
        .or_else(|| body.get("error").and_then(|v| v.as_str()));
    assert_eq!(
        code,
        Some("not_found"),
        "{label}: expected code=\"not_found\", got body {body}",
    );
}

// ── update_provider_connection_handler ─────────────────────────────────────

#[tokio::test]
async fn update_cross_tenant_returns_404_not_403() {
    // Gemini SEC-007: codex's first cut returned 403 ("provider
    // connection belongs to a different tenant") on cross-tenant. That
    // distinguishes "exists in another tenant" from "doesn't exist at
    // all", letting a foreign operator enumerate valid conn_ids by
    // diffing 403 vs 404. Helper now returns 404 for both.
    let h = LiveHarness::setup().await;
    let connection_id = seed_connection(&h, "upd_xtenant").await;

    let attacker_tenant = format!("foreign_{}", h.project);
    let attacker_token = mint_operator_token(&h, "attacker_upd", &attacker_tenant).await;

    let r = h
        .client()
        .put(format!(
            "{}/v1/providers/connections/{}",
            h.base_url, connection_id
        ))
        .bearer_auth(&attacker_token)
        .json(&json!({
            "provider_family": "openrouter",
            "adapter_type": "openrouter",
            "supported_models": ["openrouter/attacker-model"],
            "endpoint_url": "https://evil.example",
        }))
        .send()
        .await
        .expect("update reaches server");

    assert_helper_404("update cross-tenant", r).await;
}

#[tokio::test]
async fn update_same_tenant_operator_succeeds() {
    let h = LiveHarness::setup().await;
    let connection_id = seed_connection(&h, "upd_same").await;
    let operator = mint_operator_token(&h, "op_same_upd", &h.tenant).await;

    let r = h
        .client()
        .put(format!(
            "{}/v1/providers/connections/{}",
            h.base_url, connection_id
        ))
        .bearer_auth(&operator)
        .json(&json!({
            "provider_family": "openrouter",
            "adapter_type": "openrouter",
            "supported_models": ["openrouter/updated"],
            "endpoint_url": "http://127.0.0.1:1",
        }))
        .send()
        .await
        .expect("update reaches server");

    assert_eq!(
        r.status().as_u16(),
        200,
        "same-tenant operator must update its own connection: {}",
        r.text().await.unwrap_or_default(),
    );
}

#[tokio::test]
async fn update_admin_bypass_succeeds() {
    let h = LiveHarness::setup().await;
    let connection_id = seed_connection(&h, "upd_admin").await;

    let r = h
        .client()
        .put(format!(
            "{}/v1/providers/connections/{}",
            h.base_url, connection_id
        ))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "provider_family": "openrouter",
            "adapter_type": "openrouter",
            "supported_models": ["openrouter/admin-update"],
            "endpoint_url": "http://127.0.0.1:1",
        }))
        .send()
        .await
        .expect("update reaches server");

    assert_eq!(r.status().as_u16(), 200);
}

// ── delete_provider_connection_handler ─────────────────────────────────────

#[tokio::test]
async fn delete_cross_tenant_returns_404() {
    let h = LiveHarness::setup().await;
    let connection_id = seed_connection(&h, "del_xtenant").await;

    let attacker_tenant = format!("foreign_{}", h.project);
    let attacker_token = mint_operator_token(&h, "attacker_del", &attacker_tenant).await;

    let r = h
        .client()
        .delete(format!(
            "{}/v1/providers/connections/{}",
            h.base_url, connection_id
        ))
        .bearer_auth(&attacker_token)
        .send()
        .await
        .expect("delete reaches server");

    assert_helper_404("delete cross-tenant", r).await;
}

#[tokio::test]
async fn delete_same_tenant_operator_succeeds() {
    let h = LiveHarness::setup().await;
    let connection_id = seed_connection(&h, "del_same").await;
    let operator = mint_operator_token(&h, "op_same_del", &h.tenant).await;

    let r = h
        .client()
        .delete(format!(
            "{}/v1/providers/connections/{}",
            h.base_url, connection_id
        ))
        .bearer_auth(&operator)
        .send()
        .await
        .expect("delete reaches server");

    assert_eq!(
        r.status().as_u16(),
        200,
        "same-tenant operator must delete its own connection: {}",
        r.text().await.unwrap_or_default(),
    );
}

#[tokio::test]
async fn delete_admin_bypass_succeeds() {
    let h = LiveHarness::setup().await;
    let connection_id = seed_connection(&h, "del_admin").await;

    let r = h
        .client()
        .delete(format!(
            "{}/v1/providers/connections/{}",
            h.base_url, connection_id
        ))
        .bearer_auth(&h.admin_token)
        .send()
        .await
        .expect("delete reaches server");

    assert_eq!(r.status().as_u16(), 200);
}

// ── recover_provider_handler ───────────────────────────────────────────────

#[tokio::test]
async fn recover_cross_tenant_returns_404() {
    let h = LiveHarness::setup().await;
    let connection_id = seed_connection(&h, "rec_xtenant").await;

    let attacker_tenant = format!("foreign_{}", h.project);
    let attacker_token = mint_operator_token(&h, "attacker_rec", &attacker_tenant).await;

    let r = h
        .client()
        .post(format!(
            "{}/v1/providers/{}/recover",
            h.base_url, connection_id
        ))
        .bearer_auth(&attacker_token)
        .send()
        .await
        .expect("recover reaches server");

    assert_helper_404("recover cross-tenant", r).await;
}

#[tokio::test]
async fn recover_same_tenant_operator_passes_authz() {
    // Recover requires a prior health record; seed one via the
    // admin-gated manual health check. The point of this test is the
    // authz path — we assert the operator gets *past* the helper's
    // 404 (i.e. status is anything but 404 with code=not_found).
    let h = LiveHarness::setup().await;
    let connection_id = seed_connection(&h, "rec_same").await;
    let operator = mint_operator_token(&h, "op_same_rec", &h.tenant).await;

    // Seed a health record so mark_recovered's projection-readback
    // returns a row instead of "provider health not found".
    let seed = h
        .client()
        .post(format!(
            "{}/v1/providers/{}/health-check",
            h.base_url, connection_id
        ))
        .bearer_auth(&h.admin_token)
        .json(&json!({ "success": true, "latency_ms": 5 }))
        .send()
        .await
        .expect("seed health reaches server");
    assert_eq!(seed.status().as_u16(), 200);

    let r = h
        .client()
        .post(format!(
            "{}/v1/providers/{}/recover",
            h.base_url, connection_id
        ))
        .bearer_auth(&operator)
        .send()
        .await
        .expect("recover reaches server");

    assert_eq!(
        r.status().as_u16(),
        200,
        "same-tenant operator must recover its own connection: {}",
        r.text().await.unwrap_or_default(),
    );
}

#[tokio::test]
async fn recover_admin_bypass_passes_authz() {
    let h = LiveHarness::setup().await;
    let connection_id = seed_connection(&h, "rec_admin").await;

    let seed = h
        .client()
        .post(format!(
            "{}/v1/providers/{}/health-check",
            h.base_url, connection_id
        ))
        .bearer_auth(&h.admin_token)
        .json(&json!({ "success": true, "latency_ms": 5 }))
        .send()
        .await
        .expect("seed health reaches server");
    assert_eq!(seed.status().as_u16(), 200);

    let r = h
        .client()
        .post(format!(
            "{}/v1/providers/{}/recover",
            h.base_url, connection_id
        ))
        .bearer_auth(&h.admin_token)
        .send()
        .await
        .expect("recover reaches server");

    assert_eq!(r.status().as_u16(), 200);
}

// ── set_provider_health_schedule_handler ───────────────────────────────────

#[tokio::test]
async fn set_health_schedule_cross_tenant_returns_404() {
    let h = LiveHarness::setup().await;
    let connection_id = seed_connection(&h, "sched_xtenant").await;

    let attacker_tenant = format!("foreign_{}", h.project);
    let attacker_token = mint_operator_token(&h, "attacker_sched", &attacker_tenant).await;

    let r = h
        .client()
        .post(format!(
            "{}/v1/providers/connections/{}/health-schedule",
            h.base_url, connection_id
        ))
        .bearer_auth(&attacker_token)
        .json(&json!({ "interval_ms": 60_000 }))
        .send()
        .await
        .expect("set health schedule reaches server");

    assert_helper_404("set_health_schedule cross-tenant", r).await;
}

#[tokio::test]
async fn set_health_schedule_same_tenant_operator_succeeds() {
    let h = LiveHarness::setup().await;
    let connection_id = seed_connection(&h, "sched_same").await;
    let operator = mint_operator_token(&h, "op_same_sched", &h.tenant).await;

    let r = h
        .client()
        .post(format!(
            "{}/v1/providers/connections/{}/health-schedule",
            h.base_url, connection_id
        ))
        .bearer_auth(&operator)
        .json(&json!({ "interval_ms": 60_000 }))
        .send()
        .await
        .expect("set health schedule reaches server");

    assert_eq!(
        r.status().as_u16(),
        200,
        "same-tenant operator must schedule own connection: {}",
        r.text().await.unwrap_or_default(),
    );
}

#[tokio::test]
async fn set_health_schedule_admin_bypass_succeeds() {
    let h = LiveHarness::setup().await;
    let connection_id = seed_connection(&h, "sched_admin").await;

    let r = h
        .client()
        .post(format!(
            "{}/v1/providers/connections/{}/health-schedule",
            h.base_url, connection_id
        ))
        .bearer_auth(&h.admin_token)
        .json(&json!({ "interval_ms": 60_000 }))
        .send()
        .await
        .expect("set health schedule reaches server");

    assert_eq!(r.status().as_u16(), 200);
}

// ── set_provider_retry_policy_handler ──────────────────────────────────────

#[tokio::test]
async fn set_retry_policy_cross_tenant_returns_404() {
    // Pre-fix the handler had `TenantScope` but stamped the caller's
    // tenant_id onto the event regardless of who owned the conn_id —
    // foreign operators could pollute the event log with phantom
    // retry-policy events targeting tenant A's conn. Helper closes it.
    let h = LiveHarness::setup().await;
    let connection_id = seed_connection(&h, "retry_xtenant").await;

    let attacker_tenant = format!("foreign_{}", h.project);
    let attacker_token = mint_operator_token(&h, "attacker_retry", &attacker_tenant).await;

    let r = h
        .client()
        .put(format!(
            "{}/v1/providers/connections/{}/retry-policy",
            h.base_url, connection_id
        ))
        .bearer_auth(&attacker_token)
        .json(&json!({
            "max_attempts": 5,
            "backoff_ms": 250,
            "retryable_error_classes": ["timeout"],
        }))
        .send()
        .await
        .expect("set retry policy reaches server");

    assert_helper_404("set_retry_policy cross-tenant", r).await;
}

#[tokio::test]
async fn set_retry_policy_same_tenant_operator_succeeds() {
    let h = LiveHarness::setup().await;
    let connection_id = seed_connection(&h, "retry_same").await;
    let operator = mint_operator_token(&h, "op_same_retry", &h.tenant).await;

    let r = h
        .client()
        .put(format!(
            "{}/v1/providers/connections/{}/retry-policy",
            h.base_url, connection_id
        ))
        .bearer_auth(&operator)
        .json(&json!({
            "max_attempts": 3,
            "backoff_ms": 100,
            "retryable_error_classes": ["network"],
        }))
        .send()
        .await
        .expect("set retry policy reaches server");

    assert_eq!(
        r.status().as_u16(),
        200,
        "same-tenant operator must set retry policy: {}",
        r.text().await.unwrap_or_default(),
    );
}

#[tokio::test]
async fn set_retry_policy_admin_bypass_succeeds() {
    let h = LiveHarness::setup().await;
    let connection_id = seed_connection(&h, "retry_admin").await;

    let r = h
        .client()
        .put(format!(
            "{}/v1/providers/connections/{}/retry-policy",
            h.base_url, connection_id
        ))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "max_attempts": 7,
            "backoff_ms": 500,
            "retryable_error_classes": ["throttle"],
        }))
        .send()
        .await
        .expect("set retry policy reaches server");

    assert_eq!(r.status().as_u16(), 200);
}

// ── resolve_provider_key_handler ───────────────────────────────────────────

#[tokio::test]
async fn resolve_provider_key_cross_tenant_returns_404() {
    // Pre-fix this handler distinguished 200 (key linked) / 404
    // (no_credential) / 410 (revoked) by conn_id alone. A foreign
    // operator could probe a tenant's credential-binding state by
    // diffing those status codes. Helper collapses the cross-tenant
    // path into the same 404 not_found used for missing ids.
    let h = LiveHarness::setup().await;
    let connection_id = seed_connection(&h, "rk_xtenant").await;

    let attacker_tenant = format!("foreign_{}", h.project);
    let attacker_token = mint_operator_token(&h, "attacker_rk", &attacker_tenant).await;

    let r = h
        .client()
        .get(format!(
            "{}/v1/providers/connections/{}/resolve-key",
            h.base_url, connection_id
        ))
        .bearer_auth(&attacker_token)
        .send()
        .await
        .expect("resolve-key reaches server");

    assert_helper_404("resolve_provider_key cross-tenant", r).await;
}

#[tokio::test]
async fn resolve_provider_key_same_tenant_operator_resolves() {
    let h = LiveHarness::setup().await;
    let connection_id = seed_connection(&h, "rk_same").await;
    let operator = mint_operator_token(&h, "op_same_rk", &h.tenant).await;

    let r = h
        .client()
        .get(format!(
            "{}/v1/providers/connections/{}/resolve-key",
            h.base_url, connection_id
        ))
        .bearer_auth(&operator)
        .send()
        .await
        .expect("resolve-key reaches server");

    // `seed_connection` binds a credential, so success here returns
    // 200 with `has_key=true`. Anything other than 404 not_found
    // would mean the helper let us through; we additionally pin the
    // happy path to catch a future regression that breaks the
    // resolve-key projection.
    assert_eq!(
        r.status().as_u16(),
        200,
        "same-tenant operator must resolve own key: {}",
        r.text().await.unwrap_or_default(),
    );
    let body: Value = r.json().await.unwrap();
    assert_eq!(body["has_key"].as_bool(), Some(true));
}

#[tokio::test]
async fn resolve_provider_key_admin_bypass_resolves() {
    let h = LiveHarness::setup().await;
    let connection_id = seed_connection(&h, "rk_admin").await;

    let r = h
        .client()
        .get(format!(
            "{}/v1/providers/connections/{}/resolve-key",
            h.base_url, connection_id
        ))
        .bearer_auth(&h.admin_token)
        .send()
        .await
        .expect("resolve-key reaches server");

    assert_eq!(r.status().as_u16(), 200);
}

// ── discover_models_handler (bin_providers.rs) ─────────────────────────────

#[tokio::test]
async fn discover_models_cross_tenant_returns_404() {
    // The most dangerous of the family: pre-fix, a foreign operator
    // could pass another tenant's conn_id and steer
    // `resolve_connection_probe_material` into proxying that tenant's
    // stored API key out to an attacker-controlled `endpoint_url`.
    // Cross-tenant must collapse to 404 with no upstream call made.
    let h = LiveHarness::setup().await;
    let connection_id = seed_connection(&h, "disc_xtenant").await;

    let attacker_tenant = format!("foreign_{}", h.project);
    let attacker_token = mint_operator_token(&h, "attacker_disc", &attacker_tenant).await;

    let r = h
        .client()
        .get(format!(
            "{}/v1/providers/connections/{}/discover-models",
            h.base_url, connection_id
        ))
        .bearer_auth(&attacker_token)
        .send()
        .await
        .expect("discover-models reaches server");

    // discover_models has its own envelope shape (`{error, hint}`)
    // rather than the canonical AppApiError. The status is what
    // matters for enumeration safety.
    assert_eq!(
        r.status().as_u16(),
        404,
        "discover-models cross-tenant must 404, got {}: {}",
        r.status(),
        r.text().await.unwrap_or_default(),
    );
}

#[tokio::test]
async fn discover_models_same_tenant_operator_passes_authz() {
    // The conn's `endpoint_url` is the unreachable
    // `http://127.0.0.1:1` seed. We assert that the request reaches
    // the upstream-call path (502 BAD_GATEWAY) rather than the
    // helper's 404 — i.e. authz let us through.
    let h = LiveHarness::setup().await;
    let connection_id = seed_connection(&h, "disc_same").await;
    let operator = mint_operator_token(&h, "op_same_disc", &h.tenant).await;

    let r = h
        .client()
        .get(format!(
            "{}/v1/providers/connections/{}/discover-models",
            h.base_url, connection_id
        ))
        .bearer_auth(&operator)
        .send()
        .await
        .expect("discover-models reaches server");

    assert_ne!(
        r.status().as_u16(),
        404,
        "same-tenant operator must pass discover-models authz; got 404",
    );
}

#[tokio::test]
async fn discover_models_admin_bypass_passes_authz() {
    let h = LiveHarness::setup().await;
    let connection_id = seed_connection(&h, "disc_admin").await;

    let r = h
        .client()
        .get(format!(
            "{}/v1/providers/connections/{}/discover-models",
            h.base_url, connection_id
        ))
        .bearer_auth(&h.admin_token)
        .send()
        .await
        .expect("discover-models reaches server");

    assert_ne!(r.status().as_u16(), 404);
}

// ── Unknown-id 404 (one shared test) ───────────────────────────────────────

#[tokio::test]
async fn unknown_id_returns_404_on_every_handler() {
    // Independent guarantee: an id that doesn't exist in any tenant
    // returns 404 (same envelope as the cross-tenant case) on every
    // mutated handler. This pins the `Ok(None)` arm of the helper.
    let h = LiveHarness::setup().await;
    let operator = mint_operator_token(&h, "op_unknown", &h.tenant).await;
    let unknown = format!("does_not_exist_{}", h.project);

    // PUT update
    let r = h
        .client()
        .put(format!(
            "{}/v1/providers/connections/{}",
            h.base_url, unknown
        ))
        .bearer_auth(&operator)
        .json(&json!({
            "provider_family": "openrouter",
            "adapter_type": "openrouter",
            "supported_models": [],
            "endpoint_url": "http://127.0.0.1:1",
        }))
        .send()
        .await
        .unwrap();
    assert_helper_404("update unknown-id", r).await;

    // DELETE
    let r = h
        .client()
        .delete(format!(
            "{}/v1/providers/connections/{}",
            h.base_url, unknown
        ))
        .bearer_auth(&operator)
        .send()
        .await
        .unwrap();
    assert_helper_404("delete unknown-id", r).await;

    // POST recover
    let r = h
        .client()
        .post(format!("{}/v1/providers/{}/recover", h.base_url, unknown))
        .bearer_auth(&operator)
        .send()
        .await
        .unwrap();
    assert_helper_404("recover unknown-id", r).await;

    // POST set health schedule
    let r = h
        .client()
        .post(format!(
            "{}/v1/providers/connections/{}/health-schedule",
            h.base_url, unknown
        ))
        .bearer_auth(&operator)
        .json(&json!({ "interval_ms": 60_000 }))
        .send()
        .await
        .unwrap();
    assert_helper_404("set_health_schedule unknown-id", r).await;

    // PUT retry policy
    let r = h
        .client()
        .put(format!(
            "{}/v1/providers/connections/{}/retry-policy",
            h.base_url, unknown
        ))
        .bearer_auth(&operator)
        .json(&json!({
            "max_attempts": 1,
            "backoff_ms": 1,
            "retryable_error_classes": [],
        }))
        .send()
        .await
        .unwrap();
    assert_helper_404("set_retry_policy unknown-id", r).await;

    // GET resolve-key
    let r = h
        .client()
        .get(format!(
            "{}/v1/providers/connections/{}/resolve-key",
            h.base_url, unknown
        ))
        .bearer_auth(&operator)
        .send()
        .await
        .unwrap();
    assert_helper_404("resolve_provider_key unknown-id", r).await;

    // GET discover-models — has its own envelope; only the status
    // matters for enumeration safety.
    let r = h
        .client()
        .get(format!(
            "{}/v1/providers/connections/{}/discover-models",
            h.base_url, unknown
        ))
        .bearer_auth(&operator)
        .send()
        .await
        .unwrap();
    assert_eq!(
        r.status().as_u16(),
        404,
        "discover-models unknown-id must 404 (no ?endpoint_url to fall back on)",
    );
}
