//! Handler-level security regression tests for META #461.
//!
//! The primitive-level tests live in
//! `crates/cairn-runtime/tests/credential_encryption_security.rs` — those
//! cover key material, nonce uniqueness, and the pre-fix format scanner.
//! This file drives the HTTP layer because the cluster includes ACL
//! changes on `list_credentials_handler` (#447) and a request-body
//! redaction invariant on `StoreCredentialRequest` (#492) that only
//! surface at the handler boundary.
//!
//! All three tests run against the in-memory store + FakeFabric fixture
//! (no Valkey, no Postgres) so they execute in any CI environment and on
//! every backend the workspace supports.

mod support;

use axum::{
    body::{to_bytes, Body},
    http::{Request, StatusCode},
    response::Response,
};
use cairn_api::auth::AuthPrincipal;
use cairn_api::bootstrap::{BootstrapConfig, EncryptionKeySource};
use cairn_domain::tenancy::TenantKey;
use cairn_domain::{OperatorId, TenantId};
use cairn_runtime::tenants::TenantService;
use tower::ServiceExt;

const ADMIN_TOKEN: &str = "admin-test-token";
const OPERATOR_A_TOKEN: &str = "op-a-token";
const OPERATOR_B_TOKEN: &str = "op-b-token";

fn team_config_with_env_key() -> BootstrapConfig {
    BootstrapConfig {
        encryption_key: EncryptionKeySource::EnvVar {
            var_name: "CAIRN_CREDENTIAL_KEY".to_owned(),
        },
        ..BootstrapConfig::default()
    }
}

async fn register_admin_and_operators(state: &std::sync::Arc<cairn_app::AppState>) {
    state.service_tokens.register(
        ADMIN_TOKEN.to_owned(),
        AuthPrincipal::ServiceAccount {
            name: "admin".to_owned(),
            tenant: TenantKey::new(TenantId::new("default")),
        },
    );
    state.service_tokens.register(
        OPERATOR_A_TOKEN.to_owned(),
        AuthPrincipal::Operator {
            operator_id: OperatorId::new("op_a"),
            tenant: TenantKey::new("tenant_a"),
        },
    );
    state.service_tokens.register(
        OPERATOR_B_TOKEN.to_owned(),
        AuthPrincipal::Operator {
            operator_id: OperatorId::new("op_b"),
            tenant: TenantKey::new("tenant_b"),
        },
    );

    // Create the tenants so the credential service doesn't bounce calls
    // with `tenant not found`. The FakeFabric fixture is read-only for
    // runs/tasks/sessions but the tenant service uses the shared
    // InMemoryStore directly.
    state
        .runtime
        .tenants
        .create(TenantId::new("tenant_a"), "A".to_owned())
        .await
        .unwrap();
    state
        .runtime
        .tenants
        .create(TenantId::new("tenant_b"), "B".to_owned())
        .await
        .unwrap();
}

async fn send_get(app: &axum::Router, uri: &str, token: &str) -> Response {
    app.clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(uri)
                .header("authorization", format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap()
}

async fn send_post_json(
    app: &axum::Router,
    uri: &str,
    token: &str,
    body: serde_json::Value,
) -> Response {
    app.clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(uri)
                .header("authorization", format!("Bearer {token}"))
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap()
}

async fn send_put_json(
    app: &axum::Router,
    uri: &str,
    token: &str,
    body: serde_json::Value,
) -> Response {
    app.clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(uri)
                .header("authorization", format!("Bearer {token}"))
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap()
}

async fn response_json(response: Response) -> serde_json::Value {
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    serde_json::from_slice(&body).unwrap()
}

// ── #447: list_credentials ACL ────────────────────────────────────────────────

/// Cross-tenant credential list is a 404, admin list sees every tenant.
///
/// Pre-fix: `list_credentials_handler` had no `AdminRoleGuard` and no
/// `TenantScope` extractor. Any authenticated caller could enumerate
/// any tenant's credential metadata, leaking provider_ids, credential_ids,
/// key_ids, and timestamps that are valuable staging material for a
/// targeted attack. The fix adds a `TenantScope` guard that:
///   - Admin callers pass through untouched (cross-tenant list is
///     a legitimate admin operation — the audit log endpoint has the
///     same shape).
///   - Non-admin callers get 404 on mismatch; 404 (not 403) so we don't
///     confirm tenant-id existence to an unauthorized caller.
#[tokio::test]
async fn list_credentials_rejects_cross_tenant_non_admin() {
    let (app, state) = support::build_test_router_fake_fabric(team_config_with_env_key()).await;
    register_admin_and_operators(&state).await;

    // Admin seeds a credential for tenant_a.
    let create_resp = send_post_json(
        &app,
        "/v1/admin/tenants/tenant_a/credentials",
        ADMIN_TOKEN,
        serde_json::json!({
            "provider_id": "openai",
            "plaintext_value": "sk-tenant-a-secret",
            "key_id": "key-a"
        }),
    )
    .await;
    assert_eq!(create_resp.status(), StatusCode::CREATED);

    // Operator B attempts to list tenant_a's credentials → 404.
    let cross_resp = send_get(
        &app,
        "/v1/admin/tenants/tenant_a/credentials",
        OPERATOR_B_TOKEN,
    )
    .await;
    assert_eq!(
        cross_resp.status(),
        StatusCode::NOT_FOUND,
        "non-admin cross-tenant list must be 404 (not 403, no existence disclosure)"
    );

    // Operator A listing their own tenant: OK with 1 item.
    let self_resp = send_get(
        &app,
        "/v1/admin/tenants/tenant_a/credentials",
        OPERATOR_A_TOKEN,
    )
    .await;
    assert_eq!(self_resp.status(), StatusCode::OK);
    let body = response_json(self_resp).await;
    assert_eq!(
        body["items"].as_array().map(|a| a.len()).unwrap_or(0),
        1,
        "tenant owner can list their own tenant's credentials"
    );

    // Admin listing tenant_a: still sees the credential.
    let admin_resp = send_get(&app, "/v1/admin/tenants/tenant_a/credentials", ADMIN_TOKEN).await;
    assert_eq!(admin_resp.status(), StatusCode::OK);
    let admin_body = response_json(admin_resp).await;
    assert_eq!(
        admin_body["items"].as_array().map(|a| a.len()).unwrap_or(0),
        1,
        "admin retains cross-tenant list capability"
    );
}

// ── #492: Plaintext is not carried in Debug / request logs ───────────────────
//
// The Debug-redaction invariant is pinned in the in-crate unit test at
// `crates/cairn-app/src/handlers/admin.rs::tests::store_credential_request_debug_redacts_plaintext`
// (runs against the crate-private struct). This file covers the observable
// tails that the unit test can't reach: error responses and the
// request-log ring buffer must not carry the plaintext.

/// Error responses from a failing store call do not include the plaintext.
///
/// This is the behavioral invariant #492 cares about: even when the
/// store path errors, the plaintext MUST NOT appear anywhere in the
/// response body. A bad tenant_id is the cheapest trigger — the
/// service returns `RuntimeError::NotFound { entity: "tenant", .. }`
/// before ever touching the ciphertext path.
#[tokio::test]
async fn store_credential_error_response_does_not_include_plaintext() {
    let (app, state) = support::build_test_router_fake_fabric(team_config_with_env_key()).await;
    register_admin_and_operators(&state).await;

    let secret = "sk-MUST-NOT-LEAK-IN-ERROR-RESPONSE";
    // Deliberately hit a non-existent tenant so the store path errors.
    let resp = send_post_json(
        &app,
        "/v1/admin/tenants/tenant_ghost/credentials",
        ADMIN_TOKEN,
        serde_json::json!({
            "provider_id": "openai",
            "plaintext_value": secret,
            "key_id": "primary"
        }),
    )
    .await;
    assert_eq!(
        resp.status(),
        StatusCode::NOT_FOUND,
        "store for non-existent tenant returns NotFound"
    );
    let body_bytes = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let body_str = std::str::from_utf8(&body_bytes).unwrap_or("");
    assert!(
        !body_str.contains(secret),
        "response body must not include the plaintext secret; got: {body_str}"
    );
}

#[tokio::test]
async fn create_provider_connection_rejects_foreign_tenant_credential() {
    let (app, state) = support::build_test_router_fake_fabric(team_config_with_env_key()).await;
    register_admin_and_operators(&state).await;

    let create_cred = send_post_json(
        &app,
        "/v1/admin/tenants/tenant_a/credentials",
        ADMIN_TOKEN,
        serde_json::json!({
            "provider_id": "openai",
            "plaintext_value": "sk-tenant-a-secret",
            "key_id": "key-a",
        }),
    )
    .await;
    assert_eq!(create_cred.status(), StatusCode::CREATED);
    let credential_id = response_json(create_cred).await["id"]
        .as_str()
        .unwrap()
        .to_owned();

    let create_conn = send_post_json(
        &app,
        "/v1/providers/connections",
        OPERATOR_B_TOKEN,
        serde_json::json!({
            "tenant_id": "tenant_b",
            "provider_connection_id": "conn-tenant-b",
            "provider_family": "openai_compat",
            "adapter_type": "openai_compat",
            "credential_id": credential_id,
            "endpoint_url": "http://localhost:11434"
        }),
    )
    .await;
    assert_eq!(create_conn.status(), StatusCode::FORBIDDEN);
    let body = response_json(create_conn).await;
    assert_eq!(body["code"], "credential_tenant_mismatch");
}

/// QA-of-#722: the same-tenant honest case (operator B in tenant_b
/// linking a tenant_b credential to a tenant_b connection) MUST
/// still succeed. Without this positive assertion, a future
/// over-strict tightening could lock everyone out and the test
/// suite wouldn't catch it.
#[tokio::test]
async fn create_provider_connection_accepts_same_tenant_credential() {
    let (app, state) = support::build_test_router_fake_fabric(team_config_with_env_key()).await;
    register_admin_and_operators(&state).await;

    let create_cred = send_post_json(
        &app,
        "/v1/admin/tenants/tenant_b/credentials",
        ADMIN_TOKEN,
        serde_json::json!({
            "provider_id": "openai",
            "plaintext_value": "sk-tenant-b-secret",
            "key_id": "key-b",
        }),
    )
    .await;
    assert_eq!(create_cred.status(), StatusCode::CREATED);
    let credential_id = response_json(create_cred).await["id"]
        .as_str()
        .unwrap()
        .to_owned();

    let create_conn = send_post_json(
        &app,
        "/v1/providers/connections",
        OPERATOR_B_TOKEN,
        serde_json::json!({
            "tenant_id": "tenant_b",
            "provider_connection_id": "conn-tenant-b-same",
            "provider_family": "openai_compat",
            "adapter_type": "openai_compat",
            "credential_id": credential_id,
            "endpoint_url": "http://localhost:11434"
        }),
    )
    .await;
    assert_eq!(
        create_conn.status(),
        StatusCode::CREATED,
        "honest same-tenant link must still succeed"
    );
}

/// QA-of-#722: the residual exploit codex's first cut left open.
/// Operator B authenticates as tenant_b but passes
/// `body.tenant_id = "tenant_a"` and a tenant_a credential id.
/// `validate_credential_belongs_to_tenant` is happy
/// (tenant-id-as-claimed matches tenant-id-on-credential), so
/// without an auth-derived scope check the link is accepted and
/// the /test probe later exfiltrates tenant_a's plaintext. The new
/// `tenant_scope_mismatch` 403 closes the hole.
#[tokio::test]
async fn create_provider_connection_rejects_body_tenant_spoof() {
    let (app, state) = support::build_test_router_fake_fabric(team_config_with_env_key()).await;
    register_admin_and_operators(&state).await;

    let create_cred = send_post_json(
        &app,
        "/v1/admin/tenants/tenant_a/credentials",
        ADMIN_TOKEN,
        serde_json::json!({
            "provider_id": "openai",
            "plaintext_value": "sk-tenant-a-secret",
            "key_id": "key-a",
        }),
    )
    .await;
    assert_eq!(create_cred.status(), StatusCode::CREATED);
    let credential_id = response_json(create_cred).await["id"]
        .as_str()
        .unwrap()
        .to_owned();

    // Operator B (tenant_b) lies about the tenant in the body and
    // tries to link tenant_a's credential.
    let create_conn = send_post_json(
        &app,
        "/v1/providers/connections",
        OPERATOR_B_TOKEN,
        serde_json::json!({
            "tenant_id": "tenant_a",
            "provider_connection_id": "conn-spoofed",
            "provider_family": "openai_compat",
            "adapter_type": "openai_compat",
            "credential_id": credential_id,
            "endpoint_url": "http://attacker.example/probe"
        }),
    )
    .await;
    assert_eq!(create_conn.status(), StatusCode::FORBIDDEN);
    let body = response_json(create_conn).await;
    assert_eq!(body["code"], "tenant_scope_mismatch");
}

/// QA-of-#722: codex's PR also added validation to the update path
/// but the regression test only covered create. Lock down the
/// update path with the same scenario shape.
#[tokio::test]
async fn update_provider_connection_rejects_foreign_tenant_credential() {
    let (app, state) = support::build_test_router_fake_fabric(team_config_with_env_key()).await;
    register_admin_and_operators(&state).await;

    // Create a clean tenant_b connection with a tenant_b credential.
    let cred_b = send_post_json(
        &app,
        "/v1/admin/tenants/tenant_b/credentials",
        ADMIN_TOKEN,
        serde_json::json!({
            "provider_id": "openai",
            "plaintext_value": "sk-tenant-b",
            "key_id": "key-b",
        }),
    )
    .await;
    assert_eq!(cred_b.status(), StatusCode::CREATED);
    let credential_b = response_json(cred_b).await["id"]
        .as_str()
        .unwrap()
        .to_owned();

    let create_conn = send_post_json(
        &app,
        "/v1/providers/connections",
        OPERATOR_B_TOKEN,
        serde_json::json!({
            "tenant_id": "tenant_b",
            "provider_connection_id": "conn-update-target",
            "provider_family": "openai_compat",
            "adapter_type": "openai_compat",
            "credential_id": credential_b,
            "endpoint_url": "http://localhost:11434"
        }),
    )
    .await;
    let create_status = create_conn.status();
    let create_body = response_json(create_conn).await;
    assert_eq!(
        create_status,
        StatusCode::CREATED,
        "create failed with body {create_body}"
    );

    // tenant_a credential the attacker is trying to relink onto
    // tenant_b's connection.
    let cred_a = send_post_json(
        &app,
        "/v1/admin/tenants/tenant_a/credentials",
        ADMIN_TOKEN,
        serde_json::json!({
            "provider_id": "openai",
            "plaintext_value": "sk-tenant-a",
            "key_id": "key-a",
        }),
    )
    .await;
    assert_eq!(cred_a.status(), StatusCode::CREATED);
    let credential_a = response_json(cred_a).await["id"]
        .as_str()
        .unwrap()
        .to_owned();

    // PUT with tenant_a's credential id should be rejected because
    // the connection itself is owned by tenant_b. The request body
    // for PUT requires the full config; only `credential_id` is
    // changing for the attack scenario.
    let update = send_put_json(
        &app,
        "/v1/providers/connections/conn-update-target",
        OPERATOR_B_TOKEN,
        serde_json::json!({
            "provider_family": "openai_compat",
            "adapter_type": "openai_compat",
            "supported_models": [],
            "credential_id": credential_a,
        }),
    )
    .await;
    let update_status = update.status();
    let update_body = response_json(update).await;
    assert_eq!(
        update_status,
        StatusCode::FORBIDDEN,
        "expected 403, got {update_status} with body {update_body}"
    );
    assert_eq!(update_body["code"], "credential_tenant_mismatch");
}

/// Also assert the plaintext does not end up in the request-log ring
/// buffer. The observability middleware captures method/path/query but
/// not body — this test pins that contract so if the middleware ever
/// starts buffering bodies, somebody has to update this test AND the
/// security review.
#[tokio::test]
async fn store_credential_request_body_not_in_request_log() {
    let (app, state) = support::build_test_router_fake_fabric(team_config_with_env_key()).await;
    register_admin_and_operators(&state).await;

    let secret = "sk-DO-NOT-LOG-THIS-TOKEN";
    let resp = send_post_json(
        &app,
        "/v1/admin/tenants/tenant_a/credentials",
        ADMIN_TOKEN,
        serde_json::json!({
            "provider_id": "openai",
            "plaintext_value": secret,
            "key_id": "key-a"
        }),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::CREATED);

    // Pull the request log ring buffer and assert the secret is not in
    // any captured entry (method, path, query, message).
    let log = state.request_log.read().unwrap();
    for entry in log.tail(1000, &[], None) {
        let serialized = serde_json::to_string(entry).unwrap();
        assert!(
            !serialized.contains(secret),
            "request log leaked credential plaintext in entry: {serialized}"
        );
    }
}
