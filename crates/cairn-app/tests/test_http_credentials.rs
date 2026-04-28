//! HTTP contract tests for `POST /v1/admin/tenants/:t/credentials`.
//!
//! Regression for issue #217: before the fix, posting two credentials
//! with the same `(tenant_id, provider_id)` both returned 201 and
//! silently accumulated in the tenant's credential list. Operators had
//! no way to detect the duplicate without diffing `list` responses,
//! and downstream provider-registry lookups would non-deterministically
//! bind to one of them.
//!
//! The contract after the fix:
//!   * First POST → 201 Created.
//!   * Second POST with the same `provider_id` → 409 Conflict with code
//!     `credential_exists`.
//!   * `GET …/credentials` returns exactly one record for that provider.
//!   * Revoking the first credential unblocks the rotation path: a
//!     subsequent POST for the same `provider_id` succeeds.
//!
//! # Negative-path coverage (closes #403)
//!
//! The credential endpoint is a high-value security surface — any
//! 403-miss or validation gap leaks secrets across tenants. The
//! following tests pin the expected error shapes:
//!   * operator token (non-admin) → 403.
//!   * empty `plaintext_value` → 422 `validation_error`.
//!   * oversized `plaintext_value` (>4 KiB) → 422.
//!   * empty `provider_id` → 422.
//!   * unknown tenant → 404 `not_found`.
//!   * list for a tenant with no credentials → 200 with empty `items`.

mod support;

use serde_json::{json, Value};
use support::live_fabric::LiveHarness;

#[tokio::test]
async fn duplicate_credential_for_same_provider_returns_409_and_no_silent_accumulation() {
    let h = LiveHarness::setup().await;

    // Admin token authenticates as the bootstrap tenant. The store is
    // per-subprocess so even fixed `tenant`/`provider_id` values are
    // isolated from other concurrent tests.
    let tenant = "default_tenant";
    let suffix = h.project.clone();
    let provider_id = format!("openrouter-dup-{suffix}");

    // First create — must succeed.
    let r = h
        .client()
        .post(format!(
            "{}/v1/admin/tenants/{}/credentials",
            h.base_url, tenant,
        ))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "provider_id": provider_id,
            "plaintext_value": format!("sk-first-{suffix}"),
        }))
        .send()
        .await
        .expect("first credential create reaches server");
    assert_eq!(
        r.status().as_u16(),
        201,
        "first credential create must succeed: {}",
        r.text().await.unwrap_or_default(),
    );
    let first_id = r
        .json::<Value>()
        .await
        .expect("first credential json")
        .get("id")
        .and_then(|v| v.as_str())
        .expect("first credential id")
        .to_owned();

    // Second create with the same provider_id — must 409, not 201.
    let r = h
        .client()
        .post(format!(
            "{}/v1/admin/tenants/{}/credentials",
            h.base_url, tenant,
        ))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "provider_id": provider_id,
            "plaintext_value": format!("sk-second-{suffix}"),
        }))
        .send()
        .await
        .expect("second credential create reaches server");
    let status = r.status().as_u16();
    let body_text = r.text().await.unwrap_or_default();
    assert_eq!(
        status, 409,
        "duplicate provider_id must be 409 Conflict (was 201 before #217): body={body_text}",
    );
    let body: Value = serde_json::from_str(&body_text)
        .unwrap_or_else(|e| panic!("409 body must be JSON: {e}; body={body_text}"));
    assert_eq!(
        body.get("code").and_then(|c| c.as_str()),
        Some("credential_exists"),
        "409 body must carry code=credential_exists: {body:?}",
    );
    let msg = body
        .get("message")
        .and_then(|m| m.as_str())
        .unwrap_or_default();
    assert!(
        msg.contains(&provider_id) && msg.contains(tenant),
        "409 message must name provider and tenant: {msg:?}",
    );

    // List — must contain exactly one active credential for this provider.
    let r = h
        .client()
        .get(format!(
            "{}/v1/admin/tenants/{}/credentials",
            h.base_url, tenant,
        ))
        .bearer_auth(&h.admin_token)
        .send()
        .await
        .expect("list credentials reaches server");
    assert_eq!(r.status().as_u16(), 200);
    let list: Value = r.json().await.expect("list json");
    let items = list
        .get("items")
        .and_then(|v| v.as_array())
        .expect("list items");
    let matches: Vec<&Value> = items
        .iter()
        .filter(|c| c.get("provider_id").and_then(|p| p.as_str()) == Some(&provider_id))
        .collect();
    assert_eq!(
        matches.len(),
        1,
        "exactly 1 credential must exist for provider_id={provider_id}; got {}: {items:?}",
        matches.len(),
    );
    assert_eq!(
        matches[0].get("id").and_then(|v| v.as_str()),
        Some(first_id.as_str()),
        "the surviving credential must be the first one",
    );

    // Revoke the first, then re-create — must now succeed. This protects
    // the rotate-by-revoke-then-create workflow from the uniqueness check.
    let r = h
        .client()
        .delete(format!(
            "{}/v1/admin/tenants/{}/credentials/{}",
            h.base_url, tenant, first_id,
        ))
        .bearer_auth(&h.admin_token)
        .send()
        .await
        .expect("revoke reaches server");
    assert_eq!(
        r.status().as_u16(),
        200,
        "revoke: {}",
        r.text().await.unwrap_or_default(),
    );

    let r = h
        .client()
        .post(format!(
            "{}/v1/admin/tenants/{}/credentials",
            h.base_url, tenant,
        ))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "provider_id": provider_id,
            "plaintext_value": format!("sk-third-{suffix}"),
        }))
        .send()
        .await
        .expect("post-revoke credential create reaches server");
    assert_eq!(
        r.status().as_u16(),
        201,
        "re-creating after revoke must succeed (rotate-by-revoke-then-create): {}",
        r.text().await.unwrap_or_default(),
    );
}

// ═══════════════════════════════════════════════════════════════════════════
// Negative-path tests — closes #403
// ═══════════════════════════════════════════════════════════════════════════

/// Mint an operator (non-admin) token scoped to a specific tenant.
async fn mint_operator_token(h: &LiveHarness, operator_id: &str, tenant_id: &str) -> String {
    let r = h
        .client()
        .post(format!("{}/v1/auth/tokens", h.base_url))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "operator_id": operator_id,
            "tenant_id": tenant_id,
            "name": format!("cred-neg-test-{operator_id}"),
        }))
        .send()
        .await
        .expect("mint-operator-token reaches server");
    assert_eq!(
        r.status().as_u16(),
        201,
        "operator-token mint: {}",
        r.text().await.unwrap_or_default(),
    );
    let body: Value = r.json().await.expect("mint body json");
    body["token"]
        .as_str()
        .expect("mint body must carry `token`")
        .to_owned()
}

/// POST a credential with the provided token. Returns (status, body).
async fn post_credential(
    h: &LiveHarness,
    token: &str,
    tenant: &str,
    provider_id: &str,
    plaintext: &str,
) -> (u16, Value) {
    let r = h
        .client()
        .post(format!(
            "{}/v1/admin/tenants/{}/credentials",
            h.base_url, tenant,
        ))
        .bearer_auth(token)
        .json(&json!({
            "provider_id": provider_id,
            "plaintext_value": plaintext,
        }))
        .send()
        .await
        .expect("credential-create request reaches server");
    let status = r.status().as_u16();
    let body = r.json::<Value>().await.unwrap_or(Value::Null);
    (status, body)
}

/// A non-admin operator token must not be able to store a credential
/// in ANY tenant (including its own). The AdminRoleGuard on
/// `POST /v1/admin/tenants/:t/credentials` fails closed with 403.
#[tokio::test]
async fn operator_token_cannot_store_credential_returns_403() {
    let h = LiveHarness::setup().await;
    let op_token = mint_operator_token(&h, "op_cred_neg", &h.tenant).await;

    let (status, body) = post_credential(
        &h,
        &op_token,
        &h.tenant,
        "openai-op-blocked",
        "sk-shouldfail",
    )
    .await;
    assert_eq!(
        status, 403,
        "operator token must be denied cred store with 403; body={body}"
    );
}

/// Same operator, attempting to create a credential in a DIFFERENT
/// tenant (cross-tenant). Still 403 — admin guard fires before any
/// tenant-scope check, so we don't leak whether the foreign tenant
/// exists. (If the admin guard were accidentally bypassed, the
/// cross-tenant check downstream would still reject, but 403 is the
/// correct outer code.)
#[tokio::test]
async fn operator_token_cannot_store_credential_for_other_tenant_returns_403() {
    let h = LiveHarness::setup().await;
    let op_token = mint_operator_token(&h, "op_cred_cross", &h.tenant).await;

    let foreign_tenant = "default_tenant"; // admin's bootstrap tenant; exists.
    let (status, body) = post_credential(
        &h,
        &op_token,
        foreign_tenant,
        "openai-cross-blocked",
        "sk-shouldfail",
    )
    .await;
    assert_eq!(
        status, 403,
        "operator must be 403 attempting cross-tenant credential create; body={body}"
    );
}

/// Empty `plaintext_value` must be rejected with 422 validation_error
/// — not silently accepted (which would store an unusable credential
/// row).
#[tokio::test]
async fn empty_plaintext_value_returns_422() {
    let h = LiveHarness::setup().await;
    let (status, body) = post_credential(
        &h,
        &h.admin_token,
        "default_tenant",
        &format!("openai-empty-{}", &h.project),
        "",
    )
    .await;
    assert_eq!(
        status, 422,
        "empty plaintext_value must be 422, got {status}; body={body}"
    );
    assert_eq!(
        body.get("code").and_then(|c| c.as_str()),
        Some("validation_error"),
        "empty plaintext must carry code=validation_error: {body:?}"
    );
}

/// Oversized `plaintext_value` (> 4 KiB) must be rejected with 422.
/// 4 KiB is well above every production API-key shape; larger is
/// almost certainly a mis-paste.
#[tokio::test]
async fn oversized_plaintext_value_returns_422() {
    let h = LiveHarness::setup().await;
    // 4097 bytes = 1 byte over the MAX_PLAINTEXT_VALUE_LEN cap.
    let huge = "x".repeat(4097);
    let (status, body) = post_credential(
        &h,
        &h.admin_token,
        "default_tenant",
        &format!("openai-huge-{}", &h.project),
        &huge,
    )
    .await;
    assert_eq!(
        status, 422,
        "oversized plaintext_value must be 422, got {status}; body={body}"
    );
    assert_eq!(
        body.get("code").and_then(|c| c.as_str()),
        Some("validation_error"),
        "oversized plaintext must carry code=validation_error: {body:?}"
    );
}

/// Empty `provider_id` must be rejected with 422.
#[tokio::test]
async fn empty_provider_id_returns_422() {
    let h = LiveHarness::setup().await;
    let (status, body) =
        post_credential(&h, &h.admin_token, "default_tenant", "", "sk-some-value").await;
    assert_eq!(
        status, 422,
        "empty provider_id must be 422, got {status}; body={body}"
    );
    assert_eq!(
        body.get("code").and_then(|c| c.as_str()),
        Some("validation_error"),
        "empty provider_id must carry code=validation_error: {body:?}"
    );
}

/// Unknown tenant id must return 404 `not_found` — the service
/// surfaces `RuntimeError::NotFound { entity: "tenant", .. }` which
/// maps through `runtime_error_response` to a 404. An admin token
/// calling on an unknown tenant is the canonical case: we reject
/// cleanly, do not auto-create, do not silently accept into a
/// phantom scope.
#[tokio::test]
async fn unknown_tenant_returns_404() {
    let h = LiveHarness::setup().await;
    let unknown_tenant = format!("does-not-exist-{}", &h.project);
    let (status, body) = post_credential(
        &h,
        &h.admin_token,
        &unknown_tenant,
        "openai-unknown-tenant",
        "sk-value",
    )
    .await;
    assert_eq!(
        status, 404,
        "unknown tenant must be 404, got {status}; body={body}"
    );
}

/// `GET /v1/admin/tenants/:t/credentials` on a tenant with no
/// credentials must return 200 and an empty `items` array — not 404,
/// not a nonsense empty body. Pins the negative baseline so a
/// regression that accidentally defaults to "no tenant yet" → 404
/// gets caught.
#[tokio::test]
async fn list_credentials_returns_200_empty_for_tenant_with_no_credentials() {
    let h = LiveHarness::setup().await;

    // Create a fresh tenant so we can assert emptiness deterministically.
    // Hitting `default_tenant` was a weaker check — sibling tests in the
    // same run may have populated credentials there. A uuid-scoped fresh
    // tenant guarantees items.len() == 0 (closes Copilot review round 3).
    let fresh_tenant = format!("list-empty-{}", &h.project);
    let r = h
        .client()
        .post(format!("{}/v1/admin/tenants", h.base_url))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "tenant_id": fresh_tenant,
            "name": "List-empty smoke test tenant",
        }))
        .send()
        .await
        .expect("tenant create reaches server");
    assert!(
        matches!(r.status().as_u16(), 201 | 409),
        "tenant create: {}",
        r.text().await.unwrap_or_default()
    );

    let r = h
        .client()
        .get(format!(
            "{}/v1/admin/tenants/{}/credentials",
            h.base_url, fresh_tenant,
        ))
        .bearer_auth(&h.admin_token)
        .send()
        .await
        .expect("list credentials reaches server");
    assert_eq!(
        r.status().as_u16(),
        200,
        "list on extant tenant with no creds must be 200, got {}; body={}",
        r.status(),
        r.text().await.unwrap_or_default(),
    );
    let body: Value = r.json().await.expect("list json");
    let items = body
        .get("items")
        .and_then(|v| v.as_array())
        .unwrap_or_else(|| panic!("list body must have `items` array: {body:?}"));
    assert_eq!(
        items.len(),
        0,
        "fresh tenant must list zero credentials, got {}: {body:?}",
        items.len(),
    );
}

/// Revoke-then-recreate round-trip (the #217 rotation workflow). This
/// is asserted in passing inside the duplicate-409 test but the doc
/// comment calls it out as a user-facing invariant — pin it in a
/// standalone test so a regression that accidentally retains a
/// revoked row as "active" (blocking recreation) gets named clearly.
#[tokio::test]
async fn revoke_then_recreate_same_provider_succeeds() {
    let h = LiveHarness::setup().await;
    let tenant = "default_tenant";
    let provider_id = format!("openai-rotate-{}", &h.project);

    // Create.
    let r = h
        .client()
        .post(format!(
            "{}/v1/admin/tenants/{}/credentials",
            h.base_url, tenant,
        ))
        .bearer_auth(&h.admin_token)
        .json(&json!({ "provider_id": provider_id, "plaintext_value": "sk-first" }))
        .send()
        .await
        .expect("create");
    assert_eq!(r.status().as_u16(), 201);
    let first_id = r
        .json::<Value>()
        .await
        .expect("create json")
        .get("id")
        .and_then(|v| v.as_str())
        .expect("create id")
        .to_owned();

    // Revoke.
    let r = h
        .client()
        .delete(format!(
            "{}/v1/admin/tenants/{}/credentials/{}",
            h.base_url, tenant, first_id,
        ))
        .bearer_auth(&h.admin_token)
        .send()
        .await
        .expect("revoke");
    assert_eq!(r.status().as_u16(), 200);

    // Recreate with the same provider_id — must succeed.
    let r = h
        .client()
        .post(format!(
            "{}/v1/admin/tenants/{}/credentials",
            h.base_url, tenant,
        ))
        .bearer_auth(&h.admin_token)
        .json(&json!({ "provider_id": provider_id, "plaintext_value": "sk-second" }))
        .send()
        .await
        .expect("recreate");
    assert_eq!(
        r.status().as_u16(),
        201,
        "recreate after revoke must succeed: {}",
        r.text().await.unwrap_or_default(),
    );
}
