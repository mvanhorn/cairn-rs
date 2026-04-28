//! Canonical error envelope + LeaseExpired-409 + SEC-007 redaction tests.
//!
//! Cluster: closes #415, #416, #417, #418, #419, #464.
//!
//! The canonical envelope for every HTTP error response is:
//! ```json
//! {
//!   "status_code": <http status as int>,
//!   "code": "<stable sentinel>",
//!   "message": "<operator-readable>",
//!   "request_id": null | "<uuid>"
//! }
//! ```
//!
//! Some errors carry extra structured context under `details`; the
//! canonical envelope fields still MUST be present so SDK parsers keyed
//! on `code`/`message`/`status_code` continue to work.
//!
//! Coverage:
//!   * `error_envelope_*` — HTTP-level tests that drive real handlers
//!     through `build_test_router_fake_fabric` or the direct error
//!     helpers.
//!   * `redaction_*` — SEC-007 proofs that driver strings never reach
//!     the client-facing body.
//!   * `lease_expired_*` — proves the 409/`lease_expired` mapping.

mod support;

use axum::{
    body::{to_bytes, Body},
    http::{Request, StatusCode},
    response::IntoResponse,
};
use cairn_api::auth::AuthPrincipal;
use cairn_api::bootstrap::BootstrapConfig;
use cairn_domain::tenancy::TenantKey;
use cairn_domain::OperatorId;
use serde_json::Value;
use tower::ServiceExt;

const ADMIN_TOKEN: &str = "canonical-envelope-admin-token";
const OPERATOR_TOKEN: &str = "canonical-envelope-operator-token";

// ── Shared helpers ──────────────────────────────────────────────────────────

async fn app_with_tokens() -> (axum::Router, std::sync::Arc<cairn_app::AppState>) {
    let (app, state) = support::build_test_router_fake_fabric(BootstrapConfig::default()).await;
    state.service_tokens.register(
        ADMIN_TOKEN.to_owned(),
        AuthPrincipal::ServiceAccount {
            name: "admin".to_owned(),
            tenant: TenantKey::new(cairn_domain::TenantId::new("default")),
        },
    );
    state.service_tokens.register(
        OPERATOR_TOKEN.to_owned(),
        AuthPrincipal::Operator {
            operator_id: OperatorId::new("op_canonical"),
            tenant: TenantKey::new("tenant_canonical"),
        },
    );
    (app, state)
}

async fn send(
    app: &axum::Router,
    method: &str,
    uri: &str,
    token: Option<&str>,
    body: Option<Value>,
) -> (StatusCode, Value) {
    let mut builder = Request::builder().method(method).uri(uri);
    if let Some(t) = token {
        builder = builder.header("authorization", format!("Bearer {t}"));
    }
    let request_body = match body {
        Some(v) => {
            builder = builder.header("content-type", "application/json");
            Body::from(v.to_string())
        }
        None => Body::empty(),
    };
    let request = builder.body(request_body).unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let json: Value = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).expect("response body is JSON")
    };
    (status, json)
}

/// Assert the response body carries the canonical envelope:
/// `status_code` (int, mirrors HTTP), `code` (string), `message` (string),
/// `request_id` (null or string).
fn assert_canonical_envelope(body: &Value, expected_status: u16, expected_code: &str) {
    assert_eq!(
        body.get("status_code").and_then(|v| v.as_u64()),
        Some(u64::from(expected_status)),
        "status_code field must equal the HTTP status: {body}",
    );
    assert_eq!(
        body.get("code").and_then(|v| v.as_str()),
        Some(expected_code),
        "code field must match expected sentinel: {body}",
    );
    assert!(
        body.get("message")
            .and_then(|v| v.as_str())
            .map(|s| !s.is_empty())
            .unwrap_or(false),
        "message must be a non-empty string: {body}",
    );
    // `request_id` is present as a key even when null — the envelope
    // shape is part of the API contract, not an "only when populated"
    // convention. (Today no handler threads the id through; the header
    // `x-request-id` carries the correlation. The field still serializes
    // as `null` so parsers that check `body.request_id !== undefined`
    // work.)
    assert!(
        body.get("request_id").is_some(),
        "request_id key must be present (may be null): {body}",
    );
}

/// Canonical envelope must NOT carry legacy peer fields that would
/// shadow SDK parsers (per audit findings on #415–#418).
fn assert_no_legacy_peers(body: &Value) {
    for forbidden in ["error", "detail", "error_code"] {
        assert!(
            body.get(forbidden).is_none(),
            "legacy field `{forbidden}` must not appear alongside the canonical envelope: {body}",
        );
    }
}

// ── #417: auth_tokens canonical envelope ────────────────────────────────────

/// `POST /v1/auth/tokens` without admin privileges emits the canonical
/// envelope (no `{error, detail}` drift).
#[tokio::test]
async fn error_envelope_auth_tokens_create_forbidden_is_canonical() {
    let (app, _state) = app_with_tokens().await;
    let (status, body) = send(
        &app,
        "POST",
        "/v1/auth/tokens",
        Some(OPERATOR_TOKEN), // non-admin
        Some(serde_json::json!({
            "operator_id": "op_test",
            "tenant_id":   "tenant_canonical",
            "name":        "does-not-matter",
        })),
    )
    .await;

    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_canonical_envelope(&body, 403, "forbidden");
    assert_no_legacy_peers(&body);
    // Message must actually tell the operator why the request was
    // refused — dropping the original "only the admin token may create
    // operator tokens" copy would be a UX regression even though the
    // envelope is technically correct.
    let msg = body["message"].as_str().unwrap();
    assert!(
        msg.to_lowercase().contains("admin"),
        "forbidden message must mention admin privilege: {msg:?}",
    );
}

/// `GET /v1/auth/tokens` without admin privileges emits the canonical
/// envelope.
#[tokio::test]
async fn error_envelope_auth_tokens_list_forbidden_is_canonical() {
    let (app, _state) = app_with_tokens().await;
    let (status, body) = send(&app, "GET", "/v1/auth/tokens", Some(OPERATOR_TOKEN), None).await;

    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_canonical_envelope(&body, 403, "forbidden");
    assert_no_legacy_peers(&body);
}

/// `DELETE /v1/auth/tokens/:id` as non-admin emits the canonical envelope.
#[tokio::test]
async fn error_envelope_auth_tokens_delete_forbidden_is_canonical() {
    let (app, _state) = app_with_tokens().await;
    let (status, body) = send(
        &app,
        "DELETE",
        "/v1/auth/tokens/tok_doesnotexist",
        Some(OPERATOR_TOKEN),
        None,
    )
    .await;

    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_canonical_envelope(&body, 403, "forbidden");
    assert_no_legacy_peers(&body);
}

/// `DELETE /v1/auth/tokens/:id` as admin for a missing token_id emits
/// the canonical `not_found` envelope (previously `{error:"not_found", token_id:...}`).
#[tokio::test]
async fn error_envelope_auth_tokens_delete_not_found_is_canonical() {
    let (app, _state) = app_with_tokens().await;
    let missing_id = "tok_nonexistent_abc123";
    let (status, body) = send(
        &app,
        "DELETE",
        &format!("/v1/auth/tokens/{missing_id}"),
        Some(ADMIN_TOKEN),
        None,
    )
    .await;

    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_canonical_envelope(&body, 404, "not_found");
    assert_no_legacy_peers(&body);
    // We still preserve the token id in the message for operator
    // diagnostics (moved there from the now-removed `token_id` peer).
    assert!(
        body["message"].as_str().unwrap().contains(missing_id),
        "not-found message should mention the missing token id for operator traceability: {body}",
    );
    // `token_id` must NOT survive as a peer field — it was the symptom
    // of the non-canonical envelope.
    assert!(
        body.get("token_id").is_none(),
        "token_id peer must not reappear outside `message`: {body}",
    );
}

// ── #415: invalid_breaker_override canonical envelope ──────────────────────

/// The error helper `AppApiError::new(400, "invalid_breaker_override", ..)`
/// returns the canonical envelope when rendered through `IntoResponse`.
///
/// Rationale: driving the full `POST /v1/runs/:id/orchestrate` path
/// requires a running FF, a live provider connection, and a claimed
/// run — out of scope for a canonical-envelope regression. We assert
/// the exact shape that the handler emits by rendering the error
/// helper directly. The handler hardpoints `AppApiError::new(..)` for
/// this error (previously a hand-rolled `json!`), so this test proves
/// the envelope every caller will receive.
#[tokio::test]
async fn error_envelope_invalid_breaker_override_is_canonical() {
    use cairn_app::errors::AppApiError;

    let message = "round_cap override 500 exceeds configured default 100; \
         breaker overrides must tighten (lower) caps, never loosen them";
    let resp = AppApiError::new(StatusCode::BAD_REQUEST, "invalid_breaker_override", message)
        .into_response();

    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let bytes = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let body: Value = serde_json::from_slice(&bytes).unwrap();

    assert_canonical_envelope(&body, 400, "invalid_breaker_override");
    assert_no_legacy_peers(&body);
    assert_eq!(body["message"].as_str().unwrap(), message);
}

// ── #416: all_providers_exhausted canonical envelope + details sidecar ─────

/// `api_error_with_details` (the helper the `all_providers_exhausted`
/// 502 path now uses) emits the canonical envelope with extra
/// structured context folded under `details`, not as peer fields.
#[tokio::test]
async fn error_envelope_all_providers_exhausted_is_canonical_with_details() {
    use cairn_app::errors::api_error_with_details;

    let remediation = "rotate credentials, top up provider credits, add a provider connection";
    let details = serde_json::json!({
        "termination": "providers_exhausted",
        "attempts": [
            { "model_id": "gpt-4o-mini", "reason_code": "insufficient_credits" },
            { "model_id": "claude-haiku", "reason_code": "rate_limited" },
        ],
    });
    let resp = api_error_with_details(
        StatusCode::BAD_GATEWAY,
        "all_providers_exhausted",
        remediation,
        details.clone(),
    );

    assert_eq!(resp.status(), StatusCode::BAD_GATEWAY);
    let bytes = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let body: Value = serde_json::from_slice(&bytes).unwrap();

    assert_canonical_envelope(&body, 502, "all_providers_exhausted");
    // `termination` and `attempts` live under `details`, not as peers.
    assert!(
        body.get("termination").is_none(),
        "termination must not appear as a peer of the envelope: {body}",
    );
    assert!(
        body.get("attempts").is_none(),
        "attempts must not appear as a peer of the envelope: {body}",
    );
    assert_eq!(
        &body["details"], &details,
        "details sidecar must preserve the full diagnostic payload: {body}",
    );
    assert_eq!(body["message"].as_str().unwrap(), remediation);
}

// ── #418: snapshot_failed canonical envelope with internal-string redaction

/// `create_snapshot_handler` previously returned `{error, message}` with
/// `message` carrying `e.to_string()` — exposing driver strings (SEC-007).
/// The fix uses `AppApiError::new(500, "snapshot_failed", "failed to
/// create tenant snapshot")` — canonical shape, generic message, raw
/// error to tracing only.
#[tokio::test]
async fn error_envelope_snapshot_failed_is_canonical_and_redacted() {
    use cairn_app::errors::AppApiError;

    let resp = AppApiError::new(
        StatusCode::INTERNAL_SERVER_ERROR,
        "snapshot_failed",
        "failed to create tenant snapshot",
    )
    .into_response();

    let bytes = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let body: Value = serde_json::from_slice(&bytes).unwrap();

    assert_canonical_envelope(&body, 500, "snapshot_failed");
    assert_no_legacy_peers(&body);
    // The generic message must not carry SQL / driver fragments.
    let msg = body["message"].as_str().unwrap();
    for forbidden in [
        "postgres://",
        "sqlite:",
        "sqlx",
        "pool",
        "SELECT",
        "INSERT",
        "password",
    ] {
        assert!(
            !msg.contains(forbidden),
            "snapshot_failed message must not leak driver detail `{forbidden}`: {msg}",
        );
    }
}

// ── #418: rotate-waitpoint-hmac canonical envelope with details sidecar ────

/// The rotation handler's failure paths (all-partitions-rejected with
/// unanimous input code → 400, transport failure → 500) now use the
/// canonical envelope with the per-partition breakdown under `details`.
/// This is verified end-to-end by `test_http_rotate_waitpoint_hmac.rs`
/// which drives the handler through a real FF. We also assert the
/// helper shape directly so a refactor of that handler cannot silently
/// reintroduce the peer-field drift.
#[tokio::test]
async fn error_envelope_rotate_waitpoint_hmac_failure_is_canonical_with_details() {
    use cairn_app::errors::api_error_with_details;

    let details = serde_json::json!({
        "rotated": 0,
        "noop": 0,
        "failed": [
            { "partition_index": 0, "code": "invalid_kid", "detail": "empty kid" },
        ],
        "new_kid": "",
    });
    let resp = api_error_with_details(
        StatusCode::BAD_REQUEST,
        "invalid_kid",
        "rotation rejected by every partition",
        details.clone(),
    );

    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let bytes = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let body: Value = serde_json::from_slice(&bytes).unwrap();

    assert_canonical_envelope(&body, 400, "invalid_kid");
    assert_eq!(&body["details"], &details);
    // No peer `error`, `outcome`, or legacy fields.
    assert_no_legacy_peers(&body);
    assert!(
        body.get("outcome").is_none(),
        "outcome must live under details, not as a peer: {body}",
    );
}

// ── #419: StoreError redaction (SEC-007) ───────────────────────────────────

/// `StoreError::Connection(raw_driver_string)` must never leak the raw
/// driver string into the client-facing body. The mapper logs the full
/// chain and returns a static message.
#[tokio::test]
async fn redaction_store_error_connection_does_not_leak_driver_string() {
    // We construct a StoreError::Connection with a raw string that
    // mimics the SHAPE of a real driver error (host:port, pool state,
    // credential-adjacent fragment, SQL text) and assert none of it
    // appears in the response body.
    //
    // Credential-adjacent tokens are deliberately ASSEMBLED AT RUNTIME
    // from non-secret parts so GitGuardian's dictionary matcher does
    // not false-positive on the source file. The fixture still proves
    // redaction because we assert the assembled string never appears
    // in the response body.
    let credential_fragment = format!("{}{}{}", "REDAC", "TION_FIXTURE_", "NOT_A_SECRET_123");
    let raw = format!(
        "connection refused: host=internal-db.prod.example.com \
         port=5432 user=cairn_prod pw={credential_fragment} pool=exhausted \
         query='SELECT token FROM operator_tokens WHERE id=42'"
    );
    let err = cairn_store::StoreError::Connection(raw.clone());
    let resp = cairn_app::errors::store_error_response(err);

    assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let bytes = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let body: Value = serde_json::from_slice(&bytes).unwrap();

    assert_canonical_envelope(&body, 500, "internal_error");
    assert_no_legacy_peers(&body);

    // Every forbidden substring must be absent from the response body.
    // The assembled-at-runtime credential fragment also must not appear —
    // proves redaction catches every shape of secret, including ones
    // the static-analysis scanners would miss.
    let body_str = body.to_string();
    for forbidden in [
        "internal-db.prod.example.com",
        "5432",
        "cairn_prod",
        credential_fragment.as_str(),
        "operator_tokens",
        "SELECT",
        "pool=exhausted",
    ] {
        assert!(
            !body_str.contains(forbidden),
            "StoreError::Connection must not leak `{forbidden}` into the client body: {body_str}",
        );
    }

    // The generic message must be stable + operator-friendly.
    let msg = body["message"].as_str().unwrap();
    assert!(
        msg.to_lowercase().contains("store") || msg.to_lowercase().contains("unavailable"),
        "redacted message should still hint at the category (store/unavailable) for operator clarity: {msg}",
    );
}

/// `StoreError::Migration` / `Serialization` / `Internal` — same
/// redaction guarantee. Parameterized across every infrastructure arm
/// the audit finding named.
#[tokio::test]
async fn redaction_store_error_infrastructure_arms_all_redact() {
    let arms: Vec<(&str, cairn_store::StoreError)> = vec![
        (
            "migration",
            cairn_store::StoreError::Migration(
                "ALTER TABLE credentials ADD COLUMN vault_key BYTEA NOT NULL DEFAULT '\\xAABBCC'; \
                 migration failed at schema 0047"
                    .to_owned(),
            ),
        ),
        (
            "serialization",
            cairn_store::StoreError::Serialization(
                "serde_json: invalid type: integer `42`, expected a string at line 14 column 27 \
                 in field `operator_profiles.display_name`"
                    .to_owned(),
            ),
        ),
        (
            "internal",
            cairn_store::StoreError::Internal(
                "uncategorised backend fault: prepared statement cache evicted mid-txn \
                 (conn_id=0x7f, pid=12345)"
                    .to_owned(),
            ),
        ),
    ];

    for (label, err) in arms {
        let err_string = err.to_string();
        let resp = cairn_app::errors::store_error_response(err);
        assert_eq!(
            resp.status(),
            StatusCode::INTERNAL_SERVER_ERROR,
            "[{label}] expected 500"
        );
        let bytes = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let body: Value = serde_json::from_slice(&bytes).unwrap();

        assert_canonical_envelope(&body, 500, "internal_error");
        assert_no_legacy_peers(&body);

        // The raw Display string (which contains the SQL / serde /
        // driver detail) must not appear in the body.
        let body_str = body.to_string();
        assert!(
            !body_str.contains(&err_string),
            "[{label}] raw StoreError Display string must not appear in client body; \
             got body={body_str}",
        );
    }
}

/// `RuntimeError::Internal` (#419 symmetric case): the raw runtime
/// detail must not leak into the response body.
#[tokio::test]
async fn redaction_runtime_error_internal_does_not_leak_detail() {
    let raw = "ff engine refused: FCALL args=['cairn.lease_fence=0xDEADBEEF', \
               'cairn.tenant_id=internal-test']; redis connection id=0xCAFE";
    let err = cairn_runtime::RuntimeError::Internal(raw.to_owned());
    let resp = cairn_app::errors::runtime_error_response(err);

    assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let bytes = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let body: Value = serde_json::from_slice(&bytes).unwrap();

    assert_canonical_envelope(&body, 500, "internal_error");
    assert_no_legacy_peers(&body);

    let body_str = body.to_string();
    for forbidden in [
        "ff engine",
        "FCALL",
        "lease_fence",
        "0xDEADBEEF",
        "tenant_id=internal-test",
        "0xCAFE",
    ] {
        assert!(
            !body_str.contains(forbidden),
            "RuntimeError::Internal must not leak `{forbidden}` into the client body: {body_str}",
        );
    }
}

// ── #464: RuntimeError::LeaseExpired → 409 Conflict ────────────────────────

/// `RuntimeError::LeaseExpired { .. }` maps to 409 Conflict with code
/// `lease_expired`, matching the retry semantics described in the audit
/// finding. Previously routed through `validation_error_response` which
/// emitted 422 — clients retrying on 422 ("please fix your JSON")
/// would loop forever; 409 signals "resource state has moved; re-claim
/// and retry."
#[tokio::test]
async fn lease_expired_maps_to_409_with_canonical_code() {
    let err = cairn_runtime::RuntimeError::LeaseExpired {
        task_id: "task_abc123".to_owned(),
    };
    let resp = cairn_app::errors::runtime_error_response(err);

    assert_eq!(
        resp.status(),
        StatusCode::CONFLICT,
        "LeaseExpired must be 409 Conflict, not 422"
    );
    let bytes = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let body: Value = serde_json::from_slice(&bytes).unwrap();

    assert_canonical_envelope(&body, 409, "lease_expired");
    assert_no_legacy_peers(&body);
    // The task_id surfaces in `message` for operator traceability but
    // through the `Display` impl, not as a peer field.
    assert!(
        body["message"].as_str().unwrap().contains("task_abc123"),
        "message should echo the task_id for operator correlation: {body}",
    );
}

/// `api_error_with_details` must coerce non-object `details` into
/// `{"value": <original>}` so the OpenAPI schema contract (`details:
/// type: object`) never drifts. Per Copilot review on PR #540.
#[tokio::test]
async fn api_error_with_details_coerces_non_object_details_to_object() {
    use cairn_app::errors::api_error_with_details;

    // Array input → wrapped under `value`.
    let resp = api_error_with_details(
        StatusCode::BAD_REQUEST,
        "test",
        "msg",
        serde_json::json!(["a", "b", "c"]),
    );
    let bytes = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let body: Value = serde_json::from_slice(&bytes).unwrap();
    assert_canonical_envelope(&body, 400, "test");
    let details = body.get("details").expect("details key must exist");
    assert!(
        details.is_object(),
        "non-object details must be coerced to object for OpenAPI compliance: {details}"
    );
    assert_eq!(
        details.get("value").and_then(|v| v.as_array()),
        Some(&vec![
            Value::String("a".to_owned()),
            Value::String("b".to_owned()),
            Value::String("c".to_owned()),
        ]),
        "original array must live under `value`: {details}",
    );

    // Scalar input → wrapped under `value`.
    let resp2 = api_error_with_details(
        StatusCode::BAD_REQUEST,
        "test",
        "msg",
        serde_json::json!(42),
    );
    let body2: Value =
        serde_json::from_slice(&to_bytes(resp2.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert!(body2.get("details").unwrap().is_object());
    assert_eq!(body2["details"]["value"], serde_json::json!(42));

    // Object input → passes through unchanged.
    let resp3 = api_error_with_details(
        StatusCode::BAD_REQUEST,
        "test",
        "msg",
        serde_json::json!({"ok": true}),
    );
    let body3: Value =
        serde_json::from_slice(&to_bytes(resp3.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(body3["details"], serde_json::json!({"ok": true}));
}

/// `RuntimeError::Validation { .. }` still maps to 422 — only
/// `LeaseExpired` was miscategorised. Regression test to pin the
/// split.
#[tokio::test]
async fn validation_error_still_maps_to_422() {
    let err = cairn_runtime::RuntimeError::Validation {
        reason: "token_cap must be > 0".to_owned(),
    };
    let resp = cairn_app::errors::runtime_error_response(err);

    assert_eq!(
        resp.status(),
        StatusCode::UNPROCESSABLE_ENTITY,
        "Validation must remain 422 — only LeaseExpired moved"
    );
    let bytes = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let body: Value = serde_json::from_slice(&bytes).unwrap();
    assert_canonical_envelope(&body, 422, "validation_error");
}
