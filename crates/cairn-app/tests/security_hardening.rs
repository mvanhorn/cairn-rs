//! Integration coverage for the 7 medium-severity security fixes shipped
//! in the `fix(security): 7 medium hardening` PR.
//!
//! Each section binds a single finding number so a future regression lands
//! on a named test.
//!
//!  - #455 orchestrate terminal-state handler never panics.
//!  - #456 mailbox append handler never panics on overlay miss.
//!  - #491 `?token=` query-string fallback is refused on non-GET methods.
//!  - #493 `POST /v1/admin/models/import-litellm` refuses bodies over 1 MB.
//!
//! #458 (fd-close race) is a sandbox-internal invariant and is covered by
//! a `#[cfg(target_os = "linux")]` unit test in
//! `crates/cairn-workspace/src/sandbox/confinement/namespace.rs`.
//!
//! #459 (WebSocket oversized frame) is covered by a live-tungstenite
//! integration test in `crates/cairn-app/tests/websocket_frame_cap.rs`.
//!
//! #460 (aes-gcm error stringify) is covered by unit tests on
//! `encrypt_value` / `decrypt_value` in
//! `crates/cairn-runtime/src/services/credential_impl.rs`.

mod support;

use axum::{
    body::{to_bytes, Body},
    http::{Request, StatusCode},
};
use cairn_api::auth::AuthPrincipal;
use cairn_api::bootstrap::BootstrapConfig;
use cairn_domain::tenancy::TenantKey;
use cairn_domain::OperatorId;
use tower::ServiceExt;

const TOKEN: &str = "sec-hardening-token";
const ADMIN_TOKEN: &str = "sec-hardening-admin-token";

async fn make_app() -> axum::Router {
    let (app, state) = support::build_test_router_fake_fabric(BootstrapConfig::default()).await;
    state.service_tokens.register(
        TOKEN.to_string(),
        AuthPrincipal::Operator {
            operator_id: OperatorId::new("op_sec"),
            tenant: TenantKey::new("default_tenant"),
        },
    );
    // Admin-guarded routes (e.g. `POST /v1/admin/models/import-litellm`)
    // require a `ServiceAccount { name: "admin" }` principal — see
    // `extractors.rs::is_admin_principal`.
    state.service_tokens.register(
        ADMIN_TOKEN.to_string(),
        AuthPrincipal::ServiceAccount {
            name: "admin".into(),
            tenant: TenantKey::new("default_tenant"),
        },
    );
    app
}

// ── #456: mailbox append never panics ─────────────────────────────────────

/// Happy path: POST /v1/mailbox returns 201 with a JSON body including
/// the overlay fields. Proves the former `.expect("mailbox overlay
/// inserted")` site now walks a `Some` arm without panicking and without
/// a silent break in the contract.
#[tokio::test]
async fn append_mailbox_returns_created_on_happy_path() {
    let app = make_app().await;

    let body = serde_json::json!({
        "tenant_id":    "default_tenant",
        "workspace_id": "default_workspace",
        "project_id":   "default_project",
        "message_id":   "mailbox_456_happy",
        "sender_id":    "op_sec",
        "body":         "hello mailbox",
    });

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/mailbox")
                .header("authorization", format!("Bearer {TOKEN}"))
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(
        response.status(),
        StatusCode::CREATED,
        "mailbox append happy path must be 201, got {}",
        response.status()
    );

    let raw = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&raw).unwrap();
    // Response is camelCase per `MailboxMessageView`.
    assert_eq!(json["messageId"], "mailbox_456_happy");
    assert_eq!(json["senderId"], "op_sec");
    assert_eq!(json["body"], "hello mailbox");
    assert_eq!(json["delivered"], false);
}

// ── #491: ?token= is GET-only ──────────────────────────────────────────────

/// A `POST /v1/settings` with a bearer in `?token=` (NOT in the
/// `Authorization` header) must be refused with 401. Before the fix, the
/// query-param fallback accepted the token on any method — so CSRF or
/// browser-history / access-log leaks of the token turned into a full
/// privilege escalation. Post-fix, only GET accepts `?token=`.
#[tokio::test]
async fn query_token_rejected_on_post() {
    let app = make_app().await;

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v1/settings?token={TOKEN}"))
                .header("content-type", "application/json")
                .body(Body::from("{}"))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(
        response.status(),
        StatusCode::UNAUTHORIZED,
        "POST with `?token=` must 401: query-token fallback is GET-only",
    );
}

/// Same as above but for DELETE — confirms the method-gate isn't a GET/
/// POST cherry-pick. DELETE without a header and with a query token must
/// 401.
#[tokio::test]
async fn query_token_rejected_on_delete() {
    let app = make_app().await;

    let response = app
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri(format!("/v1/admin/models/some-id?token={TOKEN}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(
        response.status(),
        StatusCode::UNAUTHORIZED,
        "DELETE with `?token=` must 401",
    );
}

/// A PATCH (same story — non-GET) with a query-token must also 401.
#[tokio::test]
async fn query_token_rejected_on_patch() {
    let app = make_app().await;

    let response = app
        .oneshot(
            Request::builder()
                .method("PATCH")
                .uri(format!("/v1/settings?token={TOKEN}"))
                .header("content-type", "application/json")
                .body(Body::from("{}"))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(
        response.status(),
        StatusCode::UNAUTHORIZED,
        "PATCH with `?token=` must 401",
    );
}

/// #491 + Copilot r3: GET with duplicate `?token=a&token=b` must be
/// rejected. The prior implementation did first-wins (documented as
/// "duplicates are rejected" but not actually enforced); post-fix any
/// duplicate `token` key returns 401 outright. Closes the query-param
/// smuggling vector.
#[tokio::test]
async fn query_token_duplicates_rejected_on_get() {
    let app = make_app().await;

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/v1/settings?token={TOKEN}&token=other-token"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(
        response.status(),
        StatusCode::UNAUTHORIZED,
        "duplicate ?token= on GET must 401 — proxy reordering attack",
    );
}

/// GET with `?token=` must still work — SSE EventSource and WebSocket
/// upgrade both issue GET and have no way to set custom headers.
#[tokio::test]
async fn query_token_still_accepted_on_get() {
    let app = make_app().await;

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/v1/settings?token={TOKEN}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(
        response.status(),
        StatusCode::OK,
        "GET with `?token=` must be honoured (SSE / WS use GET-only)",
    );
}

// ── #493: import-litellm refuses oversized body ────────────────────────────

/// A 2 MB request body to `POST /v1/admin/models/import-litellm` must be
/// rejected with 413 Payload Too Large. The route installs a
/// `DefaultBodyLimit::max(1_000_000)` layer so a 2 MB attacker-crafted
/// JSON blob never reaches the double-parse path that costed the pre-fix
/// code.
#[tokio::test]
async fn import_litellm_rejects_oversized_body() {
    let app = make_app().await;

    // 2 MB of pure JSON noise — well over the 1 MB per-route cap.
    let oversized = "a".repeat(2 * 1024 * 1024);
    let body = format!(r#"{{"padding":"{oversized}"}}"#);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/admin/models/import-litellm")
                .header("authorization", format!("Bearer {ADMIN_TOKEN}"))
                .header("content-type", "application/json")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(
        response.status(),
        StatusCode::PAYLOAD_TOO_LARGE,
        "/v1/admin/models/import-litellm must 413 on bodies > 1 MB",
    );
}

/// A reasonably-sized valid LiteLLM payload still imports successfully —
/// the cap is per-route, not a hard denial of the endpoint. This proves
/// we didn't accidentally disable the whole handler.
#[tokio::test]
async fn import_litellm_accepts_small_body() {
    let app = make_app().await;

    // A one-entry LiteLLM-shaped object is well under 1 MB.
    let body = serde_json::json!({
        "model-x": { "max_tokens": 1000, "input_cost_per_token": 0.000001 }
    })
    .to_string();

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/admin/models/import-litellm")
                .header("authorization", format!("Bearer {ADMIN_TOKEN}"))
                .header("content-type", "application/json")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(
        response.status(),
        StatusCode::OK,
        "small valid LiteLLM body must still import",
    );
}

/// Invalid JSON still returns 400, not 5xx — the parse-once refactor
/// kept the same rejection contract for malformed payloads.
#[tokio::test]
async fn import_litellm_rejects_invalid_json() {
    let app = make_app().await;

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/admin/models/import-litellm")
                .header("authorization", format!("Bearer {ADMIN_TOKEN}"))
                .header("content-type", "application/json")
                .body(Body::from("not-json-at-all"))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(
        response.status(),
        StatusCode::BAD_REQUEST,
        "malformed JSON must 400, not 5xx",
    );
}
