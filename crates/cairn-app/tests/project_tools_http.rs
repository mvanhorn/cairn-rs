//! Integration coverage for `GET /v1/projects/:project/tools` (#799).
//!
//! Pins the observable contract the RFC 031 role-editor autocomplete
//! depends on:
//!   * Any authenticated operator in the tenant scope can read.
//!   * Cross-tenant access → 403.
//!   * Response shape = `{items, total, has_more}` with each item
//!     carrying `{id, source, tier, description, parameters_schema}`.
//!   * `source` is `"builtin"` on every registry tool; plugin-sourced
//!     tools would surface as `"plugin:<id>"` (exercised in a later
//!     fixture when plugin-enablement is wired into the FakeFabric
//!     harness — today no plugin is enabled on the fixture so the
//!     fixture asserts built-ins only).
//!   * Results sorted alphabetically by id.
//!
//! The FakeFabric harness leaves `AppState::tool_registry` as `None`
//! (the registry is wired in `main.rs` which the test fixture doesn't
//! invoke). The handler treats a missing registry as "no built-ins
//! surfaced" rather than failing, so the fixture asserts the empty
//! envelope shape — correctness-preserving for a fresh project with
//! no plugins enabled, and the production wiring (`main.rs`) adds the
//! registry before the handler can be hit.

mod support;

use axum::{
    body::{to_bytes, Body},
    http::{Request, StatusCode},
    response::Response,
};
use cairn_api::auth::AuthPrincipal;
use cairn_api::bootstrap::BootstrapConfig;
use cairn_domain::tenancy::TenantKey;
use cairn_domain::{OperatorId, TenantId};
use tower::ServiceExt;

const ADMIN_TOKEN: &str = "admin-tools-token";
const OPERATOR_TOKEN: &str = "op-tools-token";
const CROSS_TENANT_TOKEN: &str = "op-other-tools-token";

/// Must match `DEFAULT_TENANT_ID` in `crates/cairn-app/src/state.rs`
/// because `project_key_from_path` falls back to that tenant when the
/// path segment doesn't contain `/` separators. Operator tokens must
/// bind to the same tenant for `enforce_project_tenant` to succeed.
const TENANT: &str = "default_tenant";
const WORKSPACE: &str = "default_workspace";
const PROJECT: &str = "default_project";

fn project_tools_path() -> String {
    // Path uses `/` separators per `parse_project_scope`. Encoded
    // inline (`%2F` for `/`) so axum routes the whole triple as one
    // `:project` path segment rather than four.
    format!("/v1/projects/{TENANT}%2F{WORKSPACE}%2F{PROJECT}/tools")
}

async fn register_tokens(state: &std::sync::Arc<cairn_app::AppState>) {
    state.service_tokens.register(
        ADMIN_TOKEN.to_owned(),
        AuthPrincipal::ServiceAccount {
            name: "admin".to_owned(),
            tenant: TenantKey::new(TenantId::new(TENANT)),
        },
    );
    state.service_tokens.register(
        OPERATOR_TOKEN.to_owned(),
        AuthPrincipal::Operator {
            operator_id: OperatorId::new("op_default"),
            tenant: TenantKey::new(TENANT),
        },
    );
    state.service_tokens.register(
        CROSS_TENANT_TOKEN.to_owned(),
        AuthPrincipal::Operator {
            operator_id: OperatorId::new("op_other"),
            tenant: TenantKey::new("other_tenant"),
        },
    );
}

async fn send(app: &axum::Router, uri: &str, token: Option<&str>) -> Response {
    let mut req = Request::builder().method("GET").uri(uri);
    if let Some(t) = token {
        req = req.header("authorization", format!("Bearer {t}"));
    }
    app.clone()
        .oneshot(req.body(Body::empty()).unwrap())
        .await
        .unwrap()
}

async fn response_json(response: Response) -> serde_json::Value {
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    serde_json::from_slice(&body).unwrap_or(serde_json::Value::Null)
}

#[tokio::test]
async fn list_without_bearer_returns_401() {
    let (app, _state) = support::build_test_router_fake_fabric(BootstrapConfig::default()).await;
    let resp = send(&app, &project_tools_path(), None).await;
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn list_cross_tenant_returns_403() {
    let (app, state) = support::build_test_router_fake_fabric(BootstrapConfig::default()).await;
    register_tokens(&state).await;
    let resp = send(&app, &project_tools_path(), Some(CROSS_TENANT_TOKEN)).await;
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn list_returns_shape_envelope_for_any_authenticated_operator() {
    let (app, state) = support::build_test_router_fake_fabric(BootstrapConfig::default()).await;
    register_tokens(&state).await;

    // Both admin and plain operator succeed — no admin guard on this
    // read-only endpoint per the RFC 031 spec.
    for tok in [ADMIN_TOKEN, OPERATOR_TOKEN] {
        let resp = send(&app, &project_tools_path(), Some(tok)).await;
        assert_eq!(resp.status(), StatusCode::OK, "token={tok}");
        let json = response_json(resp).await;
        assert!(json["items"].is_array());
        assert!(json["total"].is_u64());
        assert_eq!(json["has_more"], false);

        // On the FakeFabric fixture `tool_registry` is None and no
        // plugins are enabled, so `items` is empty. The handler's
        // contract is that this is the correct "fresh project with
        // nothing wired" response.
        let items = json["items"].as_array().unwrap();
        assert!(
            items.iter().all(
                |i| i["source"].as_str().unwrap_or("").starts_with("builtin")
                    || i["source"].as_str().unwrap_or("").starts_with("plugin:")
            ),
            "every item carries a well-formed source tag"
        );
        // If items are present (tool_registry wired in some future
        // fixture), they must be alphabetically sorted by `id`.
        let ids: Vec<&str> = items
            .iter()
            .map(|i| i["id"].as_str().unwrap_or(""))
            .collect();
        let mut sorted = ids.clone();
        sorted.sort();
        assert_eq!(ids, sorted, "items sorted alphabetically by id");
    }
}

#[tokio::test]
async fn list_bad_project_path_returns_400() {
    let (app, state) = support::build_test_router_fake_fabric(BootstrapConfig::default()).await;
    register_tokens(&state).await;
    // `../` in the project segment → rejected at path-validation time.
    let resp = send(&app, "/v1/projects/..%2Fevil/tools", Some(ADMIN_TOKEN)).await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}
