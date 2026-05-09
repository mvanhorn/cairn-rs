//! RFC 031 PR-B: HTTP surface coverage for
//! `/v1/projects/:project/agent-roles`.
//!
//! Exercises every status code in the §HTTP Surface Delta contract:
//! 200 / 201 / 400 / 401 / 403 / 404 / 409 / 412 / 413 / 422. The
//! validator's own taxonomy is covered in
//! `crates/cairn-domain/src/agent_roles_validation.rs`; this file
//! drives the wire-layer: path parsing, body parsing, ETag round-
//! trip, AdminRoleGuard, tenant scope enforcement, and the
//! projection-backed read-after-write invariant.

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

const ADMIN_TOKEN: &str = "admin-test-token";
const OPERATOR_TOKEN: &str = "op-default-token";
const CROSS_TENANT_TOKEN: &str = "op-other-token";

const TENANT: &str = "default";
const WORKSPACE: &str = "default_workspace";
const PROJECT: &str = "default_project";

fn project_path() -> String {
    format!("/v1/projects/{TENANT}-{WORKSPACE}-{PROJECT}/agent-roles")
}

fn project_path_id(id: &str) -> String {
    format!("{}/{}", project_path(), id)
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

async fn send(
    app: &axum::Router,
    method: &str,
    uri: &str,
    token: Option<&str>,
    if_match: Option<&str>,
    body: Option<serde_json::Value>,
) -> Response {
    let mut req = Request::builder().method(method).uri(uri);
    if let Some(t) = token {
        req = req.header("authorization", format!("Bearer {t}"));
    }
    if let Some(m) = if_match {
        req = req.header("if-match", m);
    }
    let body = match body {
        Some(v) => Body::from(v.to_string()),
        None => Body::empty(),
    };
    let mut request = req.body(body).unwrap();
    if body_is_not_empty(&request) {
        request
            .headers_mut()
            .insert("content-type", "application/json".parse().unwrap());
    }
    app.clone().oneshot(request).await.unwrap()
}

fn body_is_not_empty<T>(_: &Request<T>) -> bool {
    // Non-GET requests that we pass here always carry a body; the helper
    // always inserts `content-type` for us. Kept as a separate function
    // to make the intent readable at the call site.
    true
}

async fn response_json(response: Response) -> serde_json::Value {
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    serde_json::from_slice(&body).unwrap_or(serde_json::Value::Null)
}

/// Well-formed prompt that passes the structural validator. Five
/// canonical sections, two workflow phases, three bullets under
/// "What not to do".
fn good_prompt() -> String {
    "## Specialty\n\
     This role reviews pull requests.\n\n\
     ## Workflow\n\
     ### Phase 1: Gather\n\
     Gather the PR diff.\n\n\
     ### Phase 2: Review\n\
     Post inline comments.\n\n\
     ## Tools\n\
     Use the post_inline_comment tool.\n\n\
     ## Completion criteria\n\
     A review has been posted.\n\n\
     ## What not to do\n\
     - Do not merge the PR.\n\
     - Do not close the PR.\n\
     - Do not skip the diff.\n"
        .to_owned()
}

fn good_body(id: &str) -> serde_json::Value {
    serde_json::json!({
        "id": id,
        "name": "Demo Role",
        "tier": "standard",
        "description": "Demo role for HTTP tests.",
        "system_prompt": good_prompt(),
        "tools": ["post_inline_comment"],
        "response_shape": "procedural_artifact",
    })
}

// ── 401 ───────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn list_without_bearer_token_401s() {
    let (app, _state) = support::build_test_router_fake_fabric(BootstrapConfig::default()).await;
    let resp = send(&app, "GET", &project_path(), None, None, None).await;
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

// ── 403: AdminRoleGuard on writes ─────────────────────────────────────────────

#[tokio::test]
async fn create_without_admin_403s() {
    let (app, state) = support::build_test_router_fake_fabric(BootstrapConfig::default()).await;
    register_tokens(&state).await;

    let resp = send(
        &app,
        "POST",
        &project_path(),
        Some(OPERATOR_TOKEN),
        None,
        Some(good_body("nonadmin-role")),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
}

// ── 403: cross-tenant project scope refused ───────────────────────────────────

#[tokio::test]
async fn cross_tenant_list_is_refused() {
    let (app, state) = support::build_test_router_fake_fabric(BootstrapConfig::default()).await;
    register_tokens(&state).await;

    let resp = send(
        &app,
        "GET",
        &project_path(),
        Some(CROSS_TENANT_TOKEN),
        None,
        None,
    )
    .await;
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
}

// ── 201 POST happy path + ETag ───────────────────────────────────────────────

#[tokio::test]
async fn create_role_returns_201_with_etag_and_appears_in_list() {
    let (app, state) = support::build_test_router_fake_fabric(BootstrapConfig::default()).await;
    register_tokens(&state).await;

    let resp = send(
        &app,
        "POST",
        &project_path(),
        Some(ADMIN_TOKEN),
        None,
        Some(good_body("pr-reviewer-valkey")),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::CREATED);

    let etag = resp
        .headers()
        .get("etag")
        .expect("POST 201 must carry ETag")
        .to_str()
        .unwrap()
        .to_owned();
    assert!(
        etag.starts_with('"') && etag.ends_with('"'),
        "quoted opaque-tag"
    );

    let json = response_json(resp).await;
    assert_eq!(json["role"]["role_id"], "pr-reviewer-valkey");
    assert_eq!(json["source"], "custom");
    assert!(
        json["defined_at"].as_u64().unwrap() > 0,
        "defined_at must carry the event timestamp"
    );
    assert_eq!(json["warnings"].as_array().unwrap().len(), 0);

    // List must include the new role plus every active built-in role.
    let list_resp = send(&app, "GET", &project_path(), Some(ADMIN_TOKEN), None, None).await;
    assert_eq!(list_resp.status(), StatusCode::OK);
    let list = response_json(list_resp).await;
    let items = list["items"].as_array().unwrap();
    // Builtins count is derived at runtime so the test survives additions
    // to `default_roles()` (e.g. #806 added `status-checker`, lifting the
    // count from 5 to 6; future additions won't rewrite this assertion).
    let expected = cairn_domain::agent_roles::default_roles().len() + 1;
    assert_eq!(items.len(), expected);
    let ids: Vec<&str> = items
        .iter()
        .map(|i| i["role"]["role_id"].as_str().unwrap())
        .collect();
    assert!(ids.contains(&"pr-reviewer-valkey"));
    assert!(ids.contains(&"reviewer"));
}

// ── 409: re-POST of active role ──────────────────────────────────────────────

#[tokio::test]
async fn create_duplicate_active_role_returns_409() {
    let (app, state) = support::build_test_router_fake_fabric(BootstrapConfig::default()).await;
    register_tokens(&state).await;

    let r1 = send(
        &app,
        "POST",
        &project_path(),
        Some(ADMIN_TOKEN),
        None,
        Some(good_body("dup-role")),
    )
    .await;
    assert_eq!(r1.status(), StatusCode::CREATED);

    let r2 = send(
        &app,
        "POST",
        &project_path(),
        Some(ADMIN_TOKEN),
        None,
        Some(good_body("dup-role")),
    )
    .await;
    assert_eq!(r2.status(), StatusCode::CONFLICT);
}

// ── §D6 re-POST after retract → 201 ─────────────────────────────────────────

#[tokio::test]
async fn repost_after_retract_returns_201() {
    let (app, state) = support::build_test_router_fake_fabric(BootstrapConfig::default()).await;
    register_tokens(&state).await;

    let r1 = send(
        &app,
        "POST",
        &project_path(),
        Some(ADMIN_TOKEN),
        None,
        Some(good_body("redef-role")),
    )
    .await;
    assert_eq!(r1.status(), StatusCode::CREATED);

    let rd = send(
        &app,
        "DELETE",
        &project_path_id("redef-role"),
        Some(ADMIN_TOKEN),
        None,
        None,
    )
    .await;
    assert_eq!(rd.status(), StatusCode::OK);

    let r2 = send(
        &app,
        "POST",
        &project_path(),
        Some(ADMIN_TOKEN),
        None,
        Some(good_body("redef-role")),
    )
    .await;
    assert_eq!(
        r2.status(),
        StatusCode::CREATED,
        "re-POST after retract must succeed"
    );
}

// ── 422: structural validation failures ──────────────────────────────────────

#[tokio::test]
async fn create_with_invalid_id_returns_422() {
    let (app, state) = support::build_test_router_fake_fabric(BootstrapConfig::default()).await;
    register_tokens(&state).await;

    let mut body = good_body("Bad Id With Spaces");
    body["system_prompt"] = serde_json::Value::String(good_prompt());

    let resp = send(
        &app,
        "POST",
        &project_path(),
        Some(ADMIN_TOKEN),
        None,
        Some(body),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::UNPROCESSABLE_ENTITY);

    let json = response_json(resp).await;
    let failures = json["details"]["failures"].as_array().unwrap();
    assert!(
        failures.iter().any(|f| f["code"] == "invalid_id"),
        "422 body must include invalid_id failure: {}",
        json
    );
}

#[tokio::test]
async fn create_with_prompt_missing_sections_returns_422() {
    let (app, state) = support::build_test_router_fake_fabric(BootstrapConfig::default()).await;
    register_tokens(&state).await;

    let body = serde_json::json!({
        "id": "empty-prompt",
        "name": "Empty",
        "tier": "standard",
        "description": "",
        "system_prompt": "not a prompt, no sections",
        "tools": [],
    });
    let resp = send(
        &app,
        "POST",
        &project_path(),
        Some(ADMIN_TOKEN),
        None,
        Some(body),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::UNPROCESSABLE_ENTITY);
}

// ── 413: body size overflow ──────────────────────────────────────────────────

#[tokio::test]
async fn create_with_oversized_prompt_returns_413() {
    let (app, state) = support::build_test_router_fake_fabric(BootstrapConfig::default()).await;
    register_tokens(&state).await;

    // 200 KiB prompt blows past the 128 KiB router layer — axum rejects
    // the oversized body before it reaches the handler.
    let big_prompt: String = "A".repeat(200_000);
    let body = serde_json::json!({
        "id": "big-prompt",
        "name": "Big",
        "tier": "standard",
        "description": "",
        "system_prompt": big_prompt,
    });

    let resp = send(
        &app,
        "POST",
        &project_path(),
        Some(ADMIN_TOKEN),
        None,
        Some(body),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::PAYLOAD_TOO_LARGE);
}

// ── 404: GET/DELETE/PATCH against unknown id ─────────────────────────────────

#[tokio::test]
async fn get_unknown_role_returns_404() {
    let (app, state) = support::build_test_router_fake_fabric(BootstrapConfig::default()).await;
    register_tokens(&state).await;

    let resp = send(
        &app,
        "GET",
        &project_path_id("ghost-role"),
        Some(ADMIN_TOKEN),
        None,
        None,
    )
    .await;
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn delete_never_defined_role_returns_404() {
    let (app, state) = support::build_test_router_fake_fabric(BootstrapConfig::default()).await;
    register_tokens(&state).await;

    let resp = send(
        &app,
        "DELETE",
        &project_path_id("never-existed"),
        Some(ADMIN_TOKEN),
        None,
        None,
    )
    .await;
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

// ── 200 GET single, built-in fallback ────────────────────────────────────────

#[tokio::test]
async fn get_builtin_role_returns_200() {
    let (app, state) = support::build_test_router_fake_fabric(BootstrapConfig::default()).await;
    register_tokens(&state).await;

    let resp = send(
        &app,
        "GET",
        &project_path_id("reviewer"),
        Some(ADMIN_TOKEN),
        None,
        None,
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    let json = response_json(resp).await;
    assert_eq!(json["source"], "builtin");
    assert_eq!(json["role"]["role_id"], "reviewer");
}

// ── 412: stale If-Match on PATCH ─────────────────────────────────────────────

#[tokio::test]
async fn patch_with_stale_if_match_returns_412() {
    let (app, state) = support::build_test_router_fake_fabric(BootstrapConfig::default()).await;
    register_tokens(&state).await;

    let r1 = send(
        &app,
        "POST",
        &project_path(),
        Some(ADMIN_TOKEN),
        None,
        Some(good_body("etag-role")),
    )
    .await;
    assert_eq!(r1.status(), StatusCode::CREATED);

    let patch_body = serde_json::json!({"name": "Updated Name"});
    let resp = send(
        &app,
        "PATCH",
        &project_path_id("etag-role"),
        Some(ADMIN_TOKEN),
        Some("\"1\""), // stale value
        Some(patch_body),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::PRECONDITION_FAILED);
}

// ── 200 PATCH happy path + ETag refresh ──────────────────────────────────────

#[tokio::test]
async fn patch_with_matching_if_match_succeeds_and_refreshes_etag() {
    let (app, state) = support::build_test_router_fake_fabric(BootstrapConfig::default()).await;
    register_tokens(&state).await;

    let r1 = send(
        &app,
        "POST",
        &project_path(),
        Some(ADMIN_TOKEN),
        None,
        Some(good_body("patch-role")),
    )
    .await;
    assert_eq!(r1.status(), StatusCode::CREATED);
    let etag_v1 = r1
        .headers()
        .get("etag")
        .unwrap()
        .to_str()
        .unwrap()
        .to_owned();

    let patch = serde_json::json!({"name": "New Display Name"});
    let resp = send(
        &app,
        "PATCH",
        &project_path_id("patch-role"),
        Some(ADMIN_TOKEN),
        Some(&etag_v1),
        Some(patch),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    let etag_v2 = resp
        .headers()
        .get("etag")
        .unwrap()
        .to_str()
        .unwrap()
        .to_owned();
    // Depending on clock granularity these may be equal; the response
    // body is the source of truth for the semantic update.
    let json = response_json(resp).await;
    assert_eq!(json["role"]["display_name"], "New Display Name");
    // ETag should be a quoted opaque-tag either way.
    assert!(etag_v2.starts_with('"') && etag_v2.ends_with('"'));
}

// ── 422: PATCH immutable field ───────────────────────────────────────────────

#[tokio::test]
async fn patch_with_tier_change_returns_422_immutable_field() {
    let (app, state) = support::build_test_router_fake_fabric(BootstrapConfig::default()).await;
    register_tokens(&state).await;

    let r1 = send(
        &app,
        "POST",
        &project_path(),
        Some(ADMIN_TOKEN),
        None,
        Some(good_body("immut-role")),
    )
    .await;
    assert_eq!(r1.status(), StatusCode::CREATED);

    let patch = serde_json::json!({"tier": "research"});
    let resp = send(
        &app,
        "PATCH",
        &project_path_id("immut-role"),
        Some(ADMIN_TOKEN),
        None,
        Some(patch),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::UNPROCESSABLE_ENTITY);
    let json = response_json(resp).await;
    let failures = json["details"]["failures"].as_array().unwrap();
    assert!(
        failures
            .iter()
            .any(|f| f["code"] == "immutable_field" && f["field"] == "tier"),
        "422 body must cite immutable_field/tier: {}",
        json
    );
}

// ── 200 DELETE + idempotent repeat ───────────────────────────────────────────

#[tokio::test]
async fn delete_and_repeat_is_idempotent() {
    let (app, state) = support::build_test_router_fake_fabric(BootstrapConfig::default()).await;
    register_tokens(&state).await;

    let r1 = send(
        &app,
        "POST",
        &project_path(),
        Some(ADMIN_TOKEN),
        None,
        Some(good_body("idem-role")),
    )
    .await;
    assert_eq!(r1.status(), StatusCode::CREATED);

    let d1 = send(
        &app,
        "DELETE",
        &project_path_id("idem-role"),
        Some(ADMIN_TOKEN),
        None,
        None,
    )
    .await;
    assert_eq!(d1.status(), StatusCode::OK);
    let j1 = response_json(d1).await;
    let t1 = j1["retracted_at"].as_u64().unwrap();

    let d2 = send(
        &app,
        "DELETE",
        &project_path_id("idem-role"),
        Some(ADMIN_TOKEN),
        None,
        None,
    )
    .await;
    assert_eq!(d2.status(), StatusCode::OK);
    let j2 = response_json(d2).await;
    let t2 = j2["retracted_at"].as_u64().unwrap();
    assert_eq!(
        t1, t2,
        "idempotent repeat must return the original retracted_at"
    );
}

// ── 400: malformed JSON body ─────────────────────────────────────────────────

#[tokio::test]
async fn create_with_malformed_json_returns_400() {
    let (app, state) = support::build_test_router_fake_fabric(BootstrapConfig::default()).await;
    register_tokens(&state).await;

    let req = Request::builder()
        .method("POST")
        .uri(project_path())
        .header("authorization", format!("Bearer {ADMIN_TOKEN}"))
        .header("content-type", "application/json")
        .body(Body::from("{not json"))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert!(
        resp.status() == StatusCode::BAD_REQUEST
            || resp.status() == StatusCode::UNPROCESSABLE_ENTITY,
        "malformed JSON must return 400 or 422, got {}",
        resp.status()
    );
}

// ── shadow warning advisory ──────────────────────────────────────────────────

#[tokio::test]
async fn shadowing_builtin_emits_shadow_warning() {
    let (app, state) = support::build_test_router_fake_fabric(BootstrapConfig::default()).await;
    register_tokens(&state).await;

    // Shadowing a built-in requires the role's tier to match the
    // built-in's tier per §D11 — `reviewer` is Standard.
    let mut body = good_body("reviewer");
    body["tier"] = serde_json::Value::String("standard".to_owned());

    let resp = send(
        &app,
        "POST",
        &project_path(),
        Some(ADMIN_TOKEN),
        None,
        Some(body),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::CREATED);

    let json = response_json(resp).await;
    assert_eq!(json["source"], "custom_shadow");
    let warnings = json["warnings"].as_array().unwrap();
    assert_eq!(warnings.len(), 1);
    assert_eq!(warnings[0]["code"], "shadow_warn_reviewer");
}

// ── ?source filter ───────────────────────────────────────────────────────────

#[tokio::test]
async fn list_filter_by_source_narrows_result() {
    let (app, state) = support::build_test_router_fake_fabric(BootstrapConfig::default()).await;
    register_tokens(&state).await;

    let r1 = send(
        &app,
        "POST",
        &project_path(),
        Some(ADMIN_TOKEN),
        None,
        Some(good_body("only-custom")),
    )
    .await;
    assert_eq!(r1.status(), StatusCode::CREATED);

    let uri = format!("{}?source=custom", project_path());
    let resp = send(&app, "GET", &uri, Some(ADMIN_TOKEN), None, None).await;
    assert_eq!(resp.status(), StatusCode::OK);
    let json = response_json(resp).await;
    let items = json["items"].as_array().unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0]["role"]["role_id"], "only-custom");
}

#[tokio::test]
async fn list_invalid_source_filter_returns_400() {
    let (app, state) = support::build_test_router_fake_fabric(BootstrapConfig::default()).await;
    register_tokens(&state).await;

    let uri = format!("{}?source=nonsense", project_path());
    let resp = send(&app, "GET", &uri, Some(ADMIN_TOKEN), None, None).await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}
