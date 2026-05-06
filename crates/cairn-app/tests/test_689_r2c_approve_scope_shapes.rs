//! Issue #689 R2-C — `POST /v1/approvals/:id/approve` accepts both the
//! canonical tagged-enum `scope` shape AND a bare-string shorthand.
//!
//! Dogfood Round 2 Finding C: the endpoint previously only accepted
//! `{"type":"once"}` / `{"type":"session", ...}` for the `scope` field
//! but operators typing JSON by hand reach for `{"scope":"once"}`
//! first. The 422 message was technically accurate but didn't hint at
//! the required nested shape.
//!
//! Path 1 implementation (per the task brief + CLAUDE.md "error message
//! quality is product quality" line): widened the DTO deserialize to
//! accept both forms; session with a custom `match_policy` still
//! requires the object form. Missing-scope 422 now names both accepted
//! shapes verbatim so an operator hitting curl gets a copy-pasteable
//! hint in the response body.
//!
//! Coverage:
//! * string shorthand `"once"` + `"session"`
//! * object form `{"type":"once"}` + `{"type":"session"}` (regression)
//! * object form with explicit `match_policy`
//! * invalid shorthand → 422 from axum JSON rejection with a named
//!   deserialize error
//! * missing `scope` → 422 with the full expected-shape hint

mod support;

use axum::{
    body::{to_bytes, Body},
    http::{Request, StatusCode},
};
use cairn_api::auth::AuthPrincipal;
use cairn_api::bootstrap::BootstrapConfig;
use cairn_domain::tenancy::TenantKey;
use cairn_domain::{ApprovalMatchPolicy, OperatorId, ProjectKey, RunId, SessionId, ToolCallId};
use cairn_runtime::tool_call_approvals::ToolCallProposal;
use serde_json::{json, Value};
use tower::ServiceExt;

const TOKEN: &str = "scope-shapes-token";
const ACTOR: &str = "test_op";

fn bearer() -> String {
    format!("Bearer {TOKEN}")
}

fn register_principal(state: &cairn_app::AppState) {
    state.service_tokens.register(
        TOKEN.to_string(),
        AuthPrincipal::Operator {
            operator_id: OperatorId::new(ACTOR),
            tenant: TenantKey::new("default_tenant"),
        },
    );
}

async fn seed_proposal(state: &cairn_app::AppState, call_id: &str) {
    let proposal = ToolCallProposal {
        call_id: ToolCallId::new(call_id),
        session_id: SessionId::new("sess_689"),
        run_id: RunId::new("run_689"),
        project: ProjectKey::new("default_tenant", "default_workspace", "default_project"),
        tool_name: "read".to_owned(),
        tool_args: json!({ "path": "/tmp/x" }),
        display_summary: None,
        match_policy: ApprovalMatchPolicy::Exact,
    };
    state
        .runtime
        .tool_call_approvals
        .submit_proposal(proposal)
        .await
        .expect("submit");
}

async fn post_approve(app: axum::Router, call_id: &str, body: Value) -> (StatusCode, Value) {
    let res = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v1/approvals/{call_id}/approve"))
                .header("authorization", bearer())
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = res.status();
    let bytes = to_bytes(res.into_body(), usize::MAX).await.unwrap();
    let json = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap_or(Value::Null)
    };
    (status, json)
}

/// Object form `{"type":"once"}` — regression guard that the default
/// tagged-enum shape still works after the Deserialize rewrite.
#[tokio::test]
async fn approve_accepts_object_once_scope() {
    let (app, state) = support::build_test_router_fake_fabric(BootstrapConfig::default()).await;
    register_principal(&state);
    seed_proposal(&state, "tc_obj_once").await;

    let (status, body) =
        post_approve(app, "tc_obj_once", json!({ "scope": { "type": "once" } })).await;
    assert_eq!(status, StatusCode::OK, "object once: {body}");
    assert_eq!(body["state"], "approved");
    assert_eq!(body["scope"]["kind"], "once");
}

/// Bare-string shorthand `"once"` — the main #689 R2-C fix. A naive
/// operator-typed curl body should succeed.
#[tokio::test]
async fn approve_accepts_string_once_scope() {
    let (app, state) = support::build_test_router_fake_fabric(BootstrapConfig::default()).await;
    register_principal(&state);
    seed_proposal(&state, "tc_str_once").await;

    let (status, body) = post_approve(app, "tc_str_once", json!({ "scope": "once" })).await;
    assert_eq!(status, StatusCode::OK, "string once: {body}");
    assert_eq!(body["state"], "approved");
    assert_eq!(body["scope"]["kind"], "once");
}

/// Bare-string shorthand `"session"` — inherits the proposal's
/// match policy (Exact in this test).
#[tokio::test]
async fn approve_accepts_string_session_scope_inherits_policy() {
    let (app, state) = support::build_test_router_fake_fabric(BootstrapConfig::default()).await;
    register_principal(&state);
    seed_proposal(&state, "tc_str_session").await;

    let (status, body) = post_approve(app, "tc_str_session", json!({ "scope": "session" })).await;
    assert_eq!(status, StatusCode::OK, "string session: {body}");
    assert_eq!(body["state"], "approved");
    assert_eq!(body["scope"]["kind"], "session");
    assert_eq!(
        body["scope"]["match_policy"]["kind"], "exact",
        "match_policy inherited from proposal when the shorthand omits it",
    );
}

/// Object form `{"type":"session", "match_policy": {...}}` — explicit
/// match policy still parses through the custom deserializer.
#[tokio::test]
async fn approve_accepts_object_session_with_explicit_match_policy() {
    let (app, state) = support::build_test_router_fake_fabric(BootstrapConfig::default()).await;
    register_principal(&state);
    seed_proposal(&state, "tc_obj_sess_policy").await;

    let (status, body) = post_approve(
        app,
        "tc_obj_sess_policy",
        json!({
            "scope": {
                "type": "session",
                "match_policy": {
                    "kind": "exact_path",
                    "path": "/tmp/x",
                },
            },
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "object session w/ policy: {body}");
    assert_eq!(body["state"], "approved");
    assert_eq!(body["scope"]["kind"], "session");
    assert_eq!(body["scope"]["match_policy"]["kind"], "exact_path");
    assert_eq!(body["scope"]["match_policy"]["path"], "/tmp/x");
}

/// Invalid string shorthand surfaces through axum's JSON-body extractor
/// as a 422 (or 400) with a deserialize error that names both forms.
/// We don't pin the exact axum message — just the status + that it
/// refuses to approve.
#[tokio::test]
async fn approve_rejects_unknown_string_shorthand() {
    let (app, state) = support::build_test_router_fake_fabric(BootstrapConfig::default()).await;
    register_principal(&state);
    seed_proposal(&state, "tc_bad_str").await;

    let (status, _body) = post_approve(app, "tc_bad_str", json!({ "scope": "banana" })).await;
    assert!(
        status == StatusCode::UNPROCESSABLE_ENTITY || status == StatusCode::BAD_REQUEST,
        "unknown shorthand must not succeed; got {status}",
    );
}

/// Missing `scope` entirely — the 422 body should now include the full
/// expected-shape hint (both object form and string shorthand), per
/// #689 R2-C. This is the operator's copy-pasteable cheat-sheet.
#[tokio::test]
async fn approve_missing_scope_emits_shape_hint() {
    let (app, state) = support::build_test_router_fake_fabric(BootstrapConfig::default()).await;
    register_principal(&state);
    seed_proposal(&state, "tc_no_scope").await;

    let (status, body) = post_approve(app, "tc_no_scope", json!({})).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "no scope: {body}");
    assert_eq!(body["code"], "validation_error");
    let msg = body["message"].as_str().unwrap_or_default();
    assert!(
        msg.contains("\"type\":\"once\""),
        "hint must name object-once shape: {msg}",
    );
    assert!(
        msg.contains("\"type\":\"session\""),
        "hint must name object-session shape: {msg}",
    );
    assert!(
        msg.contains("once") && msg.contains("session"),
        "hint must name both scope values: {msg}",
    );
    assert!(
        msg.contains("shorthand"),
        "hint must advertise string shorthand: {msg}",
    );
}
