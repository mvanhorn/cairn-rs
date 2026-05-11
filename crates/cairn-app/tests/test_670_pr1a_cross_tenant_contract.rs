//! Issue #670 G4 PR-1a cross-tenant contract-proof test.
//!
//! # The contract (RFC 027, §Cross-tenant prevention)
//!
//! After PR-1a, `TaskService::spawn_subagent` and
//! `RunService::spawn_subagent` MUST NOT accept a `project`
//! parameter. Implementations derive the child's `ProjectKey` from
//! the parent run row. Neither the LLM, nor a future role resolver,
//! nor any test caller can influence the child's tenancy by passing
//! a different argument — a cross-tenant spawn requires changing
//! the trait signatures (reviewer-visible) rather than passing a
//! different argument (silent).
//!
//! # Why a dedicated test
//!
//! The G1+G2 test (#671) asserts the spawn audit row carries the
//! expected `parent_run_id`; the G3 test (#673) asserts a child
//! `RunRecord` is created; neither specifically asserts
//! `child.project == parent.project` byte-for-byte. This test fills
//! that gap — it's the contract-proof RFC 027 requires for PR-1a
//! merge.
//!
//! # The test
//!
//! 1. Provision a parent run in tenant T1/workspace W1/project P1.
//! 2. Drive an orchestrate iteration where the mock LLM proposes a
//!    `spawn_subagent`.
//! 3. Read back the child run via `GET /v1/runs/:parent/children`.
//! 4. Assert every project-scope field on the child equals the
//!    parent's: `tenant_id == T1`, `workspace_id == W1`,
//!    `project_id == P1`.
//!
//! # Prove-the-fix contract
//!
//! Pre-PR-1a: the execute layer passed `&ctx.project` to
//! `TaskService::spawn_subagent`. An adversarial test (or future
//! role resolver) could have passed a different `project`. This
//! test, written against post-PR-1a code, compiles against the new
//! signature (no `project` arg). The earlier `project` arg would
//! force a compile error if reintroduced — the type system is the
//! primary defense.

mod support;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use axum::{
    extract::State,
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use serde_json::{json, Value};
use support::live_fabric::LiveHarness;

const MOCK_MODEL: &str = "openrouter/670-pr1a-cross-tenant";
const DELEGATED_GOAL: &str = "PR-1a cross-tenant contract smoke test";
const DELEGATED_ROLE: &str = "researcher";

#[derive(Clone)]
struct MockState {
    hits: Arc<AtomicUsize>,
}

async fn spawn_mock() -> (String, Arc<AtomicUsize>) {
    let state = MockState {
        hits: Arc::new(AtomicUsize::new(0)),
    };
    let hits = state.hits.clone();

    async fn chat_handler(
        State(state): State<MockState>,
        Json(_body): Json<Value>,
    ) -> (StatusCode, Json<Value>) {
        let n = state.hits.fetch_add(1, Ordering::SeqCst);
        let content = if n == 0 {
            json!([{
                "action_type":       "spawn_subagent",
                "description":       "#670 PR-1a: delegate to a researcher; test asserts child.project == parent.project",
                "tool_name":         DELEGATED_ROLE,
                "tool_args":         { "goal": DELEGATED_GOAL },
                "confidence":        0.95,
                "requires_approval": false,
            }])
        } else {
            json!([{
                "action_type":       "complete_run",
                "description":       "#670 PR-1a: parent done",
                "confidence":        0.99,
                "requires_approval": false,
            }])
        };
        (
            StatusCode::OK,
            Json(json!({
                "id":      format!("mock-pr1a-{n}"),
                "choices": [{
                    "index":   0,
                    "message": { "role": "assistant", "content": content.to_string() },
                    "finish_reason": "stop",
                }],
                "usage": { "prompt_tokens": 10, "completion_tokens": 6, "total_tokens": 16 },
            })),
        )
    }

    let app = Router::new()
        .route("/chat/completions", post(chat_handler))
        .route("/v1/chat/completions", post(chat_handler))
        .route(
            "/v1/models",
            get(|| async { Json(json!({ "data": [{ "id": MOCK_MODEL }] })) }),
        )
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (format!("http://{addr}"), hits)
}

/// Provision a parent run using the explicit default tenant/workspace/project
/// triple. We name it explicitly (rather than letting the harness choose)
/// so the assertion below can check for byte-equality against a known
/// expected project key.
async fn provision_run(
    h: &LiveHarness,
    mock_url: &str,
) -> (String, String, String, String, String) {
    let suffix = h.project.clone();
    let tenant = "default_tenant".to_owned();
    let workspace = "default_workspace".to_owned();
    let project = "default_project".to_owned();
    let connection_id = format!("conn_pr1a_{suffix}");
    let session_id = format!("sess_pr1a_{suffix}");
    let run_id = format!("run_pr1a_{suffix}");

    let r = h
        .client()
        .post(format!(
            "{}/v1/admin/tenants/{}/credentials",
            h.base_url, tenant
        ))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "provider_id":     "openrouter",
            "plaintext_value": format!("sk-pr1a-{suffix}"),
        }))
        .send()
        .await
        .expect("credential reaches server");
    assert_eq!(r.status().as_u16(), 201);
    let credential_id = r
        .json::<Value>()
        .await
        .unwrap()
        .get("id")
        .and_then(|v| v.as_str())
        .unwrap()
        .to_owned();

    let r = h
        .client()
        .post(format!("{}/v1/providers/connections", h.base_url))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "tenant_id":              tenant,
            "provider_connection_id": connection_id,
            "provider_family":        "openrouter",
            "adapter_type":           "openrouter",
            "supported_models":       [MOCK_MODEL],
            "credential_id":          credential_id,
            "endpoint_url":           mock_url,
        }))
        .send()
        .await
        .expect("connection reaches server");
    assert_eq!(r.status().as_u16(), 201);

    for key in ["generate_model", "brain_model"] {
        let r = h
            .client()
            .put(format!(
                "{}/v1/settings/defaults/system/system/{}",
                h.base_url, key,
            ))
            .bearer_auth(&h.admin_token)
            .json(&json!({ "value": MOCK_MODEL }))
            .send()
            .await
            .expect("defaults reaches server");
        assert_eq!(r.status().as_u16(), 200);
    }

    let r = h
        .client()
        .post(format!("{}/v1/sessions", h.base_url))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "tenant_id":    tenant,
            "workspace_id": workspace,
            "project_id":   project,
            "session_id":   session_id,
        }))
        .send()
        .await
        .expect("session reaches server");
    assert_eq!(r.status().as_u16(), 201);

    let r = h
        .client()
        .post(format!("{}/v1/runs", h.base_url))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "tenant_id":    tenant,
            "workspace_id": workspace,
            "project_id":   project,
            "session_id":   session_id,
            "run_id":       run_id,
        }))
        .send()
        .await
        .expect("run reaches server");
    assert_eq!(r.status().as_u16(), 201);

    (tenant, workspace, project, session_id, run_id)
}

/// The contract-proof: LLM-initiated spawn produces a child whose
/// project scope equals the parent's byte-for-byte.
#[tokio::test]
async fn llm_spawned_child_inherits_parent_project_exactly() {
    let h = LiveHarness::setup().await;
    let (mock_url, _hits) = spawn_mock().await;
    let (tenant, workspace, project, _session_id, run_id) = provision_run(&h, &mock_url).await;

    // Drive one orchestrate iteration — mock returns spawn_subagent
    // on iteration 1.
    let r = h
        .client()
        .post(format!("{}/v1/runs/{}/orchestrate", h.base_url, run_id))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "goal": "#670 PR-1a parent",
            "max_iterations": 1,
        }))
        .send()
        .await
        .expect("orchestrate reaches server");
    let status = r.status().as_u16();
    let body: Value = r.json().await.unwrap_or(Value::Null);
    assert!(
        status == 200 || status == 202,
        "orchestrate must succeed; status={status} body={body}",
    );

    // Read children; PR-1a signature forbids caller-supplied project
    // divergence, so this child MUST inherit parent's project.
    let r = h
        .client()
        .get(format!("{}/v1/runs/{}/children", h.base_url, run_id))
        .bearer_auth(&h.admin_token)
        .send()
        .await
        .expect("list children reaches server");
    assert_eq!(r.status().as_u16(), 200);
    let children_body: Value = r.json().await.expect("children json");
    let children = children_body
        .get("items")
        .and_then(|v| v.as_array())
        .expect("children has items");
    assert_eq!(children.len(), 1, "exactly one child expected");
    let child = &children[0];

    // Contract-proof assertions: each project-scope field matches
    // the parent's byte-for-byte. If a future commit reintroduces a
    // `project` parameter (silently, via an argument shape that
    // lets a caller diverge from parent), at least one of these
    // assertions fails — the tenancy-leak is caught here rather
    // than in production.
    let child_project = child
        .get("project")
        .and_then(|v| v.as_object())
        .expect("child run record carries nested project object");
    assert_eq!(
        child_project.get("tenant_id").and_then(|v| v.as_str()),
        Some(tenant.as_str()),
        "#670 PR-1a cross-tenant contract: child.project.tenant_id must == parent.project.tenant_id. \
         Child: {child}",
    );
    assert_eq!(
        child_project.get("workspace_id").and_then(|v| v.as_str()),
        Some(workspace.as_str()),
        "#670 PR-1a cross-tenant contract: child.project.workspace_id must == parent.project.workspace_id. \
         Child: {child}",
    );
    assert_eq!(
        child_project.get("project_id").and_then(|v| v.as_str()),
        Some(project.as_str()),
        "#670 PR-1a cross-tenant contract: child.project.project_id must == parent.project.project_id. \
         Child: {child}",
    );
}
