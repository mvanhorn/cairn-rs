//! Regression test for the `GET /v1/tasks/:id` admin-bypass bug.
//!
//! Archaeology:
//! - PR #24 (commit 38ebbac2) decomposed `lib.rs` and carried a
//!   hand-rolled tenant check into `get_task_handler` verbatim.
//! - PR #50 (T6a-C3, commit 0d5e7d78) introduced
//!   `load_task_visible_to_tenant` and applied it to every MUTATION
//!   endpoint (add_dependency / set_priority / claim / heartbeat /
//!   cancel / complete / release_lease). The read endpoint
//!   `get_task_handler` was overlooked.
//! - Result: admin-token `GET /v1/tasks/:id` returned 404 for tasks
//!   outside the admin token's own tenant, even though `GET /v1/tasks`
//!   list and `POST /v1/tasks/:id/claim` for the same id returned 200.
//!
//! This test exercises the read endpoint against a real cairn-app
//! subprocess. On main (pre-fix) the admin-bypass check fails with
//! a 404 for the admin-token read. The cross-tenant leak-prevention
//! case covers the other direction of the helper's contract.

mod support;

use serde_json::json;
use support::live_fabric::LiveHarness;

/// Seed a session + run + task under the harness's uuid-scoped tenant.
/// Returns the task id for the test to read back.
async fn seed_task(h: &LiveHarness) -> String {
    let session_id = format!("sess_{}", &h.project);
    let run_id = format!("run_{}", &h.project);
    let task_id = format!("task_{}", &h.project);

    let res = h
        .client()
        .post(format!("{}/v1/sessions", h.base_url))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "tenant_id": h.tenant,
            "workspace_id": h.workspace,
            "project_id": h.project,
            "session_id": session_id,
        }))
        .send()
        .await
        .expect("POST /v1/sessions reaches server");
    assert_eq!(
        res.status().as_u16(),
        201,
        "session create: {}",
        res.text().await.unwrap_or_default()
    );

    let res = h
        .client()
        .post(format!("{}/v1/runs", h.base_url))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "tenant_id": h.tenant,
            "workspace_id": h.workspace,
            "project_id": h.project,
            "session_id": session_id,
            "run_id": run_id,
        }))
        .send()
        .await
        .expect("POST /v1/runs reaches server");
    assert_eq!(
        res.status().as_u16(),
        201,
        "run create: {}",
        res.text().await.unwrap_or_default()
    );

    let res = h
        .client()
        .post(format!("{}/v1/tasks", h.base_url))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "tenant_id": h.tenant,
            "workspace_id": h.workspace,
            "project_id": h.project,
            "task_id": task_id,
            "parent_run_id": run_id,
        }))
        .send()
        .await
        .expect("POST /v1/tasks reaches server");
    assert_eq!(
        res.status().as_u16(),
        201,
        "task create: {}",
        res.text().await.unwrap_or_default()
    );

    task_id
}

/// Mint an operator token scoped to a specific tenant via
/// `POST /v1/auth/tokens`. Returns the raw bearer token. Uses the
/// harness's admin token for the mint call (only admins may issue).
async fn mint_operator_token(h: &LiveHarness, operator_id: &str, tenant_id: &str) -> String {
    let res = h
        .client()
        .post(format!("{}/v1/auth/tokens", h.base_url))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "operator_id": operator_id,
            "tenant_id": tenant_id,
            "name": format!("get-task-admin-bypass-test-{operator_id}"),
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
    let body: serde_json::Value = res.json().await.expect("auth token json");
    body["token"]
        .as_str()
        .expect("auth token response has `token`")
        .to_owned()
}

/// Admin token reads a task belonging to a different (non-"default")
/// tenant. Must return 200 — the admin bypass is the whole point of
/// the helper. On main (pre-fix) this returns 404.
#[tokio::test]
async fn get_task_admin_token_reads_task_in_other_tenant() {
    let h = LiveHarness::setup().await;
    let task_id = seed_task(&h).await;

    // Sanity: the list endpoint (which takes tenant via query param)
    // already returns the task, proving the write landed in the store.
    let list_url = format!(
        "{}/v1/tasks?tenant_id={}&workspace_id={}&project_id={}",
        h.base_url, h.tenant, h.workspace, h.project,
    );
    let res = h
        .client()
        .get(&list_url)
        .bearer_auth(&h.admin_token)
        .send()
        .await
        .expect("GET /v1/tasks list reaches server");
    assert_eq!(res.status().as_u16(), 200, "list status");
    let body: serde_json::Value = res.json().await.expect("list json");
    let items = body
        .as_array()
        .cloned()
        .or_else(|| body.get("items").and_then(|v| v.as_array()).cloned())
        .expect("list body shape");
    assert!(
        items
            .iter()
            .any(|t| t.get("task_id").and_then(|v| v.as_str()) == Some(task_id.as_str())),
        "seed task missing from list: {body}",
    );

    // The bug: this returned 404 before the fix. With the fix it must
    // return 200 and include the task_id.
    let res = h
        .client()
        .get(format!("{}/v1/tasks/{}", h.base_url, task_id))
        .bearer_auth(&h.admin_token)
        .send()
        .await
        .expect("GET /v1/tasks/:id reaches server");
    assert_eq!(
        res.status().as_u16(),
        200,
        "admin-token read must 200 (is_admin bypass): {}",
        res.text().await.unwrap_or_default(),
    );
    let body: serde_json::Value = res.json().await.expect("get json");
    assert_eq!(
        body["task_id"].as_str(),
        Some(task_id.as_str()),
        "response echoes task_id: {body}",
    );
}

/// Non-admin operator scoped to the SAME tenant as the task must 200.
#[tokio::test]
async fn get_task_operator_same_tenant_reads_task() {
    let h = LiveHarness::setup().await;
    let task_id = seed_task(&h).await;
    let op_token = mint_operator_token(&h, "op_same", &h.tenant).await;

    let res = h
        .client()
        .get(format!("{}/v1/tasks/{}", h.base_url, task_id))
        .bearer_auth(&op_token)
        .send()
        .await
        .expect("GET /v1/tasks/:id reaches server");
    assert_eq!(
        res.status().as_u16(),
        200,
        "same-tenant operator must 200: {}",
        res.text().await.unwrap_or_default(),
    );
    let body: serde_json::Value = res.json().await.expect("get json");
    assert_eq!(body["task_id"].as_str(), Some(task_id.as_str()));
}

/// Non-admin operator scoped to a DIFFERENT tenant must 404. This is
/// the other half of the helper's contract — cross-tenant reads never
/// leak existence. The helper was already right on this path before
/// the fix (hand-rolled check would also 404); the regression test
/// locks it in so future refactors don't loosen it.
#[tokio::test]
async fn get_task_operator_cross_tenant_sees_404() {
    let h = LiveHarness::setup().await;
    let task_id = seed_task(&h).await;
    let op_token = mint_operator_token(&h, "op_cross", "some_other_tenant").await;

    let res = h
        .client()
        .get(format!("{}/v1/tasks/{}", h.base_url, task_id))
        .bearer_auth(&op_token)
        .send()
        .await
        .expect("GET /v1/tasks/:id reaches server");
    assert_eq!(
        res.status().as_u16(),
        404,
        "cross-tenant operator must 404 (no existence leak): {}",
        res.text().await.unwrap_or_default(),
    );
}

/// Unknown task id returns 404 regardless of token class. Baseline
/// shape check so no future change masks genuine not-found as 200.
#[tokio::test]
async fn get_task_unknown_id_returns_404() {
    let h = LiveHarness::setup().await;

    let res = h
        .client()
        .get(format!("{}/v1/tasks/nonexistent_{}", h.base_url, h.project))
        .bearer_auth(&h.admin_token)
        .send()
        .await
        .expect("GET /v1/tasks/:id reaches server");
    assert_eq!(
        res.status().as_u16(),
        404,
        "unknown task id must 404: {}",
        res.text().await.unwrap_or_default(),
    );
}
