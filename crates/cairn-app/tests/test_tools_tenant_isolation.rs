//! Tenant-isolation regression tests for every handler in
//! `crates/cairn-app/src/handlers/tools.rs`.
//!
//! Covers the META #372 cluster:
//!  - #362 list_tool_invocations_handler
//!  - #363 get_tool_invocation_handler
//!  - #364 get_tool_invocation_progress_handler
//!  - #365 create_tool_invocation_handler (body tenant spoof)
//!  - #366 complete_tool_invocation_handler
//!  - #367 cancel_tool_invocation_handler
//!  - #368 list_checkpoints_handler
//!  - #369 restore_checkpoint_handler
//!  - #370 save_checkpoint_handler
//!  - #371 get/set_checkpoint_strategy_handler
//!
//! Every test hits a real `cairn-app` subprocess via `LiveHarness`.
//! No mocks. Mirrors the existing `test_get_task_admin_bypass.rs` +
//! `test_http_cross_tenant.rs` shape.

mod support;

use serde_json::{json, Value};
use support::live_fabric::LiveHarness;

// ── Fixtures ────────────────────────────────────────────────────────────────

/// Mint an operator-scoped bearer token for the given tenant. The
/// caller MUST use the admin token to mint — `/v1/auth/tokens` is
/// admin-only.
async fn mint_operator_token(h: &LiveHarness, operator_id: &str, tenant_id: &str) -> String {
    let res = h
        .client()
        .post(format!("{}/v1/auth/tokens", h.base_url))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "operator_id": operator_id,
            "tenant_id": tenant_id,
            "name": format!("tools-tenant-iso-{operator_id}"),
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
    let body: Value = res.json().await.expect("auth token json");
    body["token"]
        .as_str()
        .expect("auth token response has `token`")
        .to_owned()
}

/// Seed a (session, run) under the harness's uuid-scoped tenant A.
/// Returns the run_id so the test can plant invocations / checkpoints
/// against it.
async fn seed_run(h: &LiveHarness) -> (String, String) {
    let session_id = format!("sess_{}", &h.project);
    let run_id = format!("run_{}", &h.project);

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
    assert_eq!(res.status().as_u16(), 201, "session create");

    // #702 follow-up: pin the run's agent_role to `executor` via
    // project defaults so the orchestrator shell policy does not fire
    // on this test's stand-in bash calls (which predate the policy).
    let r_role = h
        .client()
        .put(format!(
            "{}/v1/settings/defaults/project/{}/run:{run_id}:agent_role",
            h.base_url, h.project,
        ))
        .bearer_auth(&h.admin_token)
        .json(&serde_json::json!({ "value": "executor" }))
        .send()
        .await
        .expect("set agent_role default");
    assert_eq!(r_role.status().as_u16(), 200);

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
    assert_eq!(res.status().as_u16(), 201, "run create");

    (session_id, run_id)
}

/// Seed a tool invocation on tenant A's run. Uses a builtin target so
/// no plugin host is needed. Returns the invocation_id.
async fn seed_tool_invocation(h: &LiveHarness, session_id: &str, run_id: &str) -> String {
    let invocation_id = format!("inv_{}", &h.project);
    let res = h
        .client()
        .post(format!("{}/v1/tool-invocations", h.base_url))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "tenant_id": h.tenant,
            "workspace_id": h.workspace,
            "project_id": h.project,
            "invocation_id": invocation_id,
            "session_id": session_id,
            "run_id": run_id,
            "target": { "target_type": "builtin", "tool_name": "bash" },
            "execution_class": "supervised_process",
            "args": { "cmd": "echo tenant-a" },
        }))
        .send()
        .await
        .expect("POST /v1/tool-invocations reaches server");
    assert_eq!(
        res.status().as_u16(),
        201,
        "create invocation: {}",
        res.text().await.unwrap_or_default()
    );
    invocation_id
}

/// Save a checkpoint on tenant A's run. Returns the checkpoint_id.
async fn seed_checkpoint(h: &LiveHarness, run_id: &str) -> String {
    let checkpoint_id = format!("ckpt_{}", &h.project);
    let res = h
        .client()
        .post(format!("{}/v1/runs/{}/checkpoint", h.base_url, run_id))
        .bearer_auth(&h.admin_token)
        .json(&json!({ "checkpoint_id": checkpoint_id }))
        .send()
        .await
        .expect("POST /v1/runs/:id/checkpoint reaches server");
    assert_eq!(
        res.status().as_u16(),
        201,
        "create checkpoint: {}",
        res.text().await.unwrap_or_default()
    );
    checkpoint_id
}

// ── #362 list_tool_invocations_handler ─────────────────────────────────────

/// Tenant-A operator listing their own run's invocations sees them.
#[tokio::test]
async fn list_tool_invocations_same_tenant_returns_items() {
    let h = LiveHarness::setup().await;
    let (session_id, run_id) = seed_run(&h).await;
    let invocation_id = seed_tool_invocation(&h, &session_id, &run_id).await;
    let op_a = mint_operator_token(&h, "op_list_same", &h.tenant).await;

    let res = h
        .client()
        .get(format!(
            "{}/v1/tool-invocations?run_id={run_id}",
            h.base_url
        ))
        .bearer_auth(&op_a)
        .send()
        .await
        .expect("list reaches server");
    assert_eq!(res.status().as_u16(), 200);
    let body: Value = res.json().await.expect("list json");
    let items = body["items"].as_array().expect("items array");
    assert!(
        items
            .iter()
            .any(|i| i["record"]["invocation_id"].as_str() == Some(invocation_id.as_str())),
        "expected invocation in list: {body}",
    );
}

/// #362: tenant-B operator listing tenant-A's run sees an empty list.
/// The previous handler returned tenant-A's invocations to any
/// authenticated caller — a cross-tenant read of arg JSON (which
/// often carries secrets).
#[tokio::test]
async fn list_tool_invocations_cross_tenant_sees_empty() {
    let h = LiveHarness::setup().await;
    let (session_id, run_id) = seed_run(&h).await;
    let _invocation_id = seed_tool_invocation(&h, &session_id, &run_id).await;
    let op_b = mint_operator_token(&h, "op_list_cross", "some_other_tenant").await;

    let res = h
        .client()
        .get(format!(
            "{}/v1/tool-invocations?run_id={run_id}",
            h.base_url
        ))
        .bearer_auth(&op_b)
        .send()
        .await
        .expect("list reaches server");
    assert_eq!(res.status().as_u16(), 200);
    let body: Value = res.json().await.expect("list json");
    let items = body["items"].as_array().expect("items array");
    assert!(
        items.is_empty(),
        "cross-tenant operator must see empty list (no existence leak): {body}",
    );
}

// ── #363 get_tool_invocation_handler ───────────────────────────────────────

/// Tenant-A operator reading their own invocation succeeds.
#[tokio::test]
async fn get_tool_invocation_same_tenant_returns_200() {
    let h = LiveHarness::setup().await;
    let (session_id, run_id) = seed_run(&h).await;
    let invocation_id = seed_tool_invocation(&h, &session_id, &run_id).await;
    let op_a = mint_operator_token(&h, "op_get_same", &h.tenant).await;

    let res = h
        .client()
        .get(format!(
            "{}/v1/tool-invocations/{invocation_id}",
            h.base_url
        ))
        .bearer_auth(&op_a)
        .send()
        .await
        .expect("get reaches server");
    assert_eq!(res.status().as_u16(), 200);
}

/// #363: tenant-B operator reading tenant-A's invocation sees 404.
#[tokio::test]
async fn get_tool_invocation_cross_tenant_sees_404() {
    let h = LiveHarness::setup().await;
    let (session_id, run_id) = seed_run(&h).await;
    let invocation_id = seed_tool_invocation(&h, &session_id, &run_id).await;
    let op_b = mint_operator_token(&h, "op_get_cross", "some_other_tenant").await;

    let res = h
        .client()
        .get(format!(
            "{}/v1/tool-invocations/{invocation_id}",
            h.base_url
        ))
        .bearer_auth(&op_b)
        .send()
        .await
        .expect("get reaches server");
    assert_eq!(
        res.status().as_u16(),
        404,
        "cross-tenant get must 404: {}",
        res.text().await.unwrap_or_default()
    );
}

// ── #364 get_tool_invocation_progress_handler ──────────────────────────────

/// #364: tenant-B operator querying tenant-A's invocation progress
/// sees 404. The previous handler scanned the whole event log and
/// leaked any tenant's progress frame. No progress event is planted
/// — the 404 must come from the tenant check, not from "no progress
/// yet".
#[tokio::test]
async fn get_tool_invocation_progress_cross_tenant_sees_404() {
    let h = LiveHarness::setup().await;
    let (session_id, run_id) = seed_run(&h).await;
    let invocation_id = seed_tool_invocation(&h, &session_id, &run_id).await;
    let op_b = mint_operator_token(&h, "op_progress_cross", "some_other_tenant").await;

    let res = h
        .client()
        .get(format!(
            "{}/v1/tool-invocations/{invocation_id}/progress",
            h.base_url
        ))
        .bearer_auth(&op_b)
        .send()
        .await
        .expect("progress reaches server");
    assert_eq!(
        res.status().as_u16(),
        404,
        "cross-tenant progress must 404: {}",
        res.text().await.unwrap_or_default()
    );
}

/// Tenant-A operator querying their own invocation's progress with
/// no progress yet still sees a 404 — this is the "no rows" shape,
/// not the tenant-scope rejection. Separate test to lock in the
/// contract that the error body / status code is identical for
/// "unknown" and "out of scope".
#[tokio::test]
async fn get_tool_invocation_progress_no_events_yet_returns_404() {
    let h = LiveHarness::setup().await;
    let (session_id, run_id) = seed_run(&h).await;
    let invocation_id = seed_tool_invocation(&h, &session_id, &run_id).await;
    let op_a = mint_operator_token(&h, "op_progress_same", &h.tenant).await;

    let res = h
        .client()
        .get(format!(
            "{}/v1/tool-invocations/{invocation_id}/progress",
            h.base_url
        ))
        .bearer_auth(&op_a)
        .send()
        .await
        .expect("progress reaches server");
    assert_eq!(res.status().as_u16(), 404);
}

// ── #365 create_tool_invocation_handler ────────────────────────────────────

/// Tenant-A operator creating an invocation in their own tenant
/// succeeds.
#[tokio::test]
async fn create_tool_invocation_same_tenant_returns_201() {
    let h = LiveHarness::setup().await;
    let (session_id, run_id) = seed_run(&h).await;
    let op_a = mint_operator_token(&h, "op_create_same", &h.tenant).await;
    let invocation_id = format!("inv_op_create_{}", &h.project);

    let res = h
        .client()
        .post(format!("{}/v1/tool-invocations", h.base_url))
        .bearer_auth(&op_a)
        .json(&json!({
            "tenant_id": h.tenant,
            "workspace_id": h.workspace,
            "project_id": h.project,
            "invocation_id": invocation_id,
            "session_id": session_id,
            "run_id": run_id,
            "target": { "target_type": "builtin", "tool_name": "bash" },
            "execution_class": "supervised_process",
        }))
        .send()
        .await
        .expect("create reaches server");
    assert_eq!(
        res.status().as_u16(),
        201,
        "same-tenant create must 201: {}",
        res.text().await.unwrap_or_default()
    );
}

/// #365: tenant-B operator attempting to plant an invocation into
/// tenant-A via the body fails. The pre-fix handler accepted the
/// body tenant verbatim; the fix validates `tenant_id` against the
/// bearer-token scope via `ProjectJson<T>` and refuses.
#[tokio::test]
async fn create_tool_invocation_body_tenant_spoof_fails() {
    let h = LiveHarness::setup().await;
    let (session_id, run_id) = seed_run(&h).await;
    let op_b = mint_operator_token(&h, "op_create_spoof", "some_other_tenant").await;
    let invocation_id = format!("inv_spoof_{}", &h.project);

    let res = h
        .client()
        .post(format!("{}/v1/tool-invocations", h.base_url))
        .bearer_auth(&op_b)
        .json(&json!({
            // Tenant-B operator claiming tenant-A in the body — must
            // be refused, not silently forged.
            "tenant_id": h.tenant,
            "workspace_id": h.workspace,
            "project_id": h.project,
            "invocation_id": invocation_id,
            "session_id": session_id,
            "run_id": run_id,
            "target": { "target_type": "builtin", "tool_name": "bash" },
            "execution_class": "supervised_process",
        }))
        .send()
        .await
        .expect("create reaches server");
    assert!(
        matches!(res.status().as_u16(), 403 | 404),
        "spoofed create must be refused (403 from extractor or 404 scope): got {}",
        res.status().as_u16()
    );

    // And the record must not exist under tenant A.
    let res = h
        .client()
        .get(format!(
            "{}/v1/tool-invocations/{invocation_id}",
            h.base_url
        ))
        .bearer_auth(&h.admin_token)
        .send()
        .await
        .expect("readback reaches server");
    assert_eq!(
        res.status().as_u16(),
        404,
        "spoofed invocation must NOT have landed: {}",
        res.text().await.unwrap_or_default()
    );
}

// ── #366 complete_tool_invocation_handler ──────────────────────────────────

/// #366: tenant-B operator completing tenant-A's invocation sees 404
/// and the invocation state is unchanged.
#[tokio::test]
async fn complete_tool_invocation_cross_tenant_sees_404_and_no_mutation() {
    let h = LiveHarness::setup().await;
    let (session_id, run_id) = seed_run(&h).await;
    let invocation_id = seed_tool_invocation(&h, &session_id, &run_id).await;
    let op_b = mint_operator_token(&h, "op_complete_cross", "some_other_tenant").await;

    // Snapshot the pre-attempt state via the admin token.
    let before: Value = h
        .client()
        .get(format!(
            "{}/v1/tool-invocations/{invocation_id}",
            h.base_url
        ))
        .bearer_auth(&h.admin_token)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    let res = h
        .client()
        .post(format!(
            "{}/v1/tool-invocations/{invocation_id}/complete",
            h.base_url
        ))
        .bearer_auth(&op_b)
        .send()
        .await
        .expect("complete reaches server");
    assert_eq!(
        res.status().as_u16(),
        404,
        "cross-tenant complete must 404: {}",
        res.text().await.unwrap_or_default()
    );

    let after: Value = h
        .client()
        .get(format!(
            "{}/v1/tool-invocations/{invocation_id}",
            h.base_url
        ))
        .bearer_auth(&h.admin_token)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        before["status"], after["status"],
        "cross-tenant complete must NOT mutate state: before={before} after={after}",
    );
}

/// Tenant-A operator completing their own invocation succeeds.
#[tokio::test]
async fn complete_tool_invocation_same_tenant_returns_200() {
    let h = LiveHarness::setup().await;
    let (session_id, run_id) = seed_run(&h).await;
    let invocation_id = seed_tool_invocation(&h, &session_id, &run_id).await;
    let op_a = mint_operator_token(&h, "op_complete_same", &h.tenant).await;

    let res = h
        .client()
        .post(format!(
            "{}/v1/tool-invocations/{invocation_id}/complete",
            h.base_url
        ))
        .bearer_auth(&op_a)
        .send()
        .await
        .expect("complete reaches server");
    assert_eq!(res.status().as_u16(), 200);
}

// ── #367 cancel_tool_invocation_handler ────────────────────────────────────

/// #367: tenant-B operator cancelling tenant-A's invocation sees 404
/// and the invocation is NOT transitioned to canceled.
#[tokio::test]
async fn cancel_tool_invocation_cross_tenant_sees_404_and_no_mutation() {
    let h = LiveHarness::setup().await;
    let (session_id, run_id) = seed_run(&h).await;
    let invocation_id = seed_tool_invocation(&h, &session_id, &run_id).await;
    let op_b = mint_operator_token(&h, "op_cancel_cross", "some_other_tenant").await;

    let res = h
        .client()
        .post(format!(
            "{}/v1/tool-invocations/{invocation_id}/cancel",
            h.base_url
        ))
        .bearer_auth(&op_b)
        .send()
        .await
        .expect("cancel reaches server");
    assert_eq!(
        res.status().as_u16(),
        404,
        "cross-tenant cancel must 404: {}",
        res.text().await.unwrap_or_default()
    );

    // Confirm the invocation is NOT canceled.
    let after: Value = h
        .client()
        .get(format!(
            "{}/v1/tool-invocations/{invocation_id}",
            h.base_url
        ))
        .bearer_auth(&h.admin_token)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_ne!(
        after["status"].as_str(),
        Some("canceled"),
        "cross-tenant cancel must NOT canceled the invocation: {after}",
    );
}

/// Tenant-A operator cancelling their own invocation succeeds.
#[tokio::test]
async fn cancel_tool_invocation_same_tenant_returns_200() {
    let h = LiveHarness::setup().await;
    let (session_id, run_id) = seed_run(&h).await;
    let invocation_id = seed_tool_invocation(&h, &session_id, &run_id).await;
    let op_a = mint_operator_token(&h, "op_cancel_same", &h.tenant).await;

    let res = h
        .client()
        .post(format!(
            "{}/v1/tool-invocations/{invocation_id}/cancel",
            h.base_url
        ))
        .bearer_auth(&op_a)
        .send()
        .await
        .expect("cancel reaches server");
    assert_eq!(res.status().as_u16(), 200);
}

// ── #368 list_checkpoints_handler ──────────────────────────────────────────

/// #368: tenant-B operator listing tenant-A's checkpoints sees empty.
#[tokio::test]
async fn list_checkpoints_cross_tenant_sees_empty() {
    let h = LiveHarness::setup().await;
    let (_session_id, run_id) = seed_run(&h).await;
    let _ckpt = seed_checkpoint(&h, &run_id).await;
    let op_b = mint_operator_token(&h, "op_ckpt_list_cross", "some_other_tenant").await;

    let res = h
        .client()
        .get(format!("{}/v1/checkpoints?run_id={run_id}", h.base_url))
        .bearer_auth(&op_b)
        .send()
        .await
        .expect("list reaches server");
    assert_eq!(res.status().as_u16(), 200);
    let body: Value = res.json().await.expect("list json");
    let items = body["items"].as_array().expect("items array");
    assert!(
        items.is_empty(),
        "cross-tenant list_checkpoints must see empty: {body}",
    );
}

/// Tenant-A operator listing their own checkpoints sees them.
#[tokio::test]
async fn list_checkpoints_same_tenant_returns_items() {
    let h = LiveHarness::setup().await;
    let (_session_id, run_id) = seed_run(&h).await;
    let checkpoint_id = seed_checkpoint(&h, &run_id).await;
    let op_a = mint_operator_token(&h, "op_ckpt_list_same", &h.tenant).await;

    let res = h
        .client()
        .get(format!("{}/v1/checkpoints?run_id={run_id}", h.base_url))
        .bearer_auth(&op_a)
        .send()
        .await
        .expect("list reaches server");
    assert_eq!(res.status().as_u16(), 200);
    let body: Value = res.json().await.expect("list json");
    let items = body["items"].as_array().expect("items array");
    assert!(
        items
            .iter()
            .any(|c| c["checkpoint_id"].as_str() == Some(checkpoint_id.as_str())),
        "expected checkpoint in list: {body}",
    );
}

// ── #369 restore_checkpoint_handler ────────────────────────────────────────

/// #369: tenant-B operator restoring tenant-A's checkpoint sees 404.
/// This is the most dangerous endpoint in the cluster — restoring
/// rewinds the run and re-fires side effects, so a cross-tenant
/// restore could destroy inflight work in another tenant.
#[tokio::test]
async fn restore_checkpoint_cross_tenant_sees_404() {
    let h = LiveHarness::setup().await;
    let (_session_id, run_id) = seed_run(&h).await;
    let checkpoint_id = seed_checkpoint(&h, &run_id).await;
    let op_b = mint_operator_token(&h, "op_ckpt_restore_cross", "some_other_tenant").await;

    let res = h
        .client()
        .post(format!(
            "{}/v1/checkpoints/{checkpoint_id}/restore",
            h.base_url
        ))
        .bearer_auth(&op_b)
        .send()
        .await
        .expect("restore reaches server");
    assert_eq!(
        res.status().as_u16(),
        404,
        "cross-tenant restore must 404: {}",
        res.text().await.unwrap_or_default()
    );
}

// ── #370 save_checkpoint_handler ───────────────────────────────────────────

/// #370: tenant-B operator saving a checkpoint on tenant-A's run
/// sees 404.
#[tokio::test]
async fn save_checkpoint_cross_tenant_sees_404() {
    let h = LiveHarness::setup().await;
    let (_session_id, run_id) = seed_run(&h).await;
    let op_b = mint_operator_token(&h, "op_ckpt_save_cross", "some_other_tenant").await;

    let res = h
        .client()
        .post(format!("{}/v1/runs/{run_id}/checkpoint", h.base_url))
        .bearer_auth(&op_b)
        .json(&json!({ "checkpoint_id": format!("planted_{}", &h.project) }))
        .send()
        .await
        .expect("save checkpoint reaches server");
    assert_eq!(
        res.status().as_u16(),
        404,
        "cross-tenant save checkpoint must 404: {}",
        res.text().await.unwrap_or_default()
    );
}

// ── #371 get/set_checkpoint_strategy_handler ───────────────────────────────

/// #371: tenant-B operator reading tenant-A's checkpoint strategy
/// sees 404.
#[tokio::test]
async fn get_checkpoint_strategy_cross_tenant_sees_404() {
    let h = LiveHarness::setup().await;
    let (_session_id, run_id) = seed_run(&h).await;
    let op_b = mint_operator_token(&h, "op_ckpt_strat_cross", "some_other_tenant").await;

    let res = h
        .client()
        .get(format!(
            "{}/v1/runs/{run_id}/checkpoint-strategy",
            h.base_url
        ))
        .bearer_auth(&op_b)
        .send()
        .await
        .expect("strategy reaches server");
    assert_eq!(
        res.status().as_u16(),
        404,
        "cross-tenant get strategy must 404: {}",
        res.text().await.unwrap_or_default()
    );
}

/// #371: tenant-B operator setting a checkpoint strategy on
/// tenant-A's run sees 404.
#[tokio::test]
async fn set_checkpoint_strategy_cross_tenant_sees_404() {
    let h = LiveHarness::setup().await;
    let (_session_id, run_id) = seed_run(&h).await;
    let op_b = mint_operator_token(&h, "op_ckpt_strat_set_cross", "some_other_tenant").await;

    let res = h
        .client()
        .post(format!(
            "{}/v1/runs/{run_id}/checkpoint-strategy",
            h.base_url
        ))
        .bearer_auth(&op_b)
        .json(&json!({
            "strategy_id": format!("strat_{}", &h.project),
            "interval_ms": 1000u64,
            "max_checkpoints": 5u32,
            "trigger_on_task_complete": false,
        }))
        .send()
        .await
        .expect("set strategy reaches server");
    assert_eq!(
        res.status().as_u16(),
        404,
        "cross-tenant set strategy must 404: {}",
        res.text().await.unwrap_or_default()
    );
}

/// #371: admin token reading another tenant's checkpoint strategy
/// must pass (admin bypass — same spec as PR #337). The pre-fix
/// hand-rolled check was missing `is_admin`, so admin calls 404'd.
#[tokio::test]
async fn get_checkpoint_strategy_admin_bypass_returns_non_403() {
    let h = LiveHarness::setup().await;
    let (_session_id, run_id) = seed_run(&h).await;

    // Set a strategy first so the read returns 200 rather than "no
    // strategy yet" 404. Uses the admin token.
    let res = h
        .client()
        .post(format!(
            "{}/v1/runs/{run_id}/checkpoint-strategy",
            h.base_url
        ))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "strategy_id": format!("strat_admin_{}", &h.project),
            "interval_ms": 2000u64,
            "max_checkpoints": 10u32,
            "trigger_on_task_complete": true,
        }))
        .send()
        .await
        .expect("set strategy reaches server");
    assert_eq!(res.status().as_u16(), 200, "admin set must 200");

    // Admin token reads it — the harness's admin_token is scoped to
    // "default" but the run is under `h.tenant` (uuid-scoped). With
    // the #371 fix, admin_bypass applies and the read returns 200.
    let res = h
        .client()
        .get(format!(
            "{}/v1/runs/{run_id}/checkpoint-strategy",
            h.base_url
        ))
        .bearer_auth(&h.admin_token)
        .send()
        .await
        .expect("strategy reaches server");
    assert_eq!(
        res.status().as_u16(),
        200,
        "admin cross-tenant get must 200 (is_admin bypass): {}",
        res.text().await.unwrap_or_default()
    );
}
