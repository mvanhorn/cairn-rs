//! Cross-tenant DELETE / mutate tests — closes #405.
//!
//! # Why this file exists
//!
//! `test_http_cross_tenant.rs` covers LIST + CLAIM + READ for tasks
//! but not DELETE, cancel, complete, score, or any other mutation.
//! Audit #405 (2026-04-28) flagged that bug #337's shape —
//! "operator-of-tenant-B deletes / mutates a resource owned by
//! tenant-A" — was unverified for evals, tasks, sessions, runs,
//! credentials, and checkpoints.
//!
//! # Topology — single subprocess, two tenant scopes
//!
//! Copilot review round 1 correctly flagged that spinning up two
//! `LiveHarness` subprocesses each on `--db memory` gives you two
//! disjoint event logs + projections — so a seeded record in A is
//! invisible to B regardless of the tenant-visibility check. The
//! 404s would pass even if the handler's tenant check were removed.
//!
//! This file now uses a SINGLE subprocess. Seeding happens under
//! the harness's own uuid-scoped tenant/workspace/project (tenant
//! A). The attacker's principal is a NON-ADMIN operator token
//! minted via `POST /v1/auth/tokens` with a different tenant id
//! (tenant B). Because the operator is non-admin, the
//! `TenantScope` extractor carries `is_admin=false` and
//! `tenant_id=b_tenant`, so the handler's visibility check does
//! the actual work — the record exists in the same projection as
//! A's, so a tenant-check regression (e.g. dropping the
//! `load_run_visible_to_tenant` branch) would flip the 404 to a
//! 200/409 and the test would fail.
//!
//! # What this file does
//!
//! For each CRUD-bearing resource, seed it under tenant A's scope
//! via the harness admin token, then attempt a DELETE / cancel /
//! complete / mutate using a tenant-B operator token. Assert:
//!
//!   * Response status is 404 `not_found` — NOT 403, so we don't
//!     leak id-enumeration (matches #337 / #537 / PR #100 shape).
//!   * Resource under tenant A is unchanged after B's attempt.

mod support;

use serde_json::{json, Value};
use support::live_fabric::LiveHarness;

/// Mint a non-admin operator token scoped to `tenant_id`. Returns
/// the raw bearer token.
async fn mint_operator_token(h: &LiveHarness, operator_id: &str, tenant_id: &str) -> String {
    let r = h
        .client()
        .post(format!("{}/v1/auth/tokens", h.base_url))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "operator_id": operator_id,
            "tenant_id": tenant_id,
            "name": format!("xt-mutation-test-{operator_id}"),
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

/// A dedicated non-admin operator token scoped to a foreign tenant.
/// This is the "tenant B attacker" principal for every test below.
async fn foreign_tenant_operator_token(h: &LiveHarness) -> (String, String) {
    let foreign_tenant = format!("xt_attacker_{}", &h.project);
    let token = mint_operator_token(h, "xt_attacker_op", &foreign_tenant).await;
    (token, foreign_tenant)
}

/// Create a session + run under the harness's own (tenant-A) scope.
async fn seed_session_and_run(h: &LiveHarness) -> (String, String) {
    let session_id = format!("sess_{}", &h.project);
    let run_id = format!("run_{}", &h.project);

    let r = h
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
    assert_eq!(r.status().as_u16(), 201);

    let r = h
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
    assert_eq!(r.status().as_u16(), 201);

    (session_id, run_id)
}

async fn seed_task(h: &LiveHarness, run_id: &str) -> String {
    let task_id = format!("task_{}", &h.project);
    let r = h
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
    assert_eq!(r.status().as_u16(), 201);
    task_id
}

async fn seed_eval_run(h: &LiveHarness) -> String {
    let eval_run_id = format!("eval_{}", &h.project);
    let r = h
        .client()
        .post(format!("{}/v1/evals/runs", h.base_url))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "tenant_id": h.tenant,
            "workspace_id": h.workspace,
            "project_id": h.project,
            "eval_run_id": eval_run_id,
            "subject_kind": "prompt_release",
            "evaluator_type": "accuracy",
        }))
        .send()
        .await
        .expect("POST /v1/evals/runs reaches server");
    assert_eq!(
        r.status().as_u16(),
        201,
        "eval run create: {}",
        r.text().await.unwrap_or_default()
    );
    eval_run_id
}

async fn seed_credential(h: &LiveHarness) -> String {
    let provider_id = format!("openai-xt-{}", &h.project);
    let r = h
        .client()
        .post(format!(
            "{}/v1/admin/tenants/{}/credentials",
            h.base_url, "default_tenant",
        ))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "provider_id": provider_id,
            "plaintext_value": format!("sk-xt-{}", &h.project),
        }))
        .send()
        .await
        .expect("credential create reaches server");
    assert_eq!(
        r.status().as_u16(),
        201,
        "credential create: {}",
        r.text().await.unwrap_or_default()
    );
    let body: Value = r.json().await.expect("cred json");
    body["id"].as_str().expect("cred id").to_owned()
}

async fn status_only(
    h: &LiveHarness,
    method: reqwest::Method,
    url: &str,
    token: &str,
    body: Option<Value>,
) -> u16 {
    let mut req = h.client().request(method, url).bearer_auth(token);
    if let Some(b) = body {
        req = req.json(&b);
    }
    req.send()
        .await
        .expect("request reaches server")
        .status()
        .as_u16()
}

// ═══════════════════════════════════════════════════════════════════════════
// Run — tenant-B operator must get 404 cancelling tenant-A run
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn tenant_b_operator_cannot_cancel_tenant_a_run() {
    let h = LiveHarness::setup().await;
    let (_session_id, run_id) = seed_session_and_run(&h).await;
    let (b_token, _b_tenant) = foreign_tenant_operator_token(&h).await;

    // B's operator attempts cancel on A's run. The record exists in
    // the same projection the handler reads from, so any 404 must
    // come from the tenant-visibility check — not from a missing row.
    let status = status_only(
        &h,
        reqwest::Method::POST,
        &format!("{}/v1/runs/{}/cancel", h.base_url, run_id),
        &b_token,
        Some(json!({})),
    )
    .await;
    assert_eq!(
        status, 404,
        "tenant-B operator must get 404 cancelling A's run id={run_id}"
    );

    // A's run is still cancelable — B didn't side-effect it.
    let status = status_only(
        &h,
        reqwest::Method::POST,
        &format!("{}/v1/runs/{}/cancel", h.base_url, run_id),
        &h.admin_token,
        Some(json!({})),
    )
    .await;
    assert_eq!(status, 200, "A's cancel on its own run must still succeed");
}

// ═══════════════════════════════════════════════════════════════════════════
// Task — cross-tenant cancel + complete must be 404
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn tenant_b_operator_cannot_cancel_tenant_a_task() {
    let h = LiveHarness::setup().await;
    let (_session_id, run_id) = seed_session_and_run(&h).await;
    let task_id = seed_task(&h, &run_id).await;
    let (b_token, _b_tenant) = foreign_tenant_operator_token(&h).await;

    let status = status_only(
        &h,
        reqwest::Method::POST,
        &format!("{}/v1/tasks/{}/cancel", h.base_url, task_id),
        &b_token,
        Some(json!({})),
    )
    .await;
    assert_eq!(
        status, 404,
        "tenant-B operator must get 404 cancelling A's task id={task_id}"
    );

    // A still sees its task — B didn't silently archive it.
    let status = status_only(
        &h,
        reqwest::Method::GET,
        &format!("{}/v1/tasks/{}", h.base_url, task_id),
        &h.admin_token,
        None,
    )
    .await;
    assert_eq!(
        status, 200,
        "A's task must still be readable after B's spoof"
    );
}

#[tokio::test]
async fn tenant_b_operator_cannot_complete_tenant_a_task() {
    let h = LiveHarness::setup().await;
    let (_session_id, run_id) = seed_session_and_run(&h).await;
    let task_id = seed_task(&h, &run_id).await;
    let (b_token, _b_tenant) = foreign_tenant_operator_token(&h).await;

    let body = json!({
        "outcome": "success",
        "summary": "spoof from tenant B",
    });
    let status = status_only(
        &h,
        reqwest::Method::POST,
        &format!("{}/v1/tasks/{}/complete", h.base_url, task_id),
        &b_token,
        Some(body),
    )
    .await;
    assert_eq!(
        status, 404,
        "tenant-B operator must get 404 completing A's task id={task_id}"
    );
}

// ═══════════════════════════════════════════════════════════════════════════
// Eval run — cross-tenant DELETE / start / complete / score must be 404
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn tenant_b_operator_cannot_delete_tenant_a_eval_run() {
    let h = LiveHarness::setup().await;
    let eval_run_id = seed_eval_run(&h).await;
    let (b_token, b_tenant) = foreign_tenant_operator_token(&h).await;

    // B attempts DELETE with its OWN scope in query params.
    let url = format!(
        "{}/v1/evals/runs/{}?tenant_id={}&workspace_id=w&project_id=p",
        h.base_url, eval_run_id, b_tenant,
    );
    let status = status_only(&h, reqwest::Method::DELETE, &url, &b_token, None).await;
    assert_eq!(
        status, 404,
        "tenant-B operator must get 404 deleting A's eval run"
    );

    // B with A's scope claimed in query params must ALSO 404 — the
    // tenant-scope check runs against the projection's ProjectKey,
    // not the query oracle. This is the belt-and-suspenders check
    // added in this PR.
    let url = format!(
        "{}/v1/evals/runs/{}?tenant_id={}&workspace_id={}&project_id={}",
        h.base_url, eval_run_id, h.tenant, h.workspace, h.project,
    );
    let status = status_only(&h, reqwest::Method::DELETE, &url, &b_token, None).await;
    assert_eq!(
        status, 404,
        "tenant-B must get 404 even when claiming A's scope in query params \
         (projection-backed, not query-oracle)",
    );

    // A's eval run is still present and unarchived.
    let r = h
        .client()
        .get(format!("{}/v1/evals/runs/{}", h.base_url, eval_run_id))
        .bearer_auth(&h.admin_token)
        .send()
        .await
        .expect("A get own eval run");
    assert_eq!(
        r.status().as_u16(),
        200,
        "A's eval run must still exist after B's spoofed DELETE"
    );
    let body: Value = r.json().await.expect("eval run json");
    let archived = body
        .get("archived_at")
        .map(|v| !v.is_null())
        .unwrap_or(false);
    assert!(
        !archived,
        "A's eval run was archived by B's spoofed DELETE — #405 regression: {body}"
    );
}

#[tokio::test]
async fn tenant_b_operator_cannot_start_tenant_a_eval_run() {
    let h = LiveHarness::setup().await;
    let eval_run_id = seed_eval_run(&h).await;
    let (b_token, _b_tenant) = foreign_tenant_operator_token(&h).await;

    let status = status_only(
        &h,
        reqwest::Method::POST,
        &format!("{}/v1/evals/runs/{}/start", h.base_url, eval_run_id),
        &b_token,
        Some(json!({})),
    )
    .await;
    assert_eq!(
        status, 404,
        "tenant-B operator must get 404 starting A's eval run"
    );
}

#[tokio::test]
async fn tenant_b_operator_cannot_complete_tenant_a_eval_run() {
    let h = LiveHarness::setup().await;
    let eval_run_id = seed_eval_run(&h).await;
    let (b_token, _b_tenant) = foreign_tenant_operator_token(&h).await;

    // A starts so complete has a running run; then B attempts complete.
    let r = h
        .client()
        .post(format!(
            "{}/v1/evals/runs/{}/start",
            h.base_url, eval_run_id,
        ))
        .bearer_auth(&h.admin_token)
        .send()
        .await
        .expect("A start");
    assert_eq!(r.status().as_u16(), 200);

    let status = status_only(
        &h,
        reqwest::Method::POST,
        &format!("{}/v1/evals/runs/{}/complete", h.base_url, eval_run_id),
        &b_token,
        Some(json!({
            "metrics": { "accuracy": 1.0 },
            "cost": 0.0,
        })),
    )
    .await;
    assert_eq!(
        status, 404,
        "tenant-B operator must get 404 completing A's eval run"
    );

    // A's eval run status must still be Running.
    let r = h
        .client()
        .get(format!("{}/v1/evals/runs/{}", h.base_url, eval_run_id))
        .bearer_auth(&h.admin_token)
        .send()
        .await
        .expect("A get");
    let body: Value = r.json().await.expect("eval run json");
    let status_field = body
        .get("status")
        .and_then(|v| v.as_str())
        .unwrap_or("<missing>");
    assert_ne!(
        status_field, "completed",
        "A's eval run was silently completed by B's spoof: body={body}"
    );
}

#[tokio::test]
async fn tenant_b_operator_cannot_score_tenant_a_eval_run() {
    let h = LiveHarness::setup().await;
    let eval_run_id = seed_eval_run(&h).await;
    let (b_token, _b_tenant) = foreign_tenant_operator_token(&h).await;

    let status = status_only(
        &h,
        reqwest::Method::POST,
        &format!("{}/v1/evals/runs/{}/score", h.base_url, eval_run_id),
        &b_token,
        Some(json!({ "metrics": { "accuracy": 0.0 } })),
    )
    .await;
    assert_eq!(
        status, 404,
        "tenant-B operator must get 404 scoring A's eval run"
    );
}

// ═══════════════════════════════════════════════════════════════════════════
// Credential — cross-tenant revoke must be 403 (admin-only endpoint)
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn tenant_b_operator_cannot_revoke_tenant_a_credential() {
    let h = LiveHarness::setup().await;
    let cred_id = seed_credential(&h).await;
    let (b_token, _b_tenant) = foreign_tenant_operator_token(&h).await;

    // B attempts revoke. `revoke_credential_handler` is
    // AdminRoleGuard-protected, so a non-admin operator gets 403
    // BEFORE the tenant check even fires (the admin-role guard
    // fails closed first). This is the correct ordering — we don't
    // want to leak "credential exists" via 404 vs 403 for a non-
    // admin principal. Admin cross-tenant revoke is a separate
    // pattern (admin bypasses tenant) and is covered by the
    // handler's existing `existing.tenant_id != tenant_id` check.
    let url = format!(
        "{}/v1/admin/tenants/default_tenant/credentials/{}",
        h.base_url, cred_id,
    );
    let status = status_only(&h, reqwest::Method::DELETE, &url, &b_token, None).await;
    assert_eq!(
        status, 403,
        "tenant-B operator must get 403 revoking A's credential (AdminRoleGuard)"
    );

    // A can still revoke its own credential.
    let status = status_only(&h, reqwest::Method::DELETE, &url, &h.admin_token, None).await;
    assert_eq!(
        status, 200,
        "A's own revoke of its credential must still succeed"
    );
}

/// Closes #494: admin mis-addressing a credential under the wrong
/// `:tenant_id` path segment must return 404 (the 404-over-403
/// id-enumeration-safe convention), not 200, and not 403. This was
/// already enforced by the `existing.tenant_id != path_tenant_id`
/// check; the test pins the behaviour so the canonical-shape
/// refactor doesn't silently re-open the hole. See also:
/// `delete_session_admin_handler`, `list_credentials_handler`.
#[tokio::test]
async fn admin_cross_tenant_revoke_returns_404_when_path_tenant_mismatches_record() {
    let h = LiveHarness::setup().await;
    // Credential is seeded under `default_tenant` (see `seed_credential`).
    let cred_id = seed_credential(&h).await;

    // Admin addresses the credential under the WRONG :tenant_id path
    // segment. The handler must refuse with 404 — revealing neither
    // the existence of the credential id nor whether the other tenant
    // exists. A 200 here would mean an admin tool-typo silently
    // revoked a credential that wasn't the one it meant to address.
    let url = format!(
        "{}/v1/admin/tenants/{}/credentials/{}",
        h.base_url, "wrong_tenant_xt_494", cred_id,
    );
    let status = status_only(&h, reqwest::Method::DELETE, &url, &h.admin_token, None).await;
    assert_eq!(
        status, 404,
        "admin mis-addressing a credential under the wrong tenant_id must get 404, not 200/403",
    );

    // Confirm the credential still exists by revoking it under the
    // correct path — proves the first request didn't accidentally
    // revoke it even though it returned 404.
    let url = format!(
        "{}/v1/admin/tenants/{}/credentials/{}",
        h.base_url, "default_tenant", cred_id,
    );
    let status = status_only(&h, reqwest::Method::DELETE, &url, &h.admin_token, None).await;
    assert_eq!(
        status, 200,
        "credential must still be revocable under the correct tenant path",
    );
}

// ═══════════════════════════════════════════════════════════════════════════
// Checkpoint — cross-tenant restore must be 404
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn tenant_b_operator_cannot_restore_tenant_a_checkpoint() {
    let h = LiveHarness::setup().await;
    let (_session_id, run_id) = seed_session_and_run(&h).await;
    let checkpoint_id = format!("ckpt_{}", &h.project);

    // Create a checkpoint on A's run.
    let r = h
        .client()
        .post(format!("{}/v1/runs/{}/checkpoint", h.base_url, run_id))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "checkpoint_id": checkpoint_id,
            "strategy": "manual",
            "state_snapshot": { "step": 1 },
        }))
        .send()
        .await
        .expect("A checkpoint save");
    assert!(
        matches!(r.status().as_u16(), 200 | 201),
        "A's checkpoint save: {}",
        r.text().await.unwrap_or_default()
    );

    let (b_token, _b_tenant) = foreign_tenant_operator_token(&h).await;
    let url = format!("{}/v1/checkpoints/{}/restore", h.base_url, checkpoint_id);
    let status = status_only(&h, reqwest::Method::POST, &url, &b_token, Some(json!({}))).await;
    assert_eq!(
        status, 404,
        "tenant-B operator must get 404 restoring A's checkpoint"
    );
}
