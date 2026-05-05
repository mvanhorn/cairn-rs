//! #670 G4 PR-1b-2: operator `cancel-orphan` endpoint integration test.
//!
//! Exercises `POST /v1/admin/tenants/:tenant_id/runs/:id/cancel-orphan`
//! (RFC 027 §Orphan-child). The endpoint is the manual recovery path
//! for a child subagent run that leaked into `Pending` because
//! cairn-app crashed between Phase-1 (child row created) and Phase-2
//! (task submitted). No driver is shipped yet — PR-1b-3 adds the
//! automated path; this PR ships the class + endpoint that PR-1b-3
//! plumbs into and that operators can use immediately.
//!
//! # Coverage
//!
//! 1. **Happy path** — admin creates a root + a child with
//!    `parent_run_id = root`, both land `Pending` because no claim
//!    fires; admin POSTs cancel-orphan on the child → 204; subsequent
//!    `GET /v1/runs/:child` shows `state = failed` +
//!    `failure_class = orphan_child`.
//!
//! 2. **Cross-tenant 404** — cancel-orphan on a real run but naming
//!    the wrong tenant in the path returns 404 (not 403). The run
//!    stays `Pending`. Asserts admin cannot probe other tenants' run
//!    ids via this endpoint.
//!
//! 3. **Root rejection 422** — cancel-orphan on a root run (no
//!    `parent_run_id`) returns 422 with a validation message citing
//!    the child-only rule. The run stays `Pending`.
//!
//! 4. **Non-pending rejection 422** — transition a root to `Running`
//!    via `/claim`, then attempt cancel-orphan on it (after making
//!    it a child-shaped run conceptually — but we can only test the
//!    non-pending path on whatever is non-pending). Actually: we
//!    can't easily get a child into a non-pending state without
//!    spinning up the driver. Instead, we complete a child and
//!    assert cancel-orphan rejects a terminal child. Tests the state
//!    predicate independent of the parent_run_id predicate.
//!
//! 5. **Unknown run 404** — cancel-orphan on a nonexistent id returns
//!    404. (Sanity; the handler's first lookup catches this.)
//!
//! # Why the test uses admin-flavored endpoints
//!
//! The cancel-orphan endpoint is admin-only (TenantAdminGuard). The
//! test uses `bearer_auth(admin_token)` for both the setup POSTs and
//! the cancel-orphan itself — matching how operators would actually
//! use it.

mod support;

use serde_json::{json, Value};
use support::live_fabric::LiveHarness;

/// Create a session + a root run (no parent). Returns `(session_id, run_id)`.
async fn provision_root_run(
    h: &LiveHarness,
    session_suffix: &str,
    run_suffix: &str,
) -> (String, String) {
    let tenant = &h.tenant;
    let workspace = &h.workspace;
    let project = &h.project;
    let session_id = format!("sess_{session_suffix}");
    let run_id = format!("run_{run_suffix}");

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
    assert_eq!(r.status().as_u16(), 201, "session create must succeed");

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
    assert_eq!(r.status().as_u16(), 201, "root run create must succeed");

    (session_id, run_id)
}

/// Create a child run under an existing parent. Returns `child_run_id`.
async fn provision_child_run(
    h: &LiveHarness,
    session_id: &str,
    parent_run_id: &str,
    child_suffix: &str,
) -> String {
    let tenant = &h.tenant;
    let workspace = &h.workspace;
    let project = &h.project;
    let child_run_id = format!("run_child_{child_suffix}");

    let r = h
        .client()
        .post(format!("{}/v1/runs", h.base_url))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "tenant_id":     tenant,
            "workspace_id":  workspace,
            "project_id":    project,
            "session_id":    session_id,
            "run_id":        child_run_id,
            "parent_run_id": parent_run_id,
        }))
        .send()
        .await
        .expect("child run reaches server");
    assert_eq!(
        r.status().as_u16(),
        201,
        "child run create must succeed; body={:?}",
        r.text().await.unwrap_or_default(),
    );

    child_run_id
}

async fn get_run(h: &LiveHarness, run_id: &str) -> Value {
    let r = h
        .client()
        .get(format!("{}/v1/runs/{}", h.base_url, run_id))
        .bearer_auth(&h.admin_token)
        .send()
        .await
        .expect("get reaches server");
    assert_eq!(
        r.status().as_u16(),
        200,
        "get /v1/runs/{run_id} must succeed"
    );
    r.json().await.expect("get returns json")
}

#[tokio::test]
async fn cancel_orphan_happy_path_transitions_child_to_failed_orphan_child() {
    let h = LiveHarness::setup().await;
    let (session_id, root_run_id) = provision_root_run(&h, "happy", "root_happy").await;
    let child_run_id = provision_child_run(&h, &session_id, &root_run_id, "happy").await;

    // Pre-condition: child is Pending (no claim has fired).
    let before = get_run(&h, &child_run_id).await;
    assert_eq!(
        before
            .get("run")
            .and_then(|r| r.get("state"))
            .and_then(|s| s.as_str()),
        Some("pending"),
        "child must be Pending before cancel-orphan; body={before}",
    );

    // Admin cancel-orphan.
    let r = h
        .client()
        .post(format!(
            "{}/v1/admin/tenants/{}/runs/{}/cancel-orphan",
            h.base_url, h.tenant, child_run_id,
        ))
        .bearer_auth(&h.admin_token)
        .send()
        .await
        .expect("cancel-orphan reaches server");
    assert_eq!(
        r.status().as_u16(),
        204,
        "cancel-orphan must return 204 on happy path; body={:?}",
        r.text().await.unwrap_or_default(),
    );

    // Post-condition: child is Failed(OrphanChild).
    let after = get_run(&h, &child_run_id).await;
    let run = after.get("run").expect("detail body has run field");
    assert_eq!(
        run.get("state").and_then(|s| s.as_str()),
        Some("failed"),
        "cancel-orphan must transition child to failed; body={after}",
    );
    assert_eq!(
        run.get("failure_class").and_then(|s| s.as_str()),
        Some("orphan_child"),
        "cancel-orphan must set failure_class=orphan_child; body={after}",
    );
}

#[tokio::test]
async fn cancel_orphan_wrong_tenant_in_path_returns_404_and_leaves_pending() {
    let h = LiveHarness::setup().await;
    let (session_id, root_run_id) = provision_root_run(&h, "xtenant", "root_xtenant").await;
    let child_run_id = provision_child_run(&h, &session_id, &root_run_id, "xtenant").await;

    // Admin names a different tenant in the path. The run exists, but
    // not under `wrong_tenant`. The endpoint must 404 — NOT 403 — so
    // admin actions cannot be used to probe other tenants' run ids.
    let r = h
        .client()
        .post(format!(
            "{}/v1/admin/tenants/{}/runs/{}/cancel-orphan",
            h.base_url, "wrong_tenant_does_not_exist", child_run_id,
        ))
        .bearer_auth(&h.admin_token)
        .send()
        .await
        .expect("cancel-orphan reaches server");
    assert_eq!(
        r.status().as_u16(),
        404,
        "wrong-tenant path must 404, not 403; body={:?}",
        r.text().await.unwrap_or_default(),
    );

    // And the run is still Pending — no mutation on the rejected path.
    let detail = get_run(&h, &child_run_id).await;
    assert_eq!(
        detail
            .get("run")
            .and_then(|r| r.get("state"))
            .and_then(|s| s.as_str()),
        Some("pending"),
        "rejected cancel-orphan must not mutate the run",
    );
}

#[tokio::test]
async fn cancel_orphan_on_root_returns_422_and_leaves_pending() {
    let h = LiveHarness::setup().await;
    let (_session_id, root_run_id) = provision_root_run(&h, "root_only", "root_only").await;

    // The endpoint validates `parent_run_id.is_some()` BEFORE mutating.
    // Roots have no parent — there's no delegation that could have
    // leaked them — so they can't be "orphaned".
    let r = h
        .client()
        .post(format!(
            "{}/v1/admin/tenants/{}/runs/{}/cancel-orphan",
            h.base_url, h.tenant, root_run_id,
        ))
        .bearer_auth(&h.admin_token)
        .send()
        .await
        .expect("cancel-orphan reaches server");
    let status = r.status().as_u16();
    let body = r.text().await.unwrap_or_default();
    assert_eq!(
        status, 422,
        "cancel-orphan on a root must be 422; body={body}",
    );
    assert!(
        body.contains("parent_run_id") || body.contains("root"),
        "422 body must cite the child-only rule; body={body}",
    );

    // And the root is still Pending.
    let detail = get_run(&h, &root_run_id).await;
    assert_eq!(
        detail
            .get("run")
            .and_then(|r| r.get("state"))
            .and_then(|s| s.as_str()),
        Some("pending"),
        "rejected cancel-orphan must not mutate the root",
    );
}

#[tokio::test]
async fn cancel_orphan_on_unknown_run_returns_404() {
    let h = LiveHarness::setup().await;

    // No run with this id exists.
    let r = h
        .client()
        .post(format!(
            "{}/v1/admin/tenants/{}/runs/run_does_not_exist/cancel-orphan",
            h.base_url, h.tenant,
        ))
        .bearer_auth(&h.admin_token)
        .send()
        .await
        .expect("cancel-orphan reaches server");
    assert_eq!(
        r.status().as_u16(),
        404,
        "unknown run must be 404; body={:?}",
        r.text().await.unwrap_or_default(),
    );
}

#[tokio::test]
async fn cancel_orphan_on_non_pending_child_returns_422() {
    let h = LiveHarness::setup().await;
    let (session_id, root_run_id) = provision_root_run(&h, "nonpend", "root_nonpend").await;
    let child_run_id = provision_child_run(&h, &session_id, &root_run_id, "nonpend").await;

    // Drive the child terminal with `cancel` (a cairn-api lifecycle
    // op, distinct from this endpoint's admin-orphan-cancel). This
    // takes the child from `Pending` → `Canceled`. Now the state
    // predicate should reject cancel-orphan — the run isn't an
    // orphan, it's terminal.
    let r = h
        .client()
        .post(format!("{}/v1/runs/{}/cancel", h.base_url, child_run_id,))
        .bearer_auth(&h.admin_token)
        .header("X-Cairn-Tenant", &h.tenant)
        .header("X-Cairn-Workspace", &h.workspace)
        .header("X-Cairn-Project", &h.project)
        .send()
        .await
        .expect("cancel reaches server");
    assert_eq!(
        r.status().as_u16(),
        200,
        "canceling the child to drive it terminal must succeed; body={:?}",
        r.text().await.unwrap_or_default(),
    );

    // Now cancel-orphan on the terminal child. The state predicate
    // rejects with 422 citing the Pending-only rule.
    let r = h
        .client()
        .post(format!(
            "{}/v1/admin/tenants/{}/runs/{}/cancel-orphan",
            h.base_url, h.tenant, child_run_id,
        ))
        .bearer_auth(&h.admin_token)
        .send()
        .await
        .expect("cancel-orphan reaches server");
    let status = r.status().as_u16();
    let body = r.text().await.unwrap_or_default();
    assert_eq!(
        status, 422,
        "cancel-orphan on a terminal child must be 422; body={body}",
    );
    assert!(
        body.contains("Pending") || body.contains("pending") || body.contains("state"),
        "422 body must cite the Pending-only rule; body={body}",
    );

    // And the child is still Canceled (not re-transitioned to Failed).
    let detail = get_run(&h, &child_run_id).await;
    assert_eq!(
        detail
            .get("run")
            .and_then(|r| r.get("state"))
            .and_then(|s| s.as_str()),
        Some("canceled"),
        "rejected cancel-orphan must not mutate the terminal child",
    );
}
