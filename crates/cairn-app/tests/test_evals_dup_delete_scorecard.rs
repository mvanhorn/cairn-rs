//! Issue #244 — evals surface gap-closing:
//!
//!   Finding 1: duplicate `eval_run_id` on `POST /v1/evals/runs` must 409
//!              (not 422). Mirrors #229 sessions + PR BA credentials.
//!   Finding 2: `DELETE /v1/evals/runs/:id` must soft-delete (204 first call,
//!              204 idempotent on re-delete, 404 cross-project/missing) and
//!              hide the archived row from the default list; the admin
//!              `?include_archived=true` flag must surface it back.
//!   Finding 3: `GET /v1/evals/scorecards` must expose the summary list the
//!              EvalsPage scorecard picker consumes, including shape
//!              `{items: [...], has_more: bool}` so the UI `getList()`
//!              normaliser works.
//!
//! Before #244: POST with a colliding id blew up with a
//! UNIQUE(event_id) constraint error surfaced as 422; no DELETE route
//! existed; no scorecard picker backend existed. All three regress the
//! EvalsPage operator UX (#244 root cause trace).
//!
//! The tests here are `LiveHarness` integration tests so the full
//! router → handler → event-log → projection stack participates. Each test
//! owns a uuid-scoped tenant/workspace/project so they can run in parallel
//! on the shared cairn-app process.

mod support;

use serde_json::{json, Value};
use support::live_fabric::LiveHarness;

/// Minimal POST /v1/evals/runs body. Bindings are omitted because the
/// dup/delete/list paths under test don't depend on them; the full
/// binding contract is locked in `test_http_evals_full.rs`.
fn create_payload(h: &LiveHarness, eval_run_id: &str) -> Value {
    json!({
        "tenant_id":      h.tenant,
        "workspace_id":   h.workspace,
        "project_id":     h.project,
        "eval_run_id":    eval_run_id,
        "subject_kind":   "prompt_release",
        "evaluator_type": "accuracy",
    })
}

// ── Finding 1: duplicate eval_run_id → 409 ───────────────────────────────────

#[tokio::test]
async fn duplicate_eval_run_id_returns_409_same_project() {
    let h = LiveHarness::setup().await;
    let eval_run_id = format!("er-{}", uuid::Uuid::new_v4().simple());

    // First create: 201.
    let r = h
        .client()
        .post(format!("{}/v1/evals/runs", h.base_url))
        .bearer_auth(&h.admin_token)
        .json(&create_payload(&h, &eval_run_id))
        .send()
        .await
        .expect("first create reaches server");
    assert_eq!(
        r.status().as_u16(),
        201,
        "first create must 201: body={}",
        r.text().await.unwrap_or_default(),
    );

    // Duplicate: must be 409, not 422 (the pre-#244 bug).
    let r = h
        .client()
        .post(format!("{}/v1/evals/runs", h.base_url))
        .bearer_auth(&h.admin_token)
        .json(&create_payload(&h, &eval_run_id))
        .send()
        .await
        .expect("duplicate create reaches server");
    let status = r.status().as_u16();
    let body = r.text().await.unwrap_or_default();
    assert_eq!(
        status, 409,
        "duplicate eval_run_id must 409 (was 422 pre-#244): body={body}",
    );
    assert!(
        body.contains(&eval_run_id),
        "409 envelope should name the colliding id: body={body}",
    );
}

#[tokio::test]
async fn duplicate_eval_run_id_across_projects_returns_409() {
    // Two disjoint project scopes on the same harness (same tenant +
    // workspace, different project_id). Re-using an id across projects is
    // still a collision because eval_run_ids are globally unique.
    let h = LiveHarness::setup().await;
    let eval_run_id = format!("er-xp-{}", uuid::Uuid::new_v4().simple());

    // Create in the harness's canonical project.
    let r = h
        .client()
        .post(format!("{}/v1/evals/runs", h.base_url))
        .bearer_auth(&h.admin_token)
        .json(&create_payload(&h, &eval_run_id))
        .send()
        .await
        .expect("first create reaches server");
    assert_eq!(r.status().as_u16(), 201);

    // Re-POST with a different project_id — still must 409 because the
    // eval_run_id is already bound to another project.
    let other_project = format!("p_other_{}", uuid::Uuid::new_v4().simple());
    let mut body = create_payload(&h, &eval_run_id);
    body["project_id"] = json!(other_project);
    let r = h
        .client()
        .post(format!("{}/v1/evals/runs", h.base_url))
        .bearer_auth(&h.admin_token)
        .json(&body)
        .send()
        .await
        .expect("cross-project duplicate reaches server");
    let status = r.status().as_u16();
    let txt = r.text().await.unwrap_or_default();
    assert_eq!(status, 409, "cross-project duplicate must 409: body={txt}",);
    assert!(
        txt.contains("another project"),
        "cross-project message should flag the tenant-isolation implication: body={txt}",
    );
}

// ── Finding 2: DELETE soft-delete + include_archived ─────────────────────────

#[tokio::test]
async fn delete_eval_run_soft_deletes_hides_from_list_and_surfaces_via_flag() {
    let h = LiveHarness::setup().await;
    let eval_run_id = format!("er-del-{}", uuid::Uuid::new_v4().simple());

    // Seed.
    let r = h
        .client()
        .post(format!("{}/v1/evals/runs", h.base_url))
        .bearer_auth(&h.admin_token)
        .json(&create_payload(&h, &eval_run_id))
        .send()
        .await
        .expect("seed create");
    assert_eq!(r.status().as_u16(), 201);

    // Pre-delete: the run appears in the default list.
    let list_url = format!(
        "{}/v1/evals/runs?tenant_id={}&workspace_id={}&project_id={}",
        h.base_url, h.tenant, h.workspace, h.project
    );
    let r = h
        .client()
        .get(&list_url)
        .bearer_auth(&h.admin_token)
        .send()
        .await
        .expect("pre-delete list");
    assert_eq!(r.status().as_u16(), 200);
    let body: Value = r.json().await.expect("list json");
    let items = body
        .get("items")
        .and_then(|v| v.as_array())
        .expect("items[]");
    assert!(
        items.iter().any(|it| {
            it.get("eval_run_id").and_then(|v| v.as_str()) == Some(eval_run_id.as_str())
        }),
        "seeded run must appear in default list pre-delete: {items:?}",
    );

    // DELETE.
    let delete_url = format!(
        "{}/v1/evals/runs/{eval_run_id}?tenant_id={}&workspace_id={}&project_id={}",
        h.base_url, h.tenant, h.workspace, h.project
    );
    let r = h
        .client()
        .delete(&delete_url)
        .bearer_auth(&h.admin_token)
        .send()
        .await
        .expect("delete");
    assert_eq!(
        r.status().as_u16(),
        204,
        "first DELETE must 204: body={}",
        r.text().await.unwrap_or_default(),
    );

    // Post-delete default list: archived run is hidden.
    let r = h
        .client()
        .get(&list_url)
        .bearer_auth(&h.admin_token)
        .send()
        .await
        .expect("post-delete default list");
    assert_eq!(r.status().as_u16(), 200);
    let body: Value = r.json().await.expect("list json");
    let items = body
        .get("items")
        .and_then(|v| v.as_array())
        .expect("items[]");
    assert!(
        !items.iter().any(|it| {
            it.get("eval_run_id").and_then(|v| v.as_str()) == Some(eval_run_id.as_str())
        }),
        "archived run must be hidden from default list: {items:?}",
    );

    // include_archived=true surfaces it back with archived_at set.
    let admin_url = format!("{list_url}&include_archived=true");
    let r = h
        .client()
        .get(&admin_url)
        .bearer_auth(&h.admin_token)
        .send()
        .await
        .expect("include_archived list");
    assert_eq!(r.status().as_u16(), 200);
    let body: Value = r.json().await.expect("list json");
    let items = body
        .get("items")
        .and_then(|v| v.as_array())
        .expect("items[]");
    let archived = items
        .iter()
        .find(|it| it.get("eval_run_id").and_then(|v| v.as_str()) == Some(eval_run_id.as_str()))
        .expect("archived run visible with include_archived=true");
    assert!(
        archived
            .get("archived_at")
            .and_then(|v| v.as_u64())
            .is_some(),
        "archived_at timestamp must be populated after DELETE: {archived:?}",
    );

    // Re-DELETE is idempotent — still 204, no duplicate archive event.
    let r = h
        .client()
        .delete(&delete_url)
        .bearer_auth(&h.admin_token)
        .send()
        .await
        .expect("re-delete");
    assert_eq!(
        r.status().as_u16(),
        204,
        "re-delete must stay 204 (idempotent): body={}",
        r.text().await.unwrap_or_default(),
    );
}

#[tokio::test]
async fn delete_missing_eval_run_returns_404() {
    let h = LiveHarness::setup().await;
    let url = format!(
        "{}/v1/evals/runs/er-does-not-exist?tenant_id={}&workspace_id={}&project_id={}",
        h.base_url, h.tenant, h.workspace, h.project
    );
    let r = h
        .client()
        .delete(&url)
        .bearer_auth(&h.admin_token)
        .send()
        .await
        .expect("delete missing");
    assert_eq!(
        r.status().as_u16(),
        404,
        "missing eval-run DELETE must 404: body={}",
        r.text().await.unwrap_or_default(),
    );
}

#[tokio::test]
async fn delete_cross_project_returns_404() {
    // Seed under the harness project scope, then attempt to DELETE from a
    // different project scope. Must 404 — a cross-project DELETE silently
    // archiving another project's run is a tenant-isolation bug.
    let h = LiveHarness::setup().await;
    let eval_run_id = format!("er-xp-del-{}", uuid::Uuid::new_v4().simple());

    let r = h
        .client()
        .post(format!("{}/v1/evals/runs", h.base_url))
        .bearer_auth(&h.admin_token)
        .json(&create_payload(&h, &eval_run_id))
        .send()
        .await
        .expect("seed create");
    assert_eq!(r.status().as_u16(), 201);

    let other_project = format!("p_xp_{}", uuid::Uuid::new_v4().simple());
    let url = format!(
        "{}/v1/evals/runs/{eval_run_id}?tenant_id={}&workspace_id={}&project_id={}",
        h.base_url, h.tenant, h.workspace, other_project
    );
    let r = h
        .client()
        .delete(&url)
        .bearer_auth(&h.admin_token)
        .send()
        .await
        .expect("cross-project delete");
    assert_eq!(
        r.status().as_u16(),
        404,
        "cross-project DELETE must 404, not silent 204: body={}",
        r.text().await.unwrap_or_default(),
    );
}

// ── Finding 3: scorecard picker backend ─────────────────────────────────────

#[tokio::test]
async fn list_scorecards_returns_list_shape_ui_expects() {
    let h = LiveHarness::setup().await;
    let url = format!(
        "{}/v1/evals/scorecards?tenant_id={}&workspace_id={}&project_id={}",
        h.base_url, h.tenant, h.workspace, h.project
    );

    let r = h
        .client()
        .get(&url)
        .bearer_auth(&h.admin_token)
        .send()
        .await
        .expect("list scorecards reaches server");
    assert_eq!(
        r.status().as_u16(),
        200,
        "GET /v1/evals/scorecards must 200 (route registered per #244): body={}",
        r.text().await.unwrap_or_default(),
    );
    let body: Value = r.json().await.expect("scorecards json");

    // UI `getList()` normaliser wants either `{items, has_more}` or a bare
    // array. We ship the first shape — assert both keys are present.
    let items = body
        .get("items")
        .and_then(|v| v.as_array())
        .expect("scorecards body must have items[] for UI getList()");
    assert!(
        body.get("has_more").is_some() || body.get("hasMore").is_some(),
        "scorecards body must have has_more/hasMore for UI getList(): {body}",
    );

    // Shape of each row matches ScorecardSummary — the UI picker relies on
    // prompt_asset_id, entry_count, best_task_success_rate. On an empty
    // project the array is empty but the shape still renders; this test
    // exists mainly so the shape regression is caught, so a single row
    // assertion is enough when one exists.
    for row in items {
        assert!(
            row.get("project_id").and_then(|v| v.as_str()).is_some(),
            "scorecard row missing project_id: {row}",
        );
        assert!(
            row.get("prompt_asset_id")
                .and_then(|v| v.as_str())
                .is_some(),
            "scorecard row missing prompt_asset_id: {row}",
        );
        assert!(
            row.get("entry_count").and_then(|v| v.as_u64()).is_some(),
            "scorecard row missing entry_count: {row}",
        );
    }
}
