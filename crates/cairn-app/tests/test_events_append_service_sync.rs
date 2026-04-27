//! HTTP integration tests for `/v1/events/append` → service-layer sync
//! (the split-brain fix).
//!
//! Background: before this fix `POST /v1/events/append` wrote the event
//! to the log + projections but did NOT populate service-layer state
//! (FF for tasks/runs/sessions). A subsequent mutation like
//! `POST /v1/tasks/:id/claim` would then return 404 because FF had no
//! record of the task even though `GET /v1/tasks/:id` happily
//! returned it from the projection.
//!
//! These tests pin the fix: after `events/append`, creation events must
//! also appear in the service layer so follow-up mutations succeed.
//! They also lock in the proper operator path (`POST /v1/tasks`) as the
//! contract surface for new tasks.

mod support;

use serde_json::json;
use support::live_fabric::LiveHarness;

/// Shared session + run setup so each task-creation test has a valid
/// parent run to bind the task's session against. Matches the smoke
/// test / dogfood sequencing.
async fn setup_session_and_run(h: &LiveHarness) -> (String, String) {
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

    (session_id, run_id)
}

/// Post a bare `TaskCreated` envelope via `/v1/events/append`, mirroring
/// the shape historically used by the smoke_worker fixture before it
/// was switched to the operator path. Returns the HTTP status for
/// caller assertions.
///
/// `event_id` is a caller-chosen suffix so duplicate-idempotency
/// tests can reuse the same `task_id` across calls while still
/// supplying a fresh `event_id` per append — the event-log schemas
/// (Postgres/SQLite) enforce `event_id` UNIQUE, so the test contract
/// for service-layer idempotency is "same task_id, different event_id".
async fn append_task_created(
    h: &LiveHarness,
    task_id: &str,
    run_id: &str,
    session_id: Option<&str>,
    event_id_suffix: &str,
) -> u16 {
    let mut payload = json!({
        "event": "task_created",
        "project": {
            "tenant_id": h.tenant,
            "workspace_id": h.workspace,
            "project_id": h.project,
        },
        "task_id": task_id,
        "parent_run_id": run_id,
        "parent_task_id": null,
        "prompt_release_id": null,
    });
    if let Some(sid) = session_id {
        payload["session_id"] = json!(sid);
    }
    let envelope = json!([{
        "event_id": format!("evt_t_{}_{}", task_id, event_id_suffix),
        "source": { "source_type": "runtime" },
        "ownership": {
            "scope": "project",
            "tenant_id": h.tenant,
            "workspace_id": h.workspace,
            "project_id": h.project,
        },
        "causation_id": null,
        "correlation_id": null,
        "payload": payload,
    }]);

    let res = h
        .client()
        .post(format!("{}/v1/events/append", h.base_url))
        .bearer_auth(&h.admin_token)
        .json(&envelope)
        .send()
        .await
        .expect("POST /v1/events/append reaches server");
    res.status().as_u16()
}

/// Bounded poll for a task to become visible via `GET /v1/tasks/:id`.
///
/// The bridge consumer that populates the cairn-store projection from
/// FF's `submit_task_execution` runs on a separate task, so the
/// projection is eventually-consistent with FF after a submit. A
/// fixed sleep would either flake under CI load or wait too long;
/// polling lets the test fail fast on a real sync gap while staying
/// resilient to scheduler variance.
async fn wait_for_task_visible(h: &LiveHarness, task_id: &str, label: &str) {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(3);
    loop {
        let res = h
            .client()
            .get(format!("{}/v1/tasks/{}", h.base_url, task_id))
            .bearer_auth(&h.admin_token)
            .send()
            .await
            .expect("GET /v1/tasks/:id reaches server");
        if res.status().as_u16() == 200 {
            return;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!(
                "{label}: task {task_id} not visible via GET /v1/tasks/:id within 3s (last status: {})",
                res.status().as_u16()
            );
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

/// Prong B: after `/v1/events/append` writes a `TaskCreated`, the
/// service-layer sync must make the task claimable. Pre-fix this test
/// returned 404 on claim because FF had no record of the task.
#[tokio::test]
async fn test_events_append_task_created_populates_service() {
    let h = LiveHarness::setup().await;
    let (session_id, run_id) = setup_session_and_run(&h).await;
    let task_id = format!("task_{}", &h.project);

    let status = append_task_created(&h, &task_id, &run_id, Some(&session_id), "a").await;
    assert_eq!(status, 201, "events/append status");

    // Poll until the projection catches up. The sync itself awaits FF
    // before events/append returns, but the bridge-driven projection
    // upsert runs on a separate task, so the test needs eventual
    // consistency semantics here.
    wait_for_task_visible(&h, &task_id, "after events/append").await;

    // List surface too — the task must show up under its own scope.
    let res = h
        .client()
        .get(format!(
            "{}/v1/tasks?tenant_id={}&workspace_id={}&project_id={}",
            h.base_url, h.tenant, h.workspace, h.project
        ))
        .bearer_auth(&h.admin_token)
        .send()
        .await
        .expect("GET /v1/tasks reaches server");
    assert_eq!(res.status().as_u16(), 200, "GET /v1/tasks");
    let body: serde_json::Value = res.json().await.expect("list json");
    let items = body["items"].as_array().expect("items array");
    assert!(
        items
            .iter()
            .any(|t| t["task_id"].as_str() == Some(task_id.as_str())),
        "task not in list after events/append: {body}",
    );

    // Service surface (the thing that was broken pre-fix): claim the
    // task. `claim_task_handler` → `load_task_visible_to_tenant`
    // bypasses tenant scope for admin tokens, so this exercises the
    // FF state directly. Pre-fix this returned 404 because FF had no
    // record of the task.
    let res = h
        .client()
        .post(format!("{}/v1/tasks/{}/claim", h.base_url, task_id))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "worker_id": "test_worker",
            "lease_duration_ms": 30_000u64,
        }))
        .send()
        .await
        .expect("POST /v1/tasks/:id/claim reaches server");
    assert_eq!(
        res.status().as_u16(),
        200,
        "claim after events/append: {}",
        res.text().await.unwrap_or_default()
    );
    let body: serde_json::Value = res.json().await.expect("claim json");
    let state = body["state"].as_str().unwrap_or("");
    assert!(
        matches!(state, "leased" | "running"),
        "task state after claim must be leased or running, got {state:?}: {body}",
    );
    // Lease fields must be populated by the claim.
    assert!(
        body["lease_owner"].as_str().is_some(),
        "lease_owner must be set after claim: {body}"
    );
    assert!(
        body["lease_expires_at"].as_u64().is_some(),
        "lease_expires_at must be set after claim: {body}"
    );
}

/// The operator path — `POST /v1/tasks` — is the contract surface for
/// creating new tasks. Pinning it here both documents the shape and
/// prevents a future refactor from silently re-introducing the
/// projection-only backdoor as the default.
#[tokio::test]
async fn test_post_tasks_creates_task_visible_to_list_and_claim() {
    let h = LiveHarness::setup().await;
    let (_session_id, run_id) = setup_session_and_run(&h).await;
    let task_id = format!("op_task_{}", &h.project);

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
            "priority": 0,
        }))
        .send()
        .await
        .expect("POST /v1/tasks reaches server");
    assert_eq!(
        res.status().as_u16(),
        201,
        "POST /v1/tasks: {}",
        res.text().await.unwrap_or_default()
    );

    // `POST /v1/tasks` returns as soon as FF confirms the submit, but
    // the cairn-store projection catches up via the async bridge
    // consumer — poll the per-id read before asserting list contents
    // so this test doesn't race the consumer.
    wait_for_task_visible(&h, &task_id, "after POST /v1/tasks").await;

    // List surface: the task must show up in /v1/tasks.
    let res = h
        .client()
        .get(format!(
            "{}/v1/tasks?tenant_id={}&workspace_id={}&project_id={}",
            h.base_url, h.tenant, h.workspace, h.project
        ))
        .bearer_auth(&h.admin_token)
        .send()
        .await
        .expect("GET /v1/tasks reaches server");
    assert_eq!(res.status().as_u16(), 200);
    let body: serde_json::Value = res.json().await.expect("list json");
    let items = body["items"].as_array().expect("items array");
    assert!(
        items
            .iter()
            .any(|t| t["task_id"].as_str() == Some(task_id.as_str())),
        "list response did not include created task: {body}",
    );

    // Claim surface: POST /v1/tasks goes through the service layer so
    // FF has the execution and claim returns 200.
    let res = h
        .client()
        .post(format!("{}/v1/tasks/{}/claim", h.base_url, task_id))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "worker_id": "test_worker",
            "lease_duration_ms": 30_000u64,
        }))
        .send()
        .await
        .expect("POST /v1/tasks/:id/claim reaches server");
    assert_eq!(
        res.status().as_u16(),
        200,
        "claim after POST /v1/tasks: {}",
        res.text().await.unwrap_or_default()
    );
}

/// Submitting the same `TaskCreated` twice via `/v1/events/append` must
/// not wedge the service layer. Both appends succeed (the log is
/// append-only and the service-sync is idempotent on task_id); the
/// task remains claimable exactly once.
#[tokio::test]
async fn test_events_append_duplicate_task_id_is_idempotent() {
    let h = LiveHarness::setup().await;
    let (session_id, run_id) = setup_session_and_run(&h).await;
    let task_id = format!("dup_task_{}", &h.project);

    // First append → 201.
    let s1 = append_task_created(&h, &task_id, &run_id, Some(&session_id), "a").await;
    assert_eq!(s1, 201, "first append");

    // Second append with the same `task_id` but a fresh `event_id`
    // (event-log schemas enforce `event_id` UNIQUE so repeating the
    // same id would be rejected by pg/sqlite before the service sync
    // even fires; the service-layer idempotency contract is "same
    // task_id, different event_id"). Still 201 — the second service
    // sync finds FF already has the task and is a silent no-op.
    let s2 = append_task_created(&h, &task_id, &run_id, Some(&session_id), "b").await;
    assert_eq!(s2, 201, "second append (idempotent)");

    wait_for_task_visible(&h, &task_id, "after duplicate events/append").await;

    // Task still claimable after the duplicate appends.
    let res = h
        .client()
        .post(format!("{}/v1/tasks/{}/claim", h.base_url, task_id))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "worker_id": "test_worker",
            "lease_duration_ms": 30_000u64,
        }))
        .send()
        .await
        .expect("claim reaches server");
    assert_eq!(
        res.status().as_u16(),
        200,
        "claim after duplicate events/append: {}",
        res.text().await.unwrap_or_default()
    );
}
