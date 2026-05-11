//! Issue #568: `EventBridge` read-after-write semantics — deterministic
//! integration test.
//!
//! Pre-fix the bridge was a pipe: `bridge.emit()` awaited `tx.send()`
//! only, with an async consumer appending to the event store on a
//! separate task. A handler that called `publish_runtime_frames_since`
//! immediately after `emit` could miss its own event because the
//! store's head hadn't advanced yet.
//!
//! The fix adds `EventBridge::flush()` (a FIFO sentinel on the same
//! consumer channel) and routes `publish_runtime_frames_since` through
//! it, so every caller of that helper sees the event it just produced.
//!
//! ## What this test proves
//!
//! Per `feedback_integration_tests_only.md` the durability claim can
//! only be proven through the live HTTP surface, not a bridge-internal
//! mock. The probe here is the `execution-trace` graph endpoint:
//!
//!   1. Handler does `bridge.emit(RunCreated + TaskCreated)` inside
//!      the service methods.
//!   2. Same handler calls `publish_runtime_frames_since(before)`
//!      before responding.
//!   3. `publish_runtime_frames_since` now awaits `bridge.flush()`
//!      (the fix), so both events have landed in the store before the
//!      handler reads them back to project into the graph.
//!   4. The *very next* GET /v1/graph/execution-trace/:run_id sees
//!      the Task node — WITHOUT a polling loop, WITHOUT a pacer
//!      write, WITHOUT a retry.
//!
//! The existing `test_rfc025_graph_ephemeral.rs` used a pacer-task
//! probe + 10-second polling window to sidestep this race. This test
//! deliberately does NOT poll or pace: one write, one immediate read,
//! assertion succeeds on the first attempt.
//!
//! If this test flakes, the fix is broken — that's the entire point.

mod support;

use serde_json::{json, Value};
use support::live_fabric::LiveHarness;

/// One-shot read-after-write: create a session + run + task, then
/// immediately GET /v1/graph/execution-trace/:run_id. The task node
/// and the run→task Spawned edge must be visible on the first
/// response with no retries.
#[tokio::test]
async fn execution_trace_visible_immediately_after_task_create() {
    let h = LiveHarness::setup().await;

    let session_id = format!("sess_bridge_raw_{}", h.project);
    let run_id = format!("run_bridge_raw_{}", h.project);

    // ── Session create
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
        .expect("POST /v1/sessions");
    assert_eq!(
        res.status().as_u16(),
        201,
        "session create: {}",
        res.text().await.unwrap_or_default(),
    );

    // ── Run create
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
        .expect("POST /v1/runs");
    assert_eq!(
        res.status().as_u16(),
        201,
        "run create: {}",
        res.text().await.unwrap_or_default(),
    );

    // ── Task create — after this POST returns, the TaskCreated event
    //    must be visible in the projection because
    //    `publish_runtime_frames_since` now awaits `bridge.flush()`.
    let task_id = format!("task_bridge_raw_{}", h.project);
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
        .expect("POST /v1/tasks");
    assert_eq!(
        res.status().as_u16(),
        201,
        "task create: {}",
        res.text().await.unwrap_or_default(),
    );

    // ── Immediate read — no poll loop, no pacer, no retry. The fix
    //    guarantees the task node has landed in the graph by the time
    //    the task-create handler returned 201.
    let res = h
        .client()
        .get(format!(
            "{}/v1/graph/execution-trace/{}",
            h.base_url, run_id,
        ))
        .bearer_auth(&h.admin_token)
        .send()
        .await
        .expect("GET /v1/graph/execution-trace/:run_id");
    assert_eq!(res.status().as_u16(), 200);
    let body: Value = res.json().await.expect("execution-trace body");

    let nodes = body
        .get("nodes")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    assert!(
        nodes
            .iter()
            .any(|n| n.get("node_id").and_then(Value::as_str) == Some(task_id.as_str())),
        "task {task_id} must be visible in the graph on the first read after \
         POST /v1/tasks returned — `EventBridge::flush` inside \
         `publish_runtime_frames_since` drains the bridge mpsc before the \
         handler responds. body={body}"
    );

    let edges = body
        .get("edges")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    assert!(
        edges.iter().any(|e| {
            e.get("source_node_id").and_then(Value::as_str) == Some(run_id.as_str())
                && e.get("target_node_id").and_then(Value::as_str) == Some(task_id.as_str())
                && e.get("kind").and_then(Value::as_str) == Some("spawned")
        }),
        "run→task Spawned edge must be present on the first read: {body}",
    );
}

/// Same contract under a tight loop — any race re-introduced at a
/// future refactor shows up here as a non-deterministic failure. Each
/// iteration is a fresh run + task + read; no pacer, no retry. 20
/// iterations is enough to surface a ~5% flake within one test run.
#[tokio::test]
async fn execution_trace_read_after_write_stable_over_iterations() {
    let h = LiveHarness::setup().await;

    let session_id = format!("sess_bridge_loop_{}", h.project);
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
        .expect("POST /v1/sessions");
    assert_eq!(res.status().as_u16(), 201);

    const ITERATIONS: usize = 20;
    for i in 0..ITERATIONS {
        let run_id = format!("run_bridge_loop_{}_{i:03}", h.project);
        let task_id = format!("task_bridge_loop_{}_{i:03}", h.project);

        // Run create
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
            .expect("POST /v1/runs");
        assert_eq!(res.status().as_u16(), 201, "iter {i}: run create");

        // Task create
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
            .expect("POST /v1/tasks");
        assert_eq!(res.status().as_u16(), 201, "iter {i}: task create");

        // Immediate read — must see the task on the FIRST try.
        let res = h
            .client()
            .get(format!(
                "{}/v1/graph/execution-trace/{}",
                h.base_url, run_id,
            ))
            .bearer_auth(&h.admin_token)
            .send()
            .await
            .expect("GET execution-trace");
        assert_eq!(res.status().as_u16(), 200, "iter {i}: execution-trace");
        let body: Value = res.json().await.expect("execution-trace body");
        let nodes = body
            .get("nodes")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        assert!(
            nodes
                .iter()
                .any(|n| n.get("node_id").and_then(Value::as_str) == Some(task_id.as_str())),
            "iter {i}: task {task_id} missing on first read — \
             read-after-write barrier (#568) regressed. body={body}"
        );
    }
}
