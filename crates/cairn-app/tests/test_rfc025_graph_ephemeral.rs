//! RFC-025 Phase 1.5b — graph read-model is Ephemeral after replay_graph
//! removal.
//!
//! The boot walker that rebuilt the in-memory graph from the event log
//! was deleted. Post-phase semantics:
//!
//! 1. **Write path still populates the graph.** Every runtime event
//!    routed through `publish_runtime_frames_since` projects into
//!    `AppState.graph` (an `Arc<InMemoryGraphStore>`). So queries
//!    against entities created in the CURRENT process lifetime return
//!    their provenance.
//!
//! 2. **Restart clears the graph index.** On a SQLite-backed subprocess
//!    restart, the event log survives (sessions, runs, tasks all read
//!    back from the projection tables), but the derived graph index
//!    resets because it lives in process memory. Queries against
//!    pre-restart node IDs return empty subgraphs — that is the
//!    Ephemeral contract documented in RFC-025 Phase 1.5b.
//!
//! 3. **Post-restart writes re-populate the graph.** Appending a new
//!    event on a new run in subprocess B flows through the same
//!    `publish_runtime_frames_since` path, so the live graph index is
//!    maintained correctly — it simply starts empty on each boot.
//!
//! No mocks, no primitives under test — full cairn-app subprocess over
//! HTTP per the integration-tests-only contract
//! (`feedback_integration_tests_only.md`).
//!
//! ## Note on the write-path timing race (pre-existing)
//!
//! cairn-fabric's `EventBridge` emits runtime events on a tokio mpsc
//! and an async consumer task appends them to the store
//! (`event_bridge.rs::emit` awaits `tx.send` only; the consumer runs
//! in a separate task). When a handler calls
//! `publish_runtime_frames_since` immediately after a service method
//! returns, the emitted event may not have landed in the store yet, so
//! that handler's graph-projection call for that specific event may be
//! a no-op. The event lands later; a subsequent publish that captures
//! a `before` position BEFORE the landed event will catch up.
//!
//! This race is inherent to the EventBridge + publish_runtime_frames_since
//! design and is **unchanged by Phase 1.5b**. Before Phase 1.5b, the
//! `replay_graph` boot walker re-projected the entire event log on
//! startup, masking the race for pre-restart events. On the live write
//! path the same race was always present.
//!
//! What the tests assert:
//!   - Test A: the live write path populates the graph AT ALL — proves
//!     the deletion of `replay_graph` did not break the projection
//!     pipeline (asserts a Task node is visible, driven through a
//!     pacer write that forces the bridge to flush prior events).
//!   - Test B: after a SQLite-backed restart, the graph does NOT carry
//!     pre-restart node IDs — the core Phase 1.5b Ephemeral contract.
//!     And a post-restart write CAN surface its node through the
//!     live write path, proving the projection pipeline is live on
//!     subprocess B (not silently broken by the restart sequence).

mod support;

use std::time::Duration;

use serde_json::{json, Value};
use support::live_fabric::LiveHarness;

/// Drive a session + run through the API so the graph projector sees
/// SessionCreated + RunCreated events on the live write path.
async fn create_session_and_run(h: &LiveHarness, session_id: &str, run_id: &str) {
    let res = h
        .client()
        .post(format!("{}/v1/sessions", h.base_url))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "tenant_id":    h.tenant,
            "workspace_id": h.workspace,
            "project_id":   h.project,
            "session_id":   session_id,
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

    let res = h
        .client()
        .post(format!("{}/v1/runs", h.base_url))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "tenant_id":    h.tenant,
            "workspace_id": h.workspace,
            "project_id":   h.project,
            "session_id":   session_id,
            "run_id":       run_id,
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
}

/// Create a pacer task under the given run. The TaskCreated event
/// projects into the graph as a Task node + a run→task Spawned edge —
/// a reliable probe that the projection pipeline is functioning on the
/// current subprocess. Returns the task_id.
async fn create_pacer_task(h: &LiveHarness, run_id: &str) -> String {
    // UTF-8-safe truncation: `&run_id[..20]` can panic mid-codepoint
    // on non-ASCII ids. Take chars, not bytes. Today the ids this
    // test builds are all ASCII, but follow the safe pattern anyway
    // so a future test can reuse the helper with unicode suffixes.
    let run_prefix: String = run_id.chars().take(20).collect();
    let task_id = format!(
        "task_pacer_{}_{}",
        run_prefix,
        uuid::Uuid::new_v4().simple().to_string()[..8].to_owned(),
    );
    let res = h
        .client()
        .post(format!("{}/v1/tasks", h.base_url))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "tenant_id":     h.tenant,
            "workspace_id":  h.workspace,
            "project_id":    h.project,
            "task_id":       task_id,
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
    task_id
}

/// GET /v1/graph/execution-trace/:run_id and return (status, parsed body).
async fn fetch_execution_trace(h: &LiveHarness, run_id: &str) -> (u16, Value) {
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
    let status = res.status().as_u16();
    let body: Value = res.json().await.unwrap_or(Value::Null);
    (status, body)
}

/// Poll the execution-trace endpoint until the given task_id appears
/// in the subgraph traversed from `run_id`, or the deadline elapses.
///
/// Uses the Task node (created by a pacer write via `create_pacer_task`)
/// as the probe instead of the Run node, because the RunCreated ↔
/// pacer-publish timing race may leave the Run node unprojected while
/// the TaskCreated is reliably projected by the pacer's own
/// publish_runtime_frames_since call. The probe is sufficient: if
/// ANY node is visible in the subgraph, the live write-path is
/// functioning, which is the Phase 1.5b contract.
async fn poll_trace_contains_task(
    h: &LiveHarness,
    run_id: &str,
    task_id: &str,
    timeout: Duration,
) -> Result<Value, Value> {
    let deadline = tokio::time::Instant::now() + timeout;
    let mut last_body = Value::Null;
    while tokio::time::Instant::now() < deadline {
        let (status, body) = fetch_execution_trace(h, run_id).await;
        assert_eq!(
            status, 200,
            "GET graph/execution-trace must 200 even on empty result: {body}",
        );
        let nodes = body
            .get("nodes")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        if nodes
            .iter()
            .any(|n| n.get("node_id").and_then(Value::as_str) == Some(task_id))
        {
            return Ok(body);
        }
        last_body = body;
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Err(last_body)
}

// ── Test A: live write path populates the graph ──────────────────────────────
//
// Smoke test the RFC-025 Phase 1.5b invariant that deleting replay_graph
// did NOT break the live write-path projection. If this fails, every
// graph query is broken regardless of restart.

#[tokio::test]
async fn graph_index_populated_by_live_write_path() {
    let h = LiveHarness::setup().await;

    let session_id = format!("sess_rfc025_graph_{}", h.project);
    let run_id = format!("run_rfc025_graph_{}", h.project);

    create_session_and_run(&h, &session_id, &run_id).await;
    // Pacer task: its POST /v1/tasks handler publishes a frame window
    // starting before TaskCreated landed, which reliably projects the
    // Task node + run→task spawned edge. The probe is the Task node.
    let task_id = create_pacer_task(&h, &run_id).await;

    let body = poll_trace_contains_task(&h, &run_id, &task_id, Duration::from_secs(10))
        .await
        .unwrap_or_else(|last| {
            panic!(
                "task {task_id} (pacer under run {run_id}) never appeared in \
                 graph execution-trace — live write-path projection is broken. \
                 last body: {last}"
            )
        });

    // Verify the edge shape: the pacer task produced a run→task Spawned
    // edge (see cairn_graph::event_projector::EventProjector for
    // TaskCreated).
    let edges = body
        .get("edges")
        .and_then(Value::as_array)
        .expect("edges array on graph response");
    assert!(
        edges.iter().any(|e| {
            e.get("source_node_id").and_then(Value::as_str) == Some(run_id.as_str())
                && e.get("target_node_id").and_then(Value::as_str) == Some(task_id.as_str())
                && e.get("kind").and_then(Value::as_str) == Some("spawned")
        }),
        "expected run -> task Spawned edge missing: {body}",
    );
}

// ── Test B: sqlite restart — graph resets while store survives ───────────────
//
// The core Phase 1.5b contract. Drive a session + run on subprocess A
// (SQLite-backed), confirm the graph holds provenance. SIGKILL,
// restart on the same SQLite file. Assert:
//
//   1. The event log survived: GET /v1/sessions still lists the
//      pre-restart session (so the store projection works — baseline).
//   2. The graph INDEX is empty for the pre-restart run: GET
//      /v1/graph/execution-trace/:run_id returns an empty subgraph
//      (Ephemeral contract — replay_graph is gone, graph does not
//      rebuild on boot).
//   3. A new run created on subprocess B post-restart CAN surface in
//      its own execution-trace (live write path still works
//      post-restart).

#[tokio::test]
async fn graph_resets_on_restart_sqlite() {
    let mut h = LiveHarness::setup_with_sqlite().await;

    let session_a = format!("sess_rfc025a_{}", h.project);
    let run_a = format!("run_rfc025a_{}", h.project);

    // ── Subprocess A: create session + run + pacer task, confirm graph
    //    holds pacer-task provenance.
    create_session_and_run(&h, &session_a, &run_a).await;
    let task_a = create_pacer_task(&h, &run_a).await;
    poll_trace_contains_task(&h, &run_a, &task_a, Duration::from_secs(10))
        .await
        .unwrap_or_else(|last| {
            panic!(
                "task {task_a} never appeared in graph on subprocess A. \
                 last body: {last}"
            )
        });

    // Give sqlx's SQLite dual-writer a beat to flush the WAL to disk
    // before SIGKILL (same rationale as test_live_harness_sigkill.rs).
    tokio::time::sleep(Duration::from_millis(500)).await;

    // ── SIGKILL + restart on the same SQLite file.
    h.sigkill_and_restart()
        .await
        .expect("sigkill+restart succeeds");
    assert!(
        h.poll_readiness_until_ready(Duration::from_secs(5)).await,
        "subprocess B did not become ready within 5s",
    );

    // ── (1) Event log survived: the session list projection returns
    //        the pre-restart session on subprocess B. This is the
    //        baseline that proves SQLite persistence works; if this
    //        fails, the test infrastructure is broken, not the graph.
    let list_url = format!(
        "{}/v1/sessions?tenant_id={}&workspace_id={}&project_id={}",
        h.base_url, h.tenant, h.workspace, h.project,
    );
    let mut found_session = false;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while tokio::time::Instant::now() < deadline {
        let res = h
            .client()
            .get(&list_url)
            .bearer_auth(&h.admin_token)
            .send()
            .await
            .expect("sessions list on B");
        assert_eq!(res.status().as_u16(), 200);
        let body: Value = res.json().await.expect("list json B");
        let items = body
            .as_array()
            .cloned()
            .or_else(|| body.get("items").and_then(|v| v.as_array()).cloned())
            .expect("sessions list body is array or {items}");
        if items
            .iter()
            .any(|s| s.get("session_id").and_then(Value::as_str) == Some(session_a.as_str()))
        {
            found_session = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        found_session,
        "pre-restart session {session_a} must survive subprocess restart via store projection — \
         otherwise the store layer itself is broken and the graph test is meaningless",
    );

    // ── (2) Ephemeral contract: the graph index is empty for the
    //        pre-restart run. Ephemeral means "process-scoped cache,
    //        does not survive restart" — this is what Phase 1.5b
    //        formalises by deleting the boot walker. The Task node
    //        created on subprocess A MUST NOT appear under a query
    //        from run_a on subprocess B.
    let (status, body) = fetch_execution_trace(&h, &run_a).await;
    assert_eq!(
        status, 200,
        "graph query must 200 even with empty result: {body}",
    );
    let nodes_post = body
        .get("nodes")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    assert!(
        !nodes_post
            .iter()
            .any(|n| n.get("node_id").and_then(Value::as_str) == Some(task_a.as_str())),
        "pre-restart task {task_a} must NOT be in the graph index after restart — \
         that would mean replay_graph (or an equivalent boot rebuild) is still \
         running, defeating the Phase 1.5b Ephemeral declaration. body={body}",
    );
    assert!(
        !nodes_post
            .iter()
            .any(|n| n.get("node_id").and_then(Value::as_str) == Some(run_a.as_str())),
        "pre-restart run {run_a} must NOT be in the graph index after restart: {body}",
    );
    let edges_post = body
        .get("edges")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    assert!(
        edges_post.is_empty(),
        "pre-restart run must have no edges in the graph index after restart: {body}",
    );

    // ── (3) Post-restart writes still populate the graph. A new
    //        session + run + pacer task on subprocess B should
    //        surface the task node via run_b's execution-trace,
    //        proving publish_runtime_frames_since still wires the
    //        EventProjector end-to-end after restart.
    let session_b = format!("sess_rfc025b_{}", h.project);
    let run_b = format!("run_rfc025b_{}", h.project);
    create_session_and_run(&h, &session_b, &run_b).await;
    let task_b = create_pacer_task(&h, &run_b).await;

    let body_b = poll_trace_contains_task(&h, &run_b, &task_b, Duration::from_secs(10))
        .await
        .unwrap_or_else(|last| {
            panic!(
                "post-restart task {task_b} (under run {run_b}) never appeared \
                 in graph — live write-path projection broken on subprocess B. \
                 last body: {last}"
            )
        });
    let edges_b = body_b
        .get("edges")
        .and_then(Value::as_array)
        .expect("edges array on post-restart graph response");
    assert!(
        edges_b.iter().any(|e| {
            e.get("source_node_id").and_then(Value::as_str) == Some(run_b.as_str())
                && e.get("target_node_id").and_then(Value::as_str) == Some(task_b.as_str())
                && e.get("kind").and_then(Value::as_str) == Some("spawned")
        }),
        "post-restart run -> task Spawned edge missing: {body_b}",
    );
}
