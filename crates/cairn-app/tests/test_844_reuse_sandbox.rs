//! #844 PR-2: `reuse_sandbox_from` — explicit opt-in sandbox inheritance
//! on `spawn_subagent`.
//!
//! Three invariants pinned here:
//!
//! 1. **Happy path.** When the orchestrator LLM emits
//!    `spawn_subagent` with `reuse_sandbox_from = <valid prior
//!    sibling run_id>`, the adapter persists a
//!    `run:<child>:reuse_sandbox_from` default whose value is the
//!    prior sibling's run_id verbatim. That's the row the child's
//!    next orchestrate iteration reads to redirect
//!    `working_dir_for_run` at the sibling's sandbox.
//!
//! 2. **Cross-root rejection.** When the target run belongs to a
//!    different root, the adapter rejects the spawn at validation
//!    time — no child row is created and the failure surfaces via
//!    `ActionStatus::Failed.reason` into step_history so the LLM
//!    can correct.
//!
//! 3. **No row on omit.** When the LLM does NOT supply the field,
//!    no default is written — absence is the fresh-sandbox signal,
//!    and writing an empty row would observationally confuse
//!    operators reading the settings surface.
//!
//! Issue: https://github.com/avifenesh/cairn-rs/issues/844

mod support;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use axum::{
    extract::State,
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use serde_json::{json, Value};
use support::live_fabric::LiveHarness;

const MOCK_MODEL: &str = "openrouter/844-reuse-sandbox-from";
const DELEGATED_ROLE: &str = "executor";

/// Mock LLM whose plan is a live `Mutex<Vec<Value>>` so the test
/// can inject a dynamic run_id into iteration N after observing it
/// post-iteration N-1. Without this, the happy-path test can't
/// reference child A's id inside child B's spawn — child ids are
/// minted at dispatch and only become visible via
/// `/v1/runs/.../children` after the fact.
#[derive(Clone)]
struct MockState {
    hits: Arc<AtomicUsize>,
    plan: Arc<Mutex<Vec<serde_json::Value>>>,
}

async fn chat_handler(
    State(state): State<MockState>,
    Json(_body): Json<Value>,
) -> (StatusCode, Json<Value>) {
    let n = state.hits.fetch_add(1, Ordering::SeqCst);
    let default_complete = json!([{
        "action_type":       "complete_run",
        "description":       "default complete",
        "confidence":        0.99,
        "requires_approval": false,
    }]);
    let content = {
        let plan = state.plan.lock().unwrap();
        plan.get(n).cloned().unwrap_or(default_complete)
    };
    (
        StatusCode::OK,
        Json(json!({
            "id":      format!("mock-844-{n}"),
            "choices": [{
                "index":   0,
                "message": {
                    "role":    "assistant",
                    "content": content.to_string(),
                },
                "finish_reason": "stop",
            }],
            "usage": {
                "prompt_tokens":     10,
                "completion_tokens": 6,
                "total_tokens":      16,
            },
        })),
    )
}

async fn models_handler() -> Json<Value> {
    Json(json!({ "data": [{ "id": MOCK_MODEL }] }))
}

async fn spawn_mock(
    plan: Vec<serde_json::Value>,
) -> (String, Arc<AtomicUsize>, Arc<Mutex<Vec<Value>>>) {
    let plan = Arc::new(Mutex::new(plan));
    let state = MockState {
        hits: Arc::new(AtomicUsize::new(0)),
        plan: plan.clone(),
    };
    let hits = state.hits.clone();
    let app = Router::new()
        .route("/chat/completions", post(chat_handler))
        .route("/v1/chat/completions", post(chat_handler))
        .route("/v1/models", get(models_handler))
        .with_state(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.ok();
    });
    (format!("http://{addr}"), hits, plan)
}

async fn provision_run(
    h: &LiveHarness,
    mock_url: &str,
    scenario: &str,
) -> (String, String, String) {
    let suffix = h.project.clone();
    let tenant = "default_tenant".to_owned();
    let workspace = "default_workspace".to_owned();
    let project = "default_project".to_owned();
    let connection_id = format!("conn_844_{scenario}_{suffix}");
    let session_id = format!("sess_844_{scenario}_{suffix}");
    let run_id = format!("run_844_{scenario}_{suffix}");

    let r = h
        .client()
        .post(format!(
            "{}/v1/admin/tenants/{}/credentials",
            h.base_url, tenant
        ))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "provider_id":     "openrouter",
            "plaintext_value": format!("sk-844-{suffix}"),
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
            .expect("defaults reach server");
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

    (session_id, run_id, project)
}

/// #844 PR-2 happy path: a single root-level parent run executes
/// TWO consecutive `spawn_subagent` proposals in the same
/// orchestrate loop. The second spawn's `reuse_sandbox_from` is
/// filled in by the test harness DYNAMICALLY with child A's real
/// run_id, which only becomes observable after child A is minted
/// at DECIDE iteration 0.
///
/// Because child A and child B share a parent (and therefore a
/// root), the adapter's same-root check passes, the child row is
/// created, and the `run:<B>:reuse_sandbox_from` default is written
/// with A's run_id verbatim.
///
/// This is the core contract #844 PR-2 adds — a replacement sub-
/// agent can be wired to a predecessor's on-disk state, instead of
/// every re-spawn landing in a fresh empty directory.
#[tokio::test]
async fn spawn_subagent_persists_reuse_sandbox_from_for_same_root_sibling() {
    let h = LiveHarness::setup().await;

    // Plan iter 0: spawn child A (no reuse).
    // Plan iter 1: placeholder — will be overwritten with child A's
    //              real run_id once it's observable.
    // Plan iter 2: complete_run.
    let placeholder_reuse_id = "PLACEHOLDER_WILL_BE_REPLACED";
    let initial_plan = vec![
        json!([{
            "action_type":       "spawn_subagent",
            "description":       "#844 PR-2 iter 0 — seed child A",
            "tool_name":         DELEGATED_ROLE,
            "tool_args":         { "goal": "child A: initial attempt" },
            "confidence":        0.95,
            "requires_approval": false,
        }]),
        json!([{
            "action_type":       "spawn_subagent",
            "description":       "#844 PR-2 iter 1 — replacement B reuses A's sandbox",
            "tool_name":         DELEGATED_ROLE,
            "tool_args":         {
                "goal":               "child B: continue A's partial work",
                "reuse_sandbox_from": placeholder_reuse_id,
            },
            "confidence":        0.95,
            "requires_approval": false,
        }]),
        json!([{
            "action_type":       "complete_run",
            "description":       "parent done",
            "confidence":        0.99,
            "requires_approval": false,
        }]),
    ];
    let (mock_url, hits, plan_handle) = spawn_mock(initial_plan).await;
    let (_session_id, parent_run_id, project_id) = provision_run(&h, &mock_url, "happy").await;

    // Phase 1: drive the loop until iter 0 lands (child A exists)
    // but BEFORE iter 1 fires. Easiest way: orchestrate with
    // max_iterations=1. That runs one full DECIDE+EXECUTE cycle.
    let r = h
        .client()
        .post(format!(
            "{}/v1/runs/{}/orchestrate",
            h.base_url, parent_run_id
        ))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "goal": "#844 PR-2 happy-path parent goal",
            "max_iterations": 1,
        }))
        .send()
        .await
        .expect("orchestrate phase 1 reaches server");
    let status = r.status().as_u16();
    assert!(matches!(status, 200 | 202), "phase 1 status={status}");
    assert_eq!(
        hits.load(Ordering::SeqCst),
        1,
        "phase 1 must consume exactly one LLM call (iter 0)",
    );

    // Observe child A's real run_id.
    let r = h
        .client()
        .get(format!("{}/v1/runs/{}/children", h.base_url, parent_run_id))
        .bearer_auth(&h.admin_token)
        .send()
        .await
        .expect("children reaches server");
    assert_eq!(r.status().as_u16(), 200);
    let body: Value = r.json().await.unwrap();
    let items = body
        .get("items")
        .and_then(|v| v.as_array())
        .expect("children items");
    assert_eq!(items.len(), 1, "phase 1: one child (A) expected");
    let child_a_run_id = items[0]
        .get("run_id")
        .and_then(|v| v.as_str())
        .expect("child A run_id")
        .to_owned();

    // Swap the placeholder for child A's real run_id in the
    // mutable plan so iter 1's reuse_sandbox_from points at a
    // genuine sibling under the same root.
    {
        let mut plan = plan_handle.lock().unwrap();
        plan[1] = json!([{
            "action_type":       "spawn_subagent",
            "description":       "#844 PR-2 iter 1 — replacement B reuses A's sandbox",
            "tool_name":         DELEGATED_ROLE,
            "tool_args":         {
                "goal":               "child B: continue A's partial work",
                "reuse_sandbox_from": &child_a_run_id,
            },
            "confidence":        0.95,
            "requires_approval": false,
        }]);
    }

    // Phase 2: run one more iteration — iter 1 fires (spawn B
    // with reuse_sandbox_from=A). Parent is still non-terminal
    // because phase 1's max_iterations=1 cap prevented the loop
    // from emitting complete_run.
    let r = h
        .client()
        .post(format!(
            "{}/v1/runs/{}/orchestrate",
            h.base_url, parent_run_id
        ))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "goal": "#844 PR-2 happy-path parent goal",
            "max_iterations": 1,
        }))
        .send()
        .await
        .expect("orchestrate phase 2 reaches server");
    let status = r.status().as_u16();
    assert!(matches!(status, 200 | 202), "phase 2 status={status}");

    // Observe child B.
    let r = h
        .client()
        .get(format!("{}/v1/runs/{}/children", h.base_url, parent_run_id))
        .bearer_auth(&h.admin_token)
        .send()
        .await
        .expect("children reaches server");
    assert_eq!(r.status().as_u16(), 200);
    let body: Value = r.json().await.unwrap();
    let items = body
        .get("items")
        .and_then(|v| v.as_array())
        .expect("children items");
    assert_eq!(
        items.len(),
        2,
        "phase 2: two children expected (A + B); got {items:?}"
    );
    let child_b_run_id = items
        .iter()
        .filter_map(|v| v.get("run_id").and_then(|id| id.as_str()))
        .find(|id| *id != child_a_run_id.as_str())
        .expect("child B distinct from A")
        .to_owned();

    // ── Core #844 PR-2 assertion ──────────────────────────────
    // Child B's `reuse_sandbox_from` default is set to child A's
    // run_id VERBATIM. This is the row the orchestrate handler
    // reads on child B's next iteration to redirect
    // `working_dir_for_run` at child A's sandbox (ephemeral path
    // keyed on child A's id) instead of child B's fresh one.
    let key = format!("run:{child_b_run_id}:reuse_sandbox_from");
    let r = h
        .client()
        .get(format!(
            "{}/v1/settings/defaults/project/{}/{}",
            h.base_url, project_id, key
        ))
        .bearer_auth(&h.admin_token)
        .send()
        .await
        .expect("defaults GET reaches server");
    assert_eq!(
        r.status().as_u16(),
        200,
        "#844 PR-2: child B's `reuse_sandbox_from` default MUST exist \
         after a same-root sibling spawn — the adapter validates and \
         persists verbatim. Body: {}",
        r.text().await.unwrap_or_default(),
    );
    let body: Value = r.json().await.expect("defaults body json");
    let value = body.get("value").and_then(|v| v.as_str()).unwrap_or("");
    assert_eq!(
        value, child_a_run_id,
        "#844 PR-2: the default's value must be child A's run_id verbatim"
    );
}

/// #844 PR-2 cross-root rejection: a spawn whose
/// `reuse_sandbox_from` targets a run under a DIFFERENT root must
/// be rejected by the adapter's same-root validator. The parent's
/// observable state: no child row is created (validation fires
/// before Phase-1 in `fabric_adapter::spawn_subagent`), and
/// step_history picks up the failure so the LLM can correct on
/// the next DECIDE.
///
/// Shape: two independent parents P1 and P2 (distinct runs under
/// the same session+project — same tenant/connection/credential
/// to avoid 409-on-duplicate during provisioning). P1 spawns
/// child A. P2 then emits a spawn targeting A as its
/// `reuse_sandbox_from`. A sits under root=P1; P2's spawns have
/// root=P2; the roots differ → reject.
#[tokio::test]
async fn spawn_subagent_rejects_cross_root_reuse_sandbox_from() {
    let h = LiveHarness::setup().await;

    // P1 plan: spawn child A, complete.
    // P2 uses the same mock (different mock URL is fine — we
    // rebuild the plan between orchestrate calls), but we MUST
    // reuse the same credential/connection/session/tenant to
    // avoid tripping 409-on-duplicate at provision time. The
    // root boundary is per-run (P1.run_id vs P2.run_id), so
    // sharing those upstream resources is orthogonal to the
    // same-root invariant this test pins.
    //
    // Path: one shared `provision_run` for P1 (which sets up
    // tenant + credential + connection + session + run). P2 is
    // an extra run under the SAME session (not a full
    // provision). We point the single mock at a plan vec that
    // serves all three iterations (P1 spawn A, P1 complete, P2
    // spawn-with-cross-root-reuse) — the orchestrator will
    // call the mock once per iteration so hits count matches.
    let (mock_url, _hits, plan_handle) = spawn_mock(vec![
        json!([{
            "action_type":       "spawn_subagent",
            "description":       "P1 spawns child A",
            "tool_name":         DELEGATED_ROLE,
            "tool_args":         { "goal": "A goal" },
            "confidence":        0.95,
            "requires_approval": false,
        }]),
        json!([{
            "action_type":       "complete_run",
            "description":       "P1 done",
            "confidence":        0.99,
            "requires_approval": false,
        }]),
        // iter 2 placeholder — filled with A's real id once observed.
        json!([{
            "action_type": "complete_run",
            "description": "placeholder",
            "confidence": 0.5,
        }]),
    ])
    .await;
    let (session_id, p1_run_id, _project) = provision_run(&h, &mock_url, "cross").await;

    let _ = h
        .client()
        .post(format!("{}/v1/runs/{}/orchestrate", h.base_url, p1_run_id))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "goal": "P1 parent goal",
            "max_iterations": 2,
        }))
        .send()
        .await
        .expect("P1 orchestrate");

    let r = h
        .client()
        .get(format!("{}/v1/runs/{}/children", h.base_url, p1_run_id))
        .bearer_auth(&h.admin_token)
        .send()
        .await
        .expect("P1 children");
    let body: Value = r.json().await.unwrap();
    let items = body.get("items").and_then(|v| v.as_array()).unwrap();
    let child_a_run_id = items[0]
        .get("run_id")
        .and_then(|v| v.as_str())
        .expect("child A run_id")
        .to_owned();

    // Swap iter-2 placeholder for the real cross-root spawn plan.
    {
        let mut plan = plan_handle.lock().unwrap();
        plan[2] = json!([{
            "action_type":       "spawn_subagent",
            "description":       "P2 tries to reuse A's sandbox — cross-root",
            "tool_name":         DELEGATED_ROLE,
            "tool_args":         {
                "goal":               "continue A's work",
                "reuse_sandbox_from": &child_a_run_id,
            },
            "confidence":        0.95,
            "requires_approval": false,
        }]);
    }

    // Create P2 as a second run under the same session. Own
    // root. Use a deterministic suffix to avoid collision with
    // P1's run_id.
    let p2_run_id = format!("{p1_run_id}_p2");
    let r = h
        .client()
        .post(format!("{}/v1/runs", h.base_url))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "tenant_id":    "default_tenant",
            "workspace_id": "default_workspace",
            "project_id":   "default_project",
            "session_id":   session_id,
            "run_id":       p2_run_id,
        }))
        .send()
        .await
        .expect("P2 run create");
    assert_eq!(r.status().as_u16(), 201);

    let r = h
        .client()
        .post(format!("{}/v1/runs/{}/orchestrate", h.base_url, p2_run_id))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "goal": "P2 parent goal",
            "max_iterations": 1,
        }))
        .send()
        .await
        .expect("P2 orchestrate");
    let status = r.status().as_u16();
    assert!(
        matches!(status, 200 | 202),
        "orchestrate should complete — the spawn fails but surfaces \
         into step_history and the loop continues; status={status}"
    );

    // Cross-root spawn rejected → no child row under P2.
    let r = h
        .client()
        .get(format!("{}/v1/runs/{}/children", h.base_url, p2_run_id))
        .bearer_auth(&h.admin_token)
        .send()
        .await
        .expect("P2 children");
    let body: Value = r.json().await.unwrap();
    let items = body.get("items").and_then(|v| v.as_array()).unwrap();
    assert!(
        items.is_empty(),
        "#844 PR-2 same-root invariant: P2's spawn with \
         reuse_sandbox_from = <child of P1> must be rejected at the \
         validation gate — no child row under P2. Got: {items:?}"
    );
}

/// #844 PR-2: spawn targeting a nonexistent run_id is rejected.
/// The validator looks the target up via `RunReadModel::get`; a
/// 404-from-projection returns a `Validation` error that lands in
/// step_history. No child row is created.
#[tokio::test]
async fn spawn_subagent_rejects_unknown_reuse_sandbox_from_id() {
    let h = LiveHarness::setup().await;
    let plan = vec![json!([{
        "action_type":       "spawn_subagent",
        "description":       "#844 PR-2: target a nonexistent prior sibling",
        "tool_name":         DELEGATED_ROLE,
        "tool_args":         {
            "goal":               "child goal",
            "reuse_sandbox_from": "run_subagent_child_task_844_does_not_exist",
        },
        "confidence":        0.95,
        "requires_approval": false,
    }])];
    let (mock_url, _hits, _plan) = spawn_mock(plan).await;
    let (_session_id, parent_run_id, _project_id) =
        provision_run(&h, &mock_url, "reject-unknown").await;

    let r = h
        .client()
        .post(format!(
            "{}/v1/runs/{}/orchestrate",
            h.base_url, parent_run_id
        ))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "goal": "#844 PR-2 unknown-target parent goal",
            "max_iterations": 1,
        }))
        .send()
        .await
        .expect("orchestrate reaches server");
    assert!(matches!(r.status().as_u16(), 200 | 202));

    let r = h
        .client()
        .get(format!("{}/v1/runs/{}/children", h.base_url, parent_run_id))
        .bearer_auth(&h.admin_token)
        .send()
        .await
        .expect("children endpoint reachable");
    let body: Value = r.json().await.unwrap();
    let items = body
        .get("items")
        .and_then(|v| v.as_array())
        .expect("children response items");
    assert!(
        items.is_empty(),
        "#844 PR-2: unknown-id reuse_sandbox_from must reject BEFORE \
         creating a child row. Got children: {items:?}"
    );
}

/// #844 PR-2 (Copilot review): when the LLM emits
/// `reuse_sandbox_from` as a non-string (number, object, etc.), the
/// execute layer MUST reject loudly via MALFORMED_SPAWN_PROPOSAL_PREFIX
/// — NOT silently fall through to a fresh sandbox. Silent fallback
/// would rob the retry loop of its schema-drift signal, and the LLM
/// would keep emitting the wrong shape without correction. Observable:
/// no child row is created, and step_history picks up the rejection.
#[tokio::test]
async fn spawn_subagent_rejects_non_string_reuse_sandbox_from() {
    let h = LiveHarness::setup().await;
    // Non-string: an integer. The tool_def declares
    // `reuse_sandbox_from: string`, so this is schema-drift.
    let plan = vec![json!([{
        "action_type":       "spawn_subagent",
        "description":       "#844: non-string reuse_sandbox_from",
        "tool_name":         DELEGATED_ROLE,
        "tool_args":         {
            "goal":               "child goal",
            "reuse_sandbox_from": 42,
        },
        "confidence":        0.95,
        "requires_approval": false,
    }])];
    let (mock_url, _hits, _plan) = spawn_mock(plan).await;
    let (_session_id, parent_run_id, _project_id) =
        provision_run(&h, &mock_url, "malformed-type").await;

    let r = h
        .client()
        .post(format!(
            "{}/v1/runs/{}/orchestrate",
            h.base_url, parent_run_id
        ))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "goal": "#844 PR-2 malformed-type parent goal",
            "max_iterations": 1,
        }))
        .send()
        .await
        .expect("orchestrate reaches server");
    assert!(matches!(r.status().as_u16(), 200 | 202));

    // No child row — the execute layer rejected the malformed
    // tool_args BEFORE calling TaskService::spawn_subagent.
    let r = h
        .client()
        .get(format!("{}/v1/runs/{}/children", h.base_url, parent_run_id))
        .bearer_auth(&h.admin_token)
        .send()
        .await
        .expect("children endpoint reachable");
    let body: Value = r.json().await.unwrap();
    let items = body
        .get("items")
        .and_then(|v| v.as_array())
        .expect("children response items");
    assert!(
        items.is_empty(),
        "#844 PR-2: non-string reuse_sandbox_from must be rejected at \
         the execute-layer type guard BEFORE creating a child row. Got \
         children: {items:?}"
    );
}

/// #844 PR-2: when the LLM omits `reuse_sandbox_from`, no default
/// row is written to the child. Parallel to
/// `spawn_subagent_omits_parent_context_default_when_unset` (#775).
/// Guards against a regression where the persistence path fires
/// unconditionally — an empty row would silently trigger the
/// orchestrate handler's reuse-branch on every child.
#[tokio::test]
async fn spawn_subagent_omits_reuse_sandbox_from_default_when_unset() {
    let h = LiveHarness::setup().await;
    let plan = vec![
        json!([{
            "action_type":       "spawn_subagent",
            "description":       "#844: spawn without reuse_sandbox_from",
            "tool_name":         DELEGATED_ROLE,
            "tool_args":         { "goal": "child goal" },
            "confidence":        0.95,
            "requires_approval": false,
        }]),
        json!([{
            "action_type":       "complete_run",
            "description":       "parent post-spawn complete",
            "confidence":        0.99,
            "requires_approval": false,
        }]),
    ];
    let (mock_url, _hits, _plan) = spawn_mock(plan).await;
    let (_session_id, parent_run_id, project_id) = provision_run(&h, &mock_url, "omit").await;

    let r = h
        .client()
        .post(format!(
            "{}/v1/runs/{}/orchestrate",
            h.base_url, parent_run_id
        ))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "goal": "#844 PR-2 omit-field parent goal",
            "max_iterations": 2,
        }))
        .send()
        .await
        .expect("orchestrate reaches server");
    assert!(matches!(r.status().as_u16(), 200 | 202));

    let r = h
        .client()
        .get(format!("{}/v1/runs/{}/children", h.base_url, parent_run_id))
        .bearer_auth(&h.admin_token)
        .send()
        .await
        .expect("children reaches server");
    let body: Value = r.json().await.unwrap();
    let items = body
        .get("items")
        .and_then(|v| v.as_array())
        .expect("children items");
    assert_eq!(items.len(), 1, "one child after single spawn");
    let child_run_id = items[0]
        .get("run_id")
        .and_then(|v| v.as_str())
        .expect("child run_id");

    // The `reuse_sandbox_from` default MUST NOT exist when the LLM
    // did not set it. 404 is the expected status.
    let key = format!("run:{child_run_id}:reuse_sandbox_from");
    let r = h
        .client()
        .get(format!(
            "{}/v1/settings/defaults/project/{}/{}",
            h.base_url, project_id, key
        ))
        .bearer_auth(&h.admin_token)
        .send()
        .await
        .expect("defaults GET reaches server");
    assert_eq!(
        r.status().as_u16(),
        404,
        "#844 PR-2: when reuse_sandbox_from is NOT set on the spawn, \
         the child's default row MUST NOT be written.",
    );
}
