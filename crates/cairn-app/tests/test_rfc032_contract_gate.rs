//! RFC 032 PR-5: end-to-end regression tests for the completion-contract
//! HTTP surface + persistence path.
//!
//! Each test drives the full cairn-app subprocess via `LiveHarness` —
//! that is the only way to exercise the HTTP body validation, the
//! `DefaultsService::set_struct` persistence, the re-read on the first
//! orchestrate boot, and the typed-defaults cap. Anything below this
//! line is a unit test on decide_impl (pinning the prompt render) or
//! cairn-domain (pinning inference + validate()).

use serde_json::{json, Value};

mod support;

use support::live_fabric::LiveHarness;

async fn provision_session(h: &LiveHarness, session_suffix: &str) -> String {
    let session_id = format!("sess_{session_suffix}");
    let r = h
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
        .expect("session reaches server");
    assert_eq!(r.status().as_u16(), 201, "session create must succeed");
    session_id
}

async fn post_run(
    h: &LiveHarness,
    session_id: &str,
    run_id: &str,
    extra: Value,
) -> reqwest::Response {
    let mut body = json!({
        "tenant_id":    h.tenant,
        "workspace_id": h.workspace,
        "project_id":   h.project,
        "session_id":   session_id,
        "run_id":       run_id,
    });
    if let Value::Object(ref mut m) = body {
        if let Value::Object(extra_m) = extra {
            for (k, v) in extra_m {
                m.insert(k, v);
            }
        }
    }
    h.client()
        .post(format!("{}/v1/runs", h.base_url))
        .bearer_auth(&h.admin_token)
        .json(&body)
        .send()
        .await
        .expect("runs.create reaches server")
}

/// Explicit `ProseNonEmpty` contract on run creation: the handler
/// validates the body, persists the contract under the per-run
/// defaults, and returns 201. Round-trips via `GET
/// /v1/settings/defaults/project/:proj/run:<id>:completion_contract`
/// to prove the key landed.
#[tokio::test]
async fn explicit_prose_non_empty_contract_persists() {
    let h = LiveHarness::setup().await;
    let sid = provision_session(&h, "prose").await;

    let r = post_run(
        &h,
        &sid,
        "run_prose_explicit",
        json!({
            "prompt": "Summarise the architecture.",
            "completion_contract": { "kind": "prose_non_empty" }
        }),
    )
    .await;
    assert_eq!(
        r.status().as_u16(),
        201,
        "explicit prose_non_empty contract must persist; body={:?}",
        r.text().await.unwrap_or_default()
    );

    // Re-read via the defaults endpoint to prove persistence.
    let key = "run:run_prose_explicit:completion_contract";
    let got = h
        .client()
        .get(format!(
            "{}/v1/settings/defaults/project/{}/{}",
            h.base_url, h.project, key
        ))
        .bearer_auth(&h.admin_token)
        .send()
        .await
        .expect("defaults GET reaches server");
    assert!(
        got.status().is_success(),
        "persisted key must read back: got {}",
        got.status()
    );
    let body: Value = got.json().await.expect("defaults response is JSON");
    let value = body
        .get("value")
        .expect("defaults response must carry `value` field");
    assert_eq!(
        value.get("kind").and_then(Value::as_str),
        Some("prose_non_empty"),
        "stored contract kind round-trips; body={body}"
    );
}

/// Explicit `File` contract on a project with neither an allowlisted
/// repo nor a registered local_fs path must reject with `400
/// contract_invalid: file_contract_requires_persistent_workspace`
/// (RFC 032 §2.4).
#[tokio::test]
async fn file_contract_on_ephemeral_project_rejects_4xx() {
    let h = LiveHarness::setup().await;
    let sid = provision_session(&h, "file_ephemeral").await;

    let r = post_run(
        &h,
        &sid,
        "run_file_ephemeral",
        json!({
            "prompt": "Write the report.",
            "completion_contract": {
                "kind": "file",
                "paths": [ { "path": "docs/report.md" } ]
            }
        }),
    )
    .await;
    let status = r.status();
    let body = r.text().await.unwrap_or_default();
    // The handler routes this through `validation_error_response`
    // which returns 422 (workspace-wide validation-error convention).
    // RFC 032 §2.2 names the HTTP family as "4xx contract_invalid";
    // the wire contract here is the `contract_invalid:` marker + the
    // specific file_contract precondition string, not the exact code.
    assert!(
        status.as_u16() == 400 || status.as_u16() == 422,
        "File contract on ephemeral project must reject with 4xx; got {status}, body={body}"
    );
    assert!(
        body.contains("file_contract_requires_persistent_workspace"),
        "rejection message must name the precondition; body={body}"
    );
}

/// Malformed contract (serialized OK but `.validate()` fails) must
/// reject with 400 contract_invalid. Example: File with an empty paths
/// vec.
#[tokio::test]
async fn malformed_contract_rejects_4xx() {
    let h = LiveHarness::setup().await;
    let sid = provision_session(&h, "malformed").await;

    let r = post_run(
        &h,
        &sid,
        "run_malformed",
        json!({
            "prompt": "Noop.",
            "completion_contract": {
                "kind": "file",
                "paths": []
            }
        }),
    )
    .await;
    let status = r.status();
    let body = r.text().await.unwrap_or_default();
    assert!(
        status.as_u16() == 400 || status.as_u16() == 422,
        "structurally-invalid contract must reject with 4xx; got {status}, body={body}"
    );
    assert!(
        body.contains("contract_invalid"),
        "rejection must carry contract_invalid marker; body={body}"
    );
}

/// Path traversal in a File contract's `path` field rejects at
/// deserialization time (RelPath's `try_from`). Validates that the
/// typed newtype wiring actually fires on the wire.
#[tokio::test]
async fn contract_path_traversal_rejects_4xx() {
    let h = LiveHarness::setup().await;
    let sid = provision_session(&h, "traversal").await;

    let r = post_run(
        &h,
        &sid,
        "run_traversal",
        json!({
            "prompt": "Noop.",
            "completion_contract": {
                "kind": "file",
                "paths": [ { "path": "../etc/passwd" } ]
            }
        }),
    )
    .await;
    let status = r.status();
    assert!(
        status.as_u16() == 400 || status.as_u16() == 422,
        "`..` path must reject at body parse; got {status}"
    );
}

/// No `completion_contract` on POST /v1/runs → the handler persists
/// nothing for the contract slots (inference runs at the first
/// orchestrate boot — a later integration test exercises that path).
/// This is the backward-compat pin: a pre-RFC-032 caller sees no
/// change to the run-create response shape.
#[tokio::test]
async fn absent_contract_preserves_backward_compat() {
    let h = LiveHarness::setup().await;
    let sid = provision_session(&h, "noop").await;

    let r = post_run(&h, &sid, "run_noop", json!({ "prompt": "Summarise." })).await;
    assert_eq!(r.status().as_u16(), 201, "run without contract still 201");

    // The completion_contract default MUST be absent at this point.
    // An explicit-contract run would have a row here; an inference-
    // pending run has not yet hit orchestrate, so the row is None.
    let key = "run:run_noop:completion_contract";
    let got = h
        .client()
        .get(format!(
            "{}/v1/settings/defaults/project/{}/{}",
            h.base_url, h.project, key
        ))
        .bearer_auth(&h.admin_token)
        .send()
        .await
        .expect("defaults GET reaches server");
    // Handler returns 404 when the key is absent.
    assert_eq!(
        got.status().as_u16(),
        404,
        "no contract declared → no row persisted yet (inference fires at orchestrate boot)"
    );
}

/// Valid `PullRequest` contract persists and round-trips through the
/// defaults store. Does not drive orchestrate — that path exercises
/// the verifier which hits live GitHub in the full deploy; here we
/// pin the body-acceptance + persistence surface only.
#[tokio::test]
async fn pull_request_contract_persists() {
    let h = LiveHarness::setup().await;
    let sid = provision_session(&h, "pr").await;

    let r = post_run(
        &h,
        &sid,
        "run_pr",
        json!({
            "prompt": "Open a PR titled 'cargo init'.",
            "completion_contract": {
                "kind": "pull_request",
                "expected_repo": "avifenesh/cairn-dogfood-roguelike",
                "must_be_open": true
            }
        }),
    )
    .await;
    assert_eq!(
        r.status().as_u16(),
        201,
        "pull_request contract persists; body={:?}",
        r.text().await.unwrap_or_default()
    );

    let key = "run:run_pr:completion_contract";
    let got = h
        .client()
        .get(format!(
            "{}/v1/settings/defaults/project/{}/{}",
            h.base_url, h.project, key
        ))
        .bearer_auth(&h.admin_token)
        .send()
        .await
        .expect("defaults GET reaches server");
    let body: Value = got.json().await.expect("defaults response is JSON");
    assert_eq!(
        body.get("value")
            .and_then(|v| v.get("kind"))
            .and_then(Value::as_str),
        Some("pull_request")
    );
    assert_eq!(
        body.get("value")
            .and_then(|v| v.get("expected_repo"))
            .and_then(Value::as_str),
        Some("avifenesh/cairn-dogfood-roguelike")
    );

    // ExplicitCreate source is persisted — operators can filter
    // inferred vs explicit via the source key.
    let source_key = "run:run_pr:contract_source";
    let src = h
        .client()
        .get(format!(
            "{}/v1/settings/defaults/project/{}/{}",
            h.base_url, h.project, source_key
        ))
        .bearer_auth(&h.admin_token)
        .send()
        .await
        .expect("source GET reaches server");
    let src_body: Value = src.json().await.expect("source response is JSON");
    assert_eq!(
        src_body.get("value").and_then(Value::as_str),
        Some("explicit_create"),
        "source key must record ExplicitCreate; body={src_body}"
    );
}
