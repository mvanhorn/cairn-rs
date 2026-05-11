//! #433 integration test — `Idempotency-Key` header on
//! `POST /v1/runs/:id/orchestrate` replays the first response on retry,
//! 409s on same-key/different-body, and remains per-tenant.
//!
//! The test avoids actually running the LLM loop — orchestrate against
//! a non-existent run_id returns 404 from inside the handler, AFTER
//! the idempotency layer claims the slot. That lets us assert replay
//! and body-reuse semantics without provisioning providers/tenants.

mod support;

use serde_json::{json, Value};
use support::live_fabric::LiveHarness;

const IDEMPOTENCY_HEADER: &str = "Idempotency-Key";

#[tokio::test]
async fn idempotency_key_replays_the_same_response() {
    let h = LiveHarness::setup().await;
    let base = &h.base_url;
    let run_id = "nonexistent-run-for-idempotency-test-001";
    let idem_key = "test-idem-key-replays-abc";
    let body = json!({
        "tenant_id":    h.tenant,
        "workspace_id": h.workspace,
        "project_id":   h.project,
    });

    // First POST — real 404 (run doesn't exist).
    let first = h
        .client()
        .post(format!("{base}/v1/runs/{run_id}/orchestrate"))
        .bearer_auth(&h.admin_token)
        .header(IDEMPOTENCY_HEADER, idem_key)
        .json(&body)
        .send()
        .await
        .expect("first orchestrate POST reaches server");
    let first_status = first.status().as_u16();
    let first_body = first.text().await.expect("first body");
    assert_eq!(
        first_status, 404,
        "expected 404 on nonexistent run, got {first_status}: {first_body}"
    );

    // Second POST with same key + same body — must replay the 404.
    let second = h
        .client()
        .post(format!("{base}/v1/runs/{run_id}/orchestrate"))
        .bearer_auth(&h.admin_token)
        .header(IDEMPOTENCY_HEADER, idem_key)
        .json(&body)
        .send()
        .await
        .expect("second orchestrate POST reaches server");
    let second_status = second.status().as_u16();
    let replayed_flag = second
        .headers()
        .get("idempotent-replayed")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_owned();
    let second_body = second.text().await.expect("second body");
    assert_eq!(
        second_status, 404,
        "replay must return the same status as the first response"
    );
    assert_eq!(
        second_body, first_body,
        "replay must return byte-identical body"
    );
    assert_eq!(
        replayed_flag, "true",
        "replay must be marked via idempotent-replayed: true header"
    );
}

#[tokio::test]
async fn idempotency_key_reuse_with_different_body_is_409() {
    let h = LiveHarness::setup().await;
    let base = &h.base_url;
    let run_id = "nonexistent-run-for-idempotency-test-002";
    let idem_key = "test-idem-key-conflict-xyz";

    let body1 = json!({
        "tenant_id":    h.tenant,
        "workspace_id": h.workspace,
        "project_id":   h.project,
    });
    let body2 = json!({
        "tenant_id":    h.tenant,
        "workspace_id": h.workspace,
        "project_id":   h.project,
        "max_iterations": 7, // different body
    });

    // First call caches the response.
    let first = h
        .client()
        .post(format!("{base}/v1/runs/{run_id}/orchestrate"))
        .bearer_auth(&h.admin_token)
        .header(IDEMPOTENCY_HEADER, idem_key)
        .json(&body1)
        .send()
        .await
        .expect("first orchestrate POST");
    assert_eq!(first.status().as_u16(), 404);

    // Second call with same key but different body — must 409.
    let conflict = h
        .client()
        .post(format!("{base}/v1/runs/{run_id}/orchestrate"))
        .bearer_auth(&h.admin_token)
        .header(IDEMPOTENCY_HEADER, idem_key)
        .json(&body2)
        .send()
        .await
        .expect("second orchestrate POST");
    assert_eq!(
        conflict.status().as_u16(),
        409,
        "Idempotency-Key reuse with a different body must 409"
    );
    let envelope: Value = conflict.json().await.expect("conflict JSON");
    assert_eq!(
        envelope.get("code").and_then(|v| v.as_str()),
        Some("idempotency_key_reuse"),
        "error code must identify the failure class: {envelope}"
    );
}

#[tokio::test]
async fn idempotency_key_omission_is_not_cached() {
    // Sanity check: the header is optional. A request without it must
    // behave identically to pre-#433 — no cache lookup, no cache
    // insert. We verify this by firing two identical requests without
    // the header and confirming neither carries the replay marker.
    let h = LiveHarness::setup().await;
    let base = &h.base_url;
    let run_id = "nonexistent-run-for-idempotency-test-003";
    let body = json!({
        "tenant_id":    h.tenant,
        "workspace_id": h.workspace,
        "project_id":   h.project,
    });

    for i in 0..2 {
        let r = h
            .client()
            .post(format!("{base}/v1/runs/{run_id}/orchestrate"))
            .bearer_auth(&h.admin_token)
            .json(&body)
            .send()
            .await
            .expect("orchestrate POST");
        assert_eq!(r.status().as_u16(), 404, "no-idempotency call #{i}");
        assert!(
            r.headers().get("idempotent-replayed").is_none(),
            "no replay marker without the header"
        );
    }
}

#[tokio::test]
async fn idempotency_key_malformed_is_400() {
    let h = LiveHarness::setup().await;
    let base = &h.base_url;
    let run_id = "nonexistent-run-for-idempotency-test-004";

    // Empty key.
    let r = h
        .client()
        .post(format!("{base}/v1/runs/{run_id}/orchestrate"))
        .bearer_auth(&h.admin_token)
        .header(IDEMPOTENCY_HEADER, "   ")
        .json(&json!({}))
        .send()
        .await
        .expect("empty key POST");
    assert_eq!(r.status().as_u16(), 400);
    let envelope: Value = r.json().await.expect("400 envelope");
    assert_eq!(
        envelope.get("code").and_then(|v| v.as_str()),
        Some("invalid_idempotency_key")
    );

    // Over-long key (>255 chars).
    let long = "x".repeat(300);
    let r = h
        .client()
        .post(format!("{base}/v1/runs/{run_id}/orchestrate"))
        .bearer_auth(&h.admin_token)
        .header(IDEMPOTENCY_HEADER, long)
        .json(&json!({}))
        .send()
        .await
        .expect("long key POST");
    assert_eq!(r.status().as_u16(), 400);
}
