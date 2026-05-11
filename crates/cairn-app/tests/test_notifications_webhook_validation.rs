//! Issue #235: `POST /v1/admin/operators/:id/notifications` must reject
//! malformed / disallowed webhook and slack channel targets with a 422 +
//! structured error body. Previously accepted anything as a string and
//! returned 201, leaving the operator to discover days later that their
//! typo'd URL had silently dropped every delivery.
//!
//! These tests drive the real cairn-app binary via `LiveHarness` and assert
//! the full request/response round-trip against the acceptance criteria:
//!
//!   - `target=not-a-url` + `kind=webhook` → 422
//!   - `target=ftp://example.com` + `kind=webhook` → 422
//!   - `target=http://evil.com` + `kind=webhook` → 422 (non-loopback)
//!   - `target=https://example.com/hook` + `kind=webhook` → 201
//!   - `target=http://localhost:3000/hook` with CAIRN_ALLOW_INSECURE_WEBHOOKS=1
//!     → 201; without the flag → 422
//!   - `kind=email` with malformed address → 422
//!   - `kind=slack` with garbage → 422
//!   - Unknown kinds (`pagerduty`, `telegram`) still accepted
//!
//! The 422 body is `{ status_code: 422, code: "validation_error",
//! message: "…" }` — matches the shared error envelope in
//! `crates/cairn-app/src/errors.rs`.

mod support;

use serde_json::{json, Value};
use support::live_fabric::LiveHarness;

/// Helper to POST notification preferences and return (status, body-json).
async fn post_prefs(h: &LiveHarness, operator_id: &str, body: Value) -> (u16, Value) {
    let r = h
        .client()
        .post(format!(
            "{}/v1/admin/operators/{}/notifications",
            h.base_url, operator_id
        ))
        .bearer_auth(&h.admin_token)
        .json(&body)
        .send()
        .await
        .expect("notifications POST must reach server");
    let status = r.status().as_u16();
    // Some error responses may not be valid JSON (fallback); tolerate that
    // so we can still assert on status without blowing up in the test body.
    let body: Value = r.json().await.unwrap_or(Value::Null);
    (status, body)
}

fn channel(kind: &str, target: &str) -> Value {
    json!({ "kind": kind, "target": target })
}

fn prefs_body(h: &LiveHarness, channels: Vec<Value>) -> Value {
    json!({
        "tenant_id": h.tenant,
        "event_types": ["run.completed"],
        "channels": channels,
    })
}

// Assert the response is a 422 with our structured error envelope.
fn assert_422(status: u16, body: &Value, expected_fragment: &str) {
    assert_eq!(status, 422, "expected 422, got {status} body={body}");
    assert_eq!(
        body["status_code"].as_u64(),
        Some(422),
        "status_code field mismatch: {body}",
    );
    assert_eq!(
        body["code"].as_str(),
        Some("validation_error"),
        "code field mismatch: {body}",
    );
    let msg = body["message"].as_str().unwrap_or_default();
    assert!(
        msg.contains(expected_fragment),
        "422 message should mention '{expected_fragment}', got: {msg}",
    );
}

// ── webhook ──────────────────────────────────────────────────────────────────

#[tokio::test]
async fn webhook_rejects_garbage_target() {
    let h = LiveHarness::setup().await;
    let (status, body) = post_prefs(
        &h,
        "op-garbage",
        prefs_body(&h, vec![channel("webhook", "not-a-url")]),
    )
    .await;
    assert_422(status, &body, "not a valid URL");
}

#[tokio::test]
async fn webhook_rejects_ftp_scheme() {
    let h = LiveHarness::setup().await;
    let (status, body) = post_prefs(
        &h,
        "op-ftp",
        prefs_body(&h, vec![channel("webhook", "ftp://example.com/hook")]),
    )
    .await;
    assert_422(status, &body, "unsupported scheme");
}

#[tokio::test]
async fn webhook_rejects_http_non_localhost() {
    let h = LiveHarness::setup().await;
    let (status, body) = post_prefs(
        &h,
        "op-http-external",
        prefs_body(&h, vec![channel("webhook", "http://evil.example.com/hook")]),
    )
    .await;
    // Without the dev flag, http is forbidden outright. "must use https"
    // message is what the user sees.
    assert_422(status, &body, "https");
}

#[tokio::test]
async fn webhook_accepts_https_target() {
    let h = LiveHarness::setup().await;
    let (status, body) = post_prefs(
        &h,
        "op-https-ok",
        prefs_body(
            &h,
            vec![channel("webhook", "https://hooks.example.com/hook")],
        ),
    )
    .await;
    assert_eq!(
        status, 201,
        "https webhook must succeed, got {status} body={body}"
    );
    assert_eq!(body["ok"].as_bool(), Some(true), "unexpected body: {body}");
}

#[tokio::test]
async fn webhook_http_localhost_needs_dev_flag() {
    // Default team-mode harness does NOT set the insecure flag. http://localhost
    // must be rejected.
    let h = LiveHarness::setup().await;
    let (status, body) = post_prefs(
        &h,
        "op-http-local-nodev",
        prefs_body(&h, vec![channel("webhook", "http://localhost:3000/hook")]),
    )
    .await;
    assert_422(status, &body, "https");
}

#[tokio::test]
async fn webhook_http_localhost_accepted_with_dev_flag() {
    // With CAIRN_ALLOW_INSECURE_WEBHOOKS=1, http://localhost is fine.
    let h = LiveHarness::setup_with_env(&[("CAIRN_ALLOW_INSECURE_WEBHOOKS", "1")]).await;
    let (status, body) = post_prefs(
        &h,
        "op-http-local-dev",
        prefs_body(&h, vec![channel("webhook", "http://localhost:3000/hook")]),
    )
    .await;
    assert_eq!(
        status, 201,
        "http://localhost with dev flag must succeed, got {status} body={body}"
    );
}

#[tokio::test]
async fn webhook_http_loopback_ipv4_accepted_with_dev_flag() {
    let h = LiveHarness::setup_with_env(&[("CAIRN_ALLOW_INSECURE_WEBHOOKS", "1")]).await;
    let (status, body) = post_prefs(
        &h,
        "op-http-127",
        prefs_body(&h, vec![channel("webhook", "http://127.0.0.1:4000/hook")]),
    )
    .await;
    assert_eq!(
        status, 201,
        "http://127.0.0.1 with dev flag must succeed, got {status} body={body}"
    );
}

#[tokio::test]
async fn webhook_http_non_loopback_rejected_even_with_dev_flag() {
    // Dev flag relaxes scheme to http but the host must still be loopback.
    // This is the defense-in-depth check against "oops I set the flag in
    // production so now http://evil.com is fine" — it isn't.
    let h = LiveHarness::setup_with_env(&[("CAIRN_ALLOW_INSECURE_WEBHOOKS", "1")]).await;
    let (status, body) = post_prefs(
        &h,
        "op-http-evil",
        prefs_body(&h, vec![channel("webhook", "http://evil.example.com/hook")]),
    )
    .await;
    assert_422(status, &body, "loopback");
}

// ── slack ────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn slack_rejects_garbage_target() {
    let h = LiveHarness::setup().await;
    let (status, body) = post_prefs(
        &h,
        "op-slack-garbage",
        prefs_body(&h, vec![channel("slack", "#general")]),
    )
    .await;
    // `#general` is not a URL — slack channel targets in this API are
    // incoming-webhook URLs (per the UI placeholder). Reject.
    assert_422(status, &body, "not a valid URL");
}

#[tokio::test]
async fn slack_accepts_https_hook() {
    let h = LiveHarness::setup().await;
    let (status, body) = post_prefs(
        &h,
        "op-slack-ok",
        prefs_body(
            &h,
            vec![channel(
                "slack",
                "https://hooks.slack.com/services/T000/B000/XYZ",
            )],
        ),
    )
    .await;
    assert_eq!(
        status, 201,
        "slack https webhook must succeed, got {status} body={body}"
    );
}

// ── email ────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn email_rejects_malformed() {
    let h = LiveHarness::setup().await;
    let (status, body) = post_prefs(
        &h,
        "op-email-bad",
        prefs_body(&h, vec![channel("email", "not-an-email")]),
    )
    .await;
    assert_422(status, &body, "'@'");
}

#[tokio::test]
async fn email_accepts_valid() {
    let h = LiveHarness::setup().await;
    let (status, body) = post_prefs(
        &h,
        "op-email-ok",
        prefs_body(&h, vec![channel("email", "alice@example.com")]),
    )
    .await;
    assert_eq!(
        status, 201,
        "valid email address must succeed, got {status} body={body}"
    );
}

// ── unknown kinds pass through (opt-in by the UI for future adapters) ────────

#[tokio::test]
async fn unknown_kind_accepts_arbitrary_target() {
    // `pagerduty` is in the UI's CHANNEL_TYPES list but has no URL shape
    // yet; passing its service-key string must not be rejected here.
    let h = LiveHarness::setup().await;
    let (status, body) = post_prefs(
        &h,
        "op-pd",
        prefs_body(&h, vec![channel("pagerduty", "SERVICE-KEY-123")]),
    )
    .await;
    assert_eq!(
        status, 201,
        "unknown channel kind must not be rejected by URL validation, got {status} body={body}"
    );
}

// ── multi-channel request — first offender reported with index ───────────────

#[tokio::test]
async fn multi_channel_reports_first_bad_with_index() {
    let h = LiveHarness::setup().await;
    let (status, body) = post_prefs(
        &h,
        "op-multi",
        prefs_body(
            &h,
            vec![
                channel("webhook", "https://ok.example.com/hook"),
                channel("webhook", "also-not-a-url"),
            ],
        ),
    )
    .await;
    assert_422(status, &body, "channels[1]");
}
