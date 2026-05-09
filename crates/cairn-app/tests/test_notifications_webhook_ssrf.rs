//! Regression tests for #451 / #452 — webhook SSRF.
//!
//! Pre-fix, `POST /v1/admin/operators/:id/notifications` accepted any
//! `https://…` URL without checking the resolved IP. An operator with
//! the `notifications` feature could register a webhook pointing at
//! `http://169.254.169.254/latest/meta-data/iam/security-credentials/…`
//! and, the moment the webhook dispatcher fired, exfiltrate the AWS
//! IAM role credentials attached to the cairn-app instance.
//!
//! Post-fix (see `crates/cairn-app/src/webhook_validation.rs`):
//!
//!   * Loopback (127/8, ::1) — blocked on both http and https (#452)
//!   * Link-local incl. IMDS (169.254/16, fe80::/10) — blocked
//!   * RFC 1918 (10/8, 172.16/12, 192.168/16) — blocked
//!   * ULA (fc00::/7) — blocked
//!   * CGNAT (100.64/10), unspecified (0/8), multicast (224/4, ff00::/8) — blocked
//!   * `CAIRN_ALLOW_INTERNAL_WEBHOOKS=1` opts in explicitly for
//!     self-hosted dev stacks with a local sink.
//!
//! These tests drive the real cairn-app binary via `LiveHarness` —
//! the validation path has to be reachable through the HTTP boundary,
//! not just through the unit-test module, so a future refactor can't
//! drop the wiring in the handler.

mod support;

use serde_json::{json, Value};
use support::live_fabric::LiveHarness;

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

// ── #451: IMDS and internal IP SSRF block list ──────────────────────────────

/// The canonical AWS IMDS v1 credential-theft URL with the http
/// scheme. The scheme gate rejects http:// outright in the default
/// team-mode policy ("must use https"), which is still a 422 — the
/// IMDS host never sees a request. We assert on that message.
///
/// The https variant (where the scheme gate doesn't fire) is the
/// sharper regression test for #451 — see `webhook_rejects_https_imds_literal`.
#[tokio::test]
async fn webhook_rejects_http_imds_literal() {
    let h = LiveHarness::setup().await;
    let (status, body) = post_prefs(
        &h,
        "op-ssrf-imds-http",
        prefs_body(
            &h,
            vec![channel(
                "webhook",
                "http://169.254.169.254/latest/meta-data/iam/security-credentials/",
            )],
        ),
    )
    .await;
    assert_422(status, &body, "https");
}

/// Same IMDS v4 literal, but with the insecure-scheme opt-in set.
/// The scheme opt-in allows http but REQUIRES the host to be
/// loopback — 169.254.169.254 is not loopback, so the "only
/// permitted for loopback" guard fires. Regression teeth for #451's
/// exact described attack surface under the dev-flag rollout.
#[tokio::test]
async fn webhook_rejects_http_imds_literal_with_insecure_flag() {
    let h = LiveHarness::setup_with_env(&[("CAIRN_ALLOW_INSECURE_WEBHOOKS", "1")]).await;
    let (status, body) = post_prefs(
        &h,
        "op-ssrf-imds-http-insec",
        prefs_body(
            &h,
            vec![channel(
                "webhook",
                "http://169.254.169.254/latest/meta-data/iam/security-credentials/",
            )],
        ),
    )
    .await;
    assert_422(status, &body, "loopback");
}

/// #452 companion: https variant. Old code only rejected http to
/// non-loopback; https to 169.254.x.x passed through.
#[tokio::test]
async fn webhook_rejects_https_imds_literal() {
    let h = LiveHarness::setup().await;
    let (status, body) = post_prefs(
        &h,
        "op-ssrf-imds-https",
        prefs_body(
            &h,
            vec![channel(
                "webhook",
                "https://169.254.169.254/latest/meta-data/",
            )],
        ),
    )
    .await;
    assert_422(status, &body, "169.254");
}

#[tokio::test]
async fn webhook_rejects_https_loopback_ipv4() {
    // #452: https://127.0.0.1/… was accepted before the fix; the
    // scheme branch skipped the loopback check.
    let h = LiveHarness::setup().await;
    let (status, body) = post_prefs(
        &h,
        "op-ssrf-loop4",
        prefs_body(&h, vec![channel("webhook", "https://127.0.0.1:8080/hook")]),
    )
    .await;
    assert_422(status, &body, "127.0.0.0/8");
}

#[tokio::test]
async fn webhook_rejects_https_loopback_ipv6() {
    let h = LiveHarness::setup().await;
    let (status, body) = post_prefs(
        &h,
        "op-ssrf-loop6",
        prefs_body(&h, vec![channel("webhook", "https://[::1]/hook")]),
    )
    .await;
    assert_422(status, &body, "loopback");
}

#[tokio::test]
async fn webhook_rejects_rfc1918_10_block() {
    let h = LiveHarness::setup().await;
    let (status, body) = post_prefs(
        &h,
        "op-ssrf-rfc1918",
        prefs_body(&h, vec![channel("webhook", "https://10.1.1.1/hook")]),
    )
    .await;
    assert_422(status, &body, "10.0.0.0/8");
}

#[tokio::test]
async fn webhook_rejects_rfc1918_192_168_block() {
    let h = LiveHarness::setup().await;
    let (status, body) = post_prefs(
        &h,
        "op-ssrf-192",
        prefs_body(&h, vec![channel("webhook", "https://192.168.1.254/hook")]),
    )
    .await;
    assert_422(status, &body, "192.168.0.0/16");
}

#[tokio::test]
async fn webhook_rejects_ipv6_ula() {
    let h = LiveHarness::setup().await;
    let (status, body) = post_prefs(
        &h,
        "op-ssrf-ula",
        prefs_body(&h, vec![channel("webhook", "https://[fc00::1]/hook")]),
    )
    .await;
    assert_422(status, &body, "fc00::/7");
}

// ── opt-in escape hatch ──────────────────────────────────────────────────────

/// Self-hosted operators with a local webhook sink (Alertmanager bound
/// to 127.0.0.1, etc.) can opt in via `CAIRN_ALLOW_INTERNAL_WEBHOOKS=1`
/// plus `CAIRN_ALLOW_INSECURE_WEBHOOKS=1` when the sink is http (no
/// TLS). The scheme flag alone (pre-existing from #235) is enough to
/// allow `http://localhost` — the `http` branch always requires
/// loopback — but a 127.0.0.1 IP literal specifically exercises the
/// fast-path v4 block list so we set both flags to keep the test
/// failure mode readable if one side regresses.
#[tokio::test]
async fn webhook_allowed_with_insecure_flag_for_loopback_http() {
    let h = LiveHarness::setup_with_env(&[("CAIRN_ALLOW_INSECURE_WEBHOOKS", "1")]).await;
    let (status, body) = post_prefs(
        &h,
        "op-insecure-loop",
        prefs_body(&h, vec![channel("webhook", "http://127.0.0.1:9999/hook")]),
    )
    .await;
    assert_eq!(
        status, 201,
        "CAIRN_ALLOW_INSECURE_WEBHOOKS=1 must permit http://127.0.0.1 sinks; \
         got {status} body={body}"
    );
}

/// Review regression (PR #533): `CAIRN_ALLOW_INSECURE_WEBHOOKS=1`
/// alone must NOT unlock the https SSRF block list. The first round
/// of this PR ORed the two flags at policy construction, which
/// re-opened #451 (an operator with only the dev-scheme flag could
/// post `https://169.254.169.254/…` and 201 it). This test locks the
/// invariant in at the HTTP boundary.
#[tokio::test]
async fn webhook_insecure_flag_alone_still_blocks_https_imds() {
    let h = LiveHarness::setup_with_env(&[("CAIRN_ALLOW_INSECURE_WEBHOOKS", "1")]).await;
    let (status, body) = post_prefs(
        &h,
        "op-insecure-imds",
        prefs_body(
            &h,
            vec![channel(
                "webhook",
                "https://169.254.169.254/latest/meta-data/",
            )],
        ),
    )
    .await;
    assert_422(status, &body, "169.254");
}

/// The internal-flag opt-in ALSO applies to https — self-hosted stacks
/// that terminate TLS on the internal host must still reach their
/// non-metadata link-local sink. Pre-#803 this test used
/// `169.254.169.254` (the IMDS IP); post-#803 the IMDS IP is always
/// blocked, so this test exercises a non-metadata link-local IP
/// (169.254.169.253) that the flag still unlocks.
#[tokio::test]
async fn webhook_allowed_with_internal_flag_for_link_local_https() {
    let h = LiveHarness::setup_with_env(&[("CAIRN_ALLOW_INTERNAL_WEBHOOKS", "1")]).await;
    let (status, body) = post_prefs(
        &h,
        "op-internal-ll",
        prefs_body(&h, vec![channel("webhook", "https://169.254.169.253/hook")]),
    )
    .await;
    assert_eq!(
        status, 201,
        "CAIRN_ALLOW_INTERNAL_WEBHOOKS=1 must permit https://169.254.169.253 \
         (non-metadata link-local) sinks; got {status} body={body}"
    );
}

/// #803: the internal-flag opt-in MUST NOT unlock cloud metadata IPs.
/// 169.254.169.254 is the canonical IMDS endpoint across AWS / GCP /
/// Azure; webhooks to it are unconditionally refused regardless of the
/// flag. Pre-#803 this scenario returned 201 in Local mode and with
/// the flag set in team mode — that's the regression the fix closes.
#[tokio::test]
async fn webhook_imds_still_blocked_even_with_internal_flag() {
    let h = LiveHarness::setup_with_env(&[("CAIRN_ALLOW_INTERNAL_WEBHOOKS", "1")]).await;
    let (status, body) = post_prefs(
        &h,
        "op-imds-still-blocked",
        prefs_body(&h, vec![channel("webhook", "https://169.254.169.254/hook")]),
    )
    .await;
    assert_eq!(
        status, 422,
        "#803: IMDS https target must be 422 even with \
         CAIRN_ALLOW_INTERNAL_WEBHOOKS=1; got {status} body={body}"
    );
    let msg = body["message"].as_str().unwrap_or_default();
    assert!(
        msg.contains("metadata") || msg.contains("IMDS"),
        "#803: error body.message must explain the metadata block; got: {msg}"
    );
}

// ── legitimate destination still works ──────────────────────────────────────

/// A public https URL must still round-trip as 201. This is the
/// baseline the fix must not regress.
#[tokio::test]
async fn webhook_accepts_public_https() {
    let h = LiveHarness::setup().await;
    let (status, body) = post_prefs(
        &h,
        "op-ssrf-public-ok",
        prefs_body(
            &h,
            vec![channel("webhook", "https://hooks.example.com/legit")],
        ),
    )
    .await;
    assert_eq!(
        status, 201,
        "legitimate public https webhook must succeed; got {status} body={body}"
    );
}
