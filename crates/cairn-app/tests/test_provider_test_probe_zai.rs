//! Integration test for the `GET /v1/providers/connections/:id/test` probe.
//!
//! Covers dogfood issue #632: the probe fell through to the OpenAI-compat
//! default base URL for every adapter except ollama/bedrock, so a zai /
//! zai-coding / anthropic / deepseek / etc. connection always returned 401
//! against an unrelated endpoint even with a valid credential.
//!
//! Two scenarios exercised end-to-end against a real cairn-app subprocess:
//!
//! 1. **Happy path (200)** — probe hits the fake provider's `/models` with
//!    the resolved `Authorization: Bearer <key>` header, `ok=true`,
//!    `detail="reachable"`.
//! 2. **Unauthorized (401)** — same wiring but the fake returns 401;
//!    `ok=false`, `status=401`, `detail` includes human-readable
//!    "401 Unauthorized" rather than the old `"returned non-2xx"` sentinel.
//!
//! The test also confirms the probe path is exactly `/models` (not
//! `/chat/completions`, not the OpenAI-compat default that would hit the
//! wrong host entirely).

mod support;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use axum::http::StatusCode as AxumStatus;
use axum::{extract::State as AxumState, http::HeaderMap, routing::get, Json, Router};
use serde_json::{json, Value};
use support::live_fabric::LiveHarness;
use tokio::sync::Mutex;

/// Records what the probe saw: whether `/models` was hit, and the
/// `Authorization` header value (if any) the server received.
#[derive(Default, Clone)]
struct ProbeRecord {
    models_hits: Arc<AtomicUsize>,
    last_auth: Arc<Mutex<Option<String>>>,
}

#[derive(Clone)]
struct FakeZaiState {
    record: ProbeRecord,
    /// HTTP status code to return from `/models` — 200 (success) or 401
    /// (unauthenticated) drive the two flavours of this test.
    models_status: u16,
}

async fn models_handler(
    AxumState(state): AxumState<FakeZaiState>,
    headers: HeaderMap,
) -> (AxumStatus, Json<Value>) {
    state.record.models_hits.fetch_add(1, Ordering::SeqCst);
    let auth_value = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);
    *state.record.last_auth.lock().await = auth_value;
    let code = AxumStatus::from_u16(state.models_status).unwrap_or(AxumStatus::OK);
    if code.is_success() {
        (code, Json(json!({"data": [{"id": "glm-4.7"}]})))
    } else {
        (code, Json(json!({"error": "invalid_token"})))
    }
}

/// Bind a minimal Axum server that answers `GET /models` with `models_status`
/// and no other route — so a probe that mistakenly hits `/chat/completions`
/// (or anything else) gets the default 404 and is caught by the assertions.
async fn spawn_fake_zai(models_status: u16) -> (String, ProbeRecord) {
    let record = ProbeRecord::default();
    let state = FakeZaiState {
        record: record.clone(),
        models_status,
    };
    let app = Router::new()
        .route("/models", get(models_handler))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    // Give the server a tick to start accepting connections.
    tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    (format!("http://{addr}"), record)
}

/// Shared setup: spin up the fake, register a `zai-coding` credential +
/// connection wired to the fake's base URL, and return the harness +
/// connection id.
async fn register_zai_coding_connection(h: &LiveHarness, fake_url: &str) -> (String, String) {
    let tenant = "default_tenant".to_owned();
    let suffix = h.project.clone();
    let connection_id = format!("conn_zai_{suffix}");

    // Credential: stored under "zai-coding" provider id so the connection
    // can bind it. The plaintext is opaque from the probe's POV; all we
    // need is that the decrypted value travels through the Authorization
    // header the fake captures.
    let r = h
        .client()
        .post(format!(
            "{}/v1/admin/tenants/{}/credentials",
            h.base_url, tenant,
        ))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "provider_id": "zai-coding",
            "plaintext_value": format!("sk-zai-live-{suffix}"),
        }))
        .send()
        .await
        .expect("credential create reaches server");
    assert_eq!(
        r.status().as_u16(),
        201,
        "credential create failed: {}",
        r.text().await.unwrap_or_default(),
    );
    let credential_id = r
        .json::<Value>()
        .await
        .expect("credential json")
        .get("id")
        .and_then(|v| v.as_str())
        .expect("credential id present")
        .to_owned();

    // Register the connection against the fake. `endpoint_url` points at
    // the fake's root (no trailing `/models` — the probe appends it). If
    // the probe ignored `endpoint_url` and fell back to the adapter's
    // canonical base (`https://api.z.ai/api/coding/paas/v4/`) the fake
    // would never see the hit, which the assertions catch.
    let r = h
        .client()
        .post(format!("{}/v1/providers/connections", h.base_url))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "tenant_id": tenant,
            "provider_connection_id": connection_id,
            "provider_family": "zai-coding",
            "adapter_type": "zai-coding",
            "supported_models": ["glm-4.7"],
            "credential_id": credential_id,
            "endpoint_url": fake_url,
        }))
        .send()
        .await
        .expect("connection create reaches server");
    assert_eq!(
        r.status().as_u16(),
        201,
        "connection create failed: {}",
        r.text().await.unwrap_or_default(),
    );

    (connection_id, credential_id)
}

#[tokio::test]
async fn zai_coding_probe_hits_models_with_bearer_on_200() {
    let h = LiveHarness::setup().await;
    let (fake_url, record) = spawn_fake_zai(200).await;
    let (connection_id, _cred_id) = register_zai_coding_connection(&h, &fake_url).await;

    let r = h
        .client()
        .get(format!(
            "{}/v1/providers/connections/{}/test",
            h.base_url, connection_id,
        ))
        .bearer_auth(&h.admin_token)
        .send()
        .await
        .expect("probe reaches server");
    assert_eq!(r.status().as_u16(), 200, "probe HTTP status");
    let body: Value = r.json().await.expect("probe body json");

    // The handler always wraps the probe in a 200 with `ok` signaling
    // reachability. 200-from-fake must surface as ok=true.
    assert_eq!(
        body.get("ok").and_then(Value::as_bool),
        Some(true),
        "ok field missing or false: {body}",
    );
    assert_eq!(
        body.get("status").and_then(Value::as_u64),
        Some(200),
        "status field missing or wrong: {body}",
    );
    assert_eq!(
        body.get("provider").and_then(Value::as_str),
        Some("zai-coding"),
        "provider field must echo adapter_type: {body}",
    );
    assert_eq!(
        body.get("detail").and_then(Value::as_str),
        Some("reachable"),
        "200 detail must be terse 'reachable': {body}",
    );

    // The probe must have landed on /models exactly once.
    assert_eq!(
        record.models_hits.load(Ordering::SeqCst),
        1,
        "probe must hit /models exactly once on the fake; this confirms \
         the zai-coding adapter no longer falls through to state.openai_compat \
         for its default base URL",
    );

    // Authorization header: `Bearer sk-zai-live-<suffix>`. We don't assert
    // the exact plaintext because the credential suffix is random per
    // LiveHarness instance, but the prefix must be present.
    let auth = record
        .last_auth
        .lock()
        .await
        .clone()
        .expect("fake observed at least one request with auth");
    assert!(
        auth.starts_with("Bearer sk-zai-live-"),
        "probe must forward Authorization: Bearer <api_key>; got {auth:?}",
    );
}

#[tokio::test]
async fn zai_coding_probe_surfaces_401_with_human_detail() {
    let h = LiveHarness::setup().await;
    let (fake_url, record) = spawn_fake_zai(401).await;
    let (connection_id, _cred_id) = register_zai_coding_connection(&h, &fake_url).await;

    let r = h
        .client()
        .get(format!(
            "{}/v1/providers/connections/{}/test",
            h.base_url, connection_id,
        ))
        .bearer_auth(&h.admin_token)
        .send()
        .await
        .expect("probe reaches server");
    assert_eq!(r.status().as_u16(), 200, "probe HTTP status");
    let body: Value = r.json().await.expect("probe body json");

    assert_eq!(
        body.get("ok").and_then(Value::as_bool),
        Some(false),
        "401 must surface as ok=false: {body}",
    );
    assert_eq!(
        body.get("status").and_then(Value::as_u64),
        Some(401),
        "status must echo upstream 401: {body}",
    );

    // Pre-#632 this was the literal string "returned non-2xx" — entirely
    // useless for the operator. The fix replaces it with a human
    // explanation keyed off the status code.
    let detail = body
        .get("detail")
        .and_then(Value::as_str)
        .expect("detail field present");
    assert!(
        detail.contains("401")
            && (detail.to_lowercase().contains("unauthoriz")
                || detail.to_lowercase().contains("credential")),
        "401 detail must include status + human-readable reason, got {detail:?}",
    );
    assert!(
        !detail.eq_ignore_ascii_case("returned non-2xx"),
        "detail must no longer be the pre-#632 sentinel, got {detail:?}",
    );

    // Fake still observed the probe — this confirms the URL resolution
    // landed on /models on the right host, not on a canonical
    // api.z.ai fallback that would give a real 401 from Z.ai itself.
    assert_eq!(
        record.models_hits.load(Ordering::SeqCst),
        1,
        "probe must hit the fake's /models even when fake returns 401",
    );
}
