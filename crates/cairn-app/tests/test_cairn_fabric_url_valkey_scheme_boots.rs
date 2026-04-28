//! PR-A (backend-config-url): the `CAIRN_FABRIC_URL` env var with a
//! `valkey://` scheme must drive a full cairn-app boot end-to-end.
//!
//! `LiveHarness::setup()` already spawns the subprocess with
//! `CAIRN_FABRIC_URL=valkey://{host}:{port}` after PR-A's env-var
//! swap (see `support/live_fabric.rs`). Reaching readiness proves the
//! parser fed a valid `BackendConfig` into `FabricRuntime::start`,
//! the ferriskey client connected, the backend seeded HMAC, and the
//! engine started — the full PR-A happy path. `/v1/status` then
//! confirms no component is `down` (which fabric-dependent event_store
//! would be if the backend connection were broken).

mod support;

use support::live_fabric::LiveHarness;

#[tokio::test]
async fn cairn_fabric_url_valkey_scheme_boots_end_to_end() {
    let h = LiveHarness::setup().await;

    // /health/ready: LiveHarness already gates on this during setup,
    // but hit it explicitly so a future refactor that removes the
    // setup-time assertion can't silently regress PR-A's boot path.
    let ready = h
        .client()
        .get(format!("{}/health/ready", h.base_url))
        .send()
        .await
        .expect("GET /health/ready reaches server");
    assert!(
        ready.status().is_success(),
        "CAIRN_FABRIC_URL=valkey:// boot must reach /health/ready 200, got {} ({})",
        ready.status(),
        ready.text().await.unwrap_or_default(),
    );

    // /v1/status: overall status must not be `incident`. The fabric
    // runtime is threaded into several service-facing components
    // (event_store, provider_routing); a broken `CAIRN_FABRIC_URL`
    // boot would surface as at least one `down` component.
    let status = h
        .client()
        .get(format!("{}/v1/status", h.base_url))
        .bearer_auth(&h.admin_token)
        .send()
        .await
        .expect("GET /v1/status reaches server");
    assert!(
        status.status().is_success(),
        "CAIRN_FABRIC_URL=valkey:// boot must surface /v1/status 200, got {}",
        status.status(),
    );
    let body: serde_json::Value = status.json().await.expect("/v1/status returns JSON");
    let overall = body
        .get("status")
        .and_then(|v| v.as_str())
        .unwrap_or_default();
    assert_ne!(
        overall, "incident",
        "CAIRN_FABRIC_URL boot must not surface incident status: {body}"
    );

    // LiveHarness Drop impl SIGKILLs the subprocess via kill_on_drop.
}
