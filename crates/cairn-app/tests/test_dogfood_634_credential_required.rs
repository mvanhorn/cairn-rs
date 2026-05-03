//! #634: `POST /v1/providers/connections` must refuse credential-less
//! registration for adapters that require an API key.
//!
//! Observed during dogfood v3 (2026-05-03). The Add-Provider wizard
//! skipped the Connection step and registered a Z.ai coding connection
//! with no credential binding. Every subsequent chat call returned 401
//! from upstream, but the connection record looked healthy in the list.
//!
//! Contract introduced by this fix:
//!   - POST without `credential_id` for a non-ollama/non-bedrock adapter
//!     AND no pre-bound `provider_credential_<id>` default → 422
//!     `credential_required` with a message naming the adapter.
//!   - POST with a valid `credential_id` → 201 (happy path).
//!   - POST without `credential_id` for `ollama` → 201 (ollama is
//!     unauthenticated by design).
//!
//! The three tests below pin each of those three paths.

mod support;

use serde_json::{json, Value};
use support::live_fabric::LiveHarness;

#[tokio::test]
async fn zai_without_credential_returns_422() {
    let h = LiveHarness::setup().await;
    let suffix = h.project.clone();
    let tenant = "default_tenant".to_owned();
    let connection_id = format!("dogfood634_nocred_{suffix}");

    // Register a Z.ai coding connection without `credential_id` and
    // without pre-binding a credential default. Before the fix this
    // returned 201 and the connection only failed on first chat. After
    // the fix the handler refuses the write with a typed 422.
    let r = h
        .client()
        .post(format!("{}/v1/providers/connections", h.base_url))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "tenant_id": tenant,
            "provider_connection_id": connection_id,
            "provider_family": "zai",
            "adapter_type": "zai-coding",
            "supported_models": ["glm-4.7"],
            // Note: no credential_id — this is exactly the body the UI
            // sent in the dogfood repro.
        }))
        .send()
        .await
        .expect("create reaches server");

    let status = r.status().as_u16();
    let body = r.json::<Value>().await.expect("422 body must be JSON");
    assert_eq!(
        status, 422,
        "credential-less POST for zai-coding must return 422, got {status}: {body}",
    );
    let code = body
        .get("code")
        .and_then(|v| v.as_str())
        .unwrap_or_default();
    assert_eq!(
        code, "credential_required",
        "422 body must carry a typed `credential_required` code, got: {body}",
    );
    let message = body
        .get("message")
        .and_then(|v| v.as_str())
        .unwrap_or_default();
    assert!(
        message.contains("zai-coding"),
        "422 message must name the adapter so the operator knows what \
         to fix, got: {message:?}",
    );

    // The refused write must NOT have created a connection record —
    // otherwise a subsequent create-with-credential would collide.
    let r = h
        .client()
        .get(format!(
            "{}/v1/providers/connections?tenant_id={}",
            h.base_url, tenant,
        ))
        .bearer_auth(&h.admin_token)
        .send()
        .await
        .expect("list reaches server");
    assert_eq!(r.status().as_u16(), 200);
    let list: Value = r.json().await.unwrap();
    let items = list
        .get("items")
        .and_then(|v| v.as_array())
        .or_else(|| list.as_array())
        .cloned()
        .unwrap_or_default();
    let exists = items.iter().any(|v| {
        v.get("provider_connection_id")
            .and_then(|v| v.as_str())
            .map(|s| s == connection_id)
            .unwrap_or(false)
    });
    assert!(
        !exists,
        "refused POST must not leave a connection record behind \
         (found {connection_id} in {items:?})",
    );
}

#[tokio::test]
async fn zai_with_credential_succeeds() {
    let h = LiveHarness::setup().await;
    let suffix = h.project.clone();
    let tenant = "default_tenant".to_owned();
    let connection_id = format!("dogfood634_cred_{suffix}");

    // Happy path: create a credential, pass its id in the body, the
    // connection registers successfully. This pins the positive case so
    // a future over-zealous tightening of the validation doesn't block
    // the first-touch onboarding flow the UI drives.
    let r = h
        .client()
        .post(format!(
            "{}/v1/admin/tenants/{}/credentials",
            h.base_url, tenant,
        ))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "provider_id": connection_id,
            "plaintext_value": format!("sk-dogfood-634-{suffix}"),
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
            "tenant_id": tenant,
            "provider_connection_id": connection_id,
            "provider_family": "zai",
            "adapter_type": "zai-coding",
            "supported_models": ["glm-4.7"],
            "credential_id": credential_id,
            "endpoint_url": "https://api.z.ai/api/coding/paas/v4",
        }))
        .send()
        .await
        .expect("create reaches server");
    assert_eq!(
        r.status().as_u16(),
        201,
        "happy-path create must succeed: {}",
        r.text().await.unwrap_or_default(),
    );
}

#[tokio::test]
async fn ollama_without_credential_succeeds() {
    let h = LiveHarness::setup().await;
    let suffix = h.project.clone();
    let tenant = "default_tenant".to_owned();
    let connection_id = format!("dogfood634_ollama_{suffix}");

    // Ollama runs unauthenticated against a local daemon — the handler
    // must not require a credential for it, or we'd block the primary
    // local-mode onboarding path.
    let r = h
        .client()
        .post(format!("{}/v1/providers/connections", h.base_url))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "tenant_id": tenant,
            "provider_connection_id": connection_id,
            "provider_family": "ollama",
            "adapter_type": "ollama",
            "supported_models": ["llama3.2:3b"],
            "endpoint_url": "http://localhost:11434/v1",
            // Note: no credential_id — ollama doesn't need one.
        }))
        .send()
        .await
        .expect("create reaches server");
    assert_eq!(
        r.status().as_u16(),
        201,
        "ollama create must succeed without credentials: {}",
        r.text().await.unwrap_or_default(),
    );
}
