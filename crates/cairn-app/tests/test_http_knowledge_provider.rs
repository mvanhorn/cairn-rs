//! RFC 029: PUT /v1/projects/:project/knowledge-provider HTTP contract.

mod support;

use serde_json::{json, Value};
use support::live_fabric::LiveHarness;

/// Percent-encode the tenant/workspace/project triple into one Axum path
/// segment (mirrors the LiveHarness id alphabet; see the equivalent
/// helper in `test_http_plugin_lifecycle.rs`).
fn project_path(h: &LiveHarness) -> String {
    format!("{}%2F{}%2F{}", h.tenant, h.workspace, h.project)
}

#[tokio::test]
async fn configure_knowledge_provider_accepts_cairn_default() {
    let h = LiveHarness::setup().await;
    let p = project_path(&h);
    let base = &h.base_url;

    let res = h
        .client()
        .put(format!("{base}/v1/projects/{p}/knowledge-provider"))
        .bearer_auth(&h.admin_token)
        .json(&json!({ "provider_ref": "cairn-default" }))
        .send()
        .await
        .expect("put reaches server");
    assert_eq!(
        res.status().as_u16(),
        200,
        "status, body: {}",
        res.text().await.unwrap_or_default(),
    );
    let body: Value = res.json().await.expect("json body");
    assert_eq!(
        body.get("provider_ref").and_then(|v| v.as_str()),
        Some("cairn-default"),
    );
}

#[tokio::test]
async fn configure_knowledge_provider_accepts_plugin_prefix() {
    let h = LiveHarness::setup().await;
    let p = project_path(&h);
    let base = &h.base_url;

    let res = h
        .client()
        .put(format!("{base}/v1/projects/{p}/knowledge-provider"))
        .bearer_auth(&h.admin_token)
        .json(&json!({ "provider_ref": "plugin:mem0" }))
        .send()
        .await
        .expect("put reaches server");
    assert_eq!(res.status().as_u16(), 200);
    let body: Value = res.json().await.expect("json body");
    assert_eq!(
        body.get("provider_ref").and_then(|v| v.as_str()),
        Some("plugin:mem0"),
    );
}

#[tokio::test]
async fn configure_knowledge_provider_rejects_empty_ref() {
    let h = LiveHarness::setup().await;
    let p = project_path(&h);
    let base = &h.base_url;

    let res = h
        .client()
        .put(format!("{base}/v1/projects/{p}/knowledge-provider"))
        .bearer_auth(&h.admin_token)
        .json(&json!({ "provider_ref": "" }))
        .send()
        .await
        .expect("put reaches server");
    assert_eq!(res.status().as_u16(), 400);
}
