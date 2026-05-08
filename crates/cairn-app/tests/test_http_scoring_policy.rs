//! RFC 029 PR-B2 / RFC 030 PR-E: HTTP contract for the scoring-policy
//! endpoints. Under RFC 030 the original `PUT /v1/projects/:project/
//! scoring-policy` was split into knowledge- and memory-family variants;
//! these tests exercise the knowledge variant. The legacy endpoint now
//! 308-redirects to the knowledge variant (see
//! `test_http_memory_provider::legacy_scoring_policy_returns_308_redirect`).

mod support;

use serde_json::{json, Value};
use support::live_fabric::LiveHarness;

fn project_path(h: &LiveHarness) -> String {
    format!("{}%2F{}%2F{}", h.tenant, h.workspace, h.project)
}

#[tokio::test]
async fn configure_scoring_policy_accepts_default_on_cairn_default_provider() {
    // No provider configured → resolver defaults to cairn-default,
    // which surfaces every provider-required dimension. The default
    // policy passes validation.
    let h = LiveHarness::setup().await;
    let p = project_path(&h);
    let base = &h.base_url;

    let policy = json!({
        "weights": {
            "semantic_weight": 0.4,
            "lexical_weight": 0.3,
            "freshness_weight": 0.1,
            "staleness_weight": 0.05,
            "credibility_weight": 0.05,
            "corroboration_weight": 0.03,
            "graph_proximity_weight": 0.05,
            "recency_weight": 0.02,
        },
        "freshness_decay_days": 30.0,
        "staleness_threshold_days": 90.0,
        "recency_enabled": false,
        "retrieval_mode_default": "hybrid",
        "reranker_default": "none",
    });

    let res = h
        .client()
        .put(format!("{base}/v1/projects/{p}/knowledge-scoring-policy"))
        .bearer_auth(&h.admin_token)
        .json(&policy)
        .send()
        .await
        .expect("put reaches server");
    assert_eq!(
        res.status().as_u16(),
        200,
        "status, body: {}",
        res.text().await.unwrap_or_default()
    );
    let body: Value = res.json().await.expect("json body");
    // Cairn-default surfaces every provider-required dimension so the
    // snapshot flows through to the response.
    assert_eq!(
        body.get("provider_id").and_then(Value::as_str),
        Some("cairn-default")
    );
}

#[tokio::test]
async fn configure_scoring_policy_rejects_zero_weight_on_runtime_owned_stays_fine() {
    // Runtime-owned weights are always fine regardless of provider.
    // Set lexical/freshness/staleness/recency to zero so we don't trip
    // the fully-surfacing-provider guard on cairn-default either.
    let h = LiveHarness::setup().await;
    let p = project_path(&h);
    let base = &h.base_url;

    let policy = json!({
        "weights": {
            "semantic_weight": 0.5,
            "lexical_weight": 0.0,
            "freshness_weight": 0.0,
            "staleness_weight": 0.0,
            "credibility_weight": 0.3,
            "corroboration_weight": 0.1,
            "graph_proximity_weight": 0.1,
            "recency_weight": 0.0,
        },
        "freshness_decay_days": 30.0,
        "staleness_threshold_days": 90.0,
        "recency_enabled": false,
        "retrieval_mode_default": "hybrid",
        "reranker_default": "none",
    });

    let res = h
        .client()
        .put(format!("{base}/v1/projects/{p}/knowledge-scoring-policy"))
        .bearer_auth(&h.admin_token)
        .json(&policy)
        .send()
        .await
        .expect("put reaches server");
    assert_eq!(res.status().as_u16(), 200);
}
