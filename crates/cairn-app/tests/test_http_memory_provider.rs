//! RFC 030 PR-E: HTTP contract tests for the memory-provider + unified
//! providers + ingest-jobs + scoring-policy-split endpoints.

mod support;

use serde_json::{json, Value};
use support::live_fabric::LiveHarness;

fn project_path(h: &LiveHarness) -> String {
    format!("{}%2F{}%2F{}", h.tenant, h.workspace, h.project)
}

// ── PUT /v1/projects/:project/memory-provider ────────────────────────────

#[tokio::test]
async fn configure_memory_provider_accepts_cairn_default() {
    let h = LiveHarness::setup().await;
    let p = project_path(&h);
    let base = &h.base_url;

    let res = h
        .client()
        .put(format!("{base}/v1/projects/{p}/memory-provider"))
        .bearer_auth(&h.admin_token)
        .json(&json!({ "provider_ref": "cairn-default" }))
        .send()
        .await
        .expect("put reaches server");
    assert_eq!(
        res.status().as_u16(),
        200,
        "body: {}",
        res.text().await.unwrap_or_default()
    );
    let body: Value = res.json().await.expect("json body");
    assert_eq!(
        body.get("provider_ref").and_then(|v| v.as_str()),
        Some("cairn-default")
    );
}

#[tokio::test]
async fn configure_memory_provider_accepts_plugin_prefix() {
    let h = LiveHarness::setup().await;
    let p = project_path(&h);
    let base = &h.base_url;

    let res = h
        .client()
        .put(format!("{base}/v1/projects/{p}/memory-provider"))
        .bearer_auth(&h.admin_token)
        .json(&json!({ "provider_ref": "plugin:mem0" }))
        .send()
        .await
        .expect("put reaches server");
    assert_eq!(res.status().as_u16(), 200);
}

#[tokio::test]
async fn configure_memory_provider_rejects_empty_ref() {
    let h = LiveHarness::setup().await;
    let p = project_path(&h);
    let base = &h.base_url;

    let res = h
        .client()
        .put(format!("{base}/v1/projects/{p}/memory-provider"))
        .bearer_auth(&h.admin_token)
        .json(&json!({ "provider_ref": "" }))
        .send()
        .await
        .expect("put reaches server");
    assert_eq!(res.status().as_u16(), 400);
}

// ── GET /v1/projects/:project/providers ─────────────────────────────────

#[tokio::test]
async fn get_providers_returns_both_slots() {
    let h = LiveHarness::setup().await;
    let p = project_path(&h);
    let base = &h.base_url;

    let res = h
        .client()
        .get(format!("{base}/v1/projects/{p}/providers"))
        .bearer_auth(&h.admin_token)
        .send()
        .await
        .expect("get reaches server");
    assert_eq!(res.status().as_u16(), 200);
    let body: Value = res.json().await.expect("json body");
    assert!(body.get("memory").is_some(), "memory slot must be present");
    assert!(
        body.get("knowledge").is_some(),
        "knowledge slot must be present"
    );
    // Pre-PR-G both slots fall back to cairn-default through the resolver.
    assert_eq!(
        body["memory"].get("provider_ref").and_then(|v| v.as_str()),
        Some("cairn-default")
    );
    assert_eq!(
        body["knowledge"]
            .get("provider_ref")
            .and_then(|v| v.as_str()),
        Some("cairn-default")
    );
}

// ── PUT /v1/projects/:project/scoring-policy → 308 redirect ─────────────

#[tokio::test]
async fn legacy_scoring_policy_returns_308_redirect() {
    let h = LiveHarness::setup().await;
    let p = project_path(&h);
    let base = &h.base_url;

    // reqwest's default client follows 308; disable redirects so we can
    // observe the status + Location header directly.
    let no_follow = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();

    let res = no_follow
        .put(format!("{base}/v1/projects/{p}/scoring-policy"))
        .bearer_auth(&h.admin_token)
        .json(&json!({}))
        .send()
        .await
        .expect("put reaches server");
    assert_eq!(
        res.status().as_u16(),
        308,
        "expected 308 Permanent Redirect"
    );
    let loc = res
        .headers()
        .get("location")
        .and_then(|v| v.to_str().ok())
        .expect("Location header");
    assert!(
        loc.ends_with("/knowledge-scoring-policy"),
        "Location must redirect to knowledge variant: {loc}"
    );
}

// ── GET/PUT /v1/projects/:project/{memory,knowledge}-scoring-policy ─────

#[tokio::test]
async fn memory_scoring_policy_round_trip() {
    let h = LiveHarness::setup().await;
    let p = project_path(&h);
    let base = &h.base_url;

    // PUT a policy pinning semantic_weight high — valid `ScoringPolicy`
    // shape matching `cairn_memory::retrieval::ScoringPolicy`.
    let policy_body = json!({
        "weights": {
            "semantic_weight": 1.0,
            "lexical_weight": 0.0,
            "freshness_weight": 0.0,
            "staleness_weight": 0.0,
            "credibility_weight": 0.0,
            "corroboration_weight": 0.0,
            "graph_proximity_weight": 0.0,
            "recency_weight": 0.0
        },
        "freshness_decay_days": 30.0,
        "staleness_threshold_days": 90.0,
        "recency_enabled": false,
        "retrieval_mode_default": "hybrid",
        "reranker_default": "none"
    });
    let put_res = h
        .client()
        .put(format!("{base}/v1/projects/{p}/memory-scoring-policy"))
        .bearer_auth(&h.admin_token)
        .json(&policy_body)
        .send()
        .await
        .expect("put reaches server");
    assert_eq!(
        put_res.status().as_u16(),
        200,
        "body: {}",
        put_res.text().await.unwrap_or_default()
    );

    // GET the policy back — should reflect the PUT.
    let get_res = h
        .client()
        .get(format!("{base}/v1/projects/{p}/memory-scoring-policy"))
        .bearer_auth(&h.admin_token)
        .send()
        .await
        .expect("get reaches server");
    assert_eq!(get_res.status().as_u16(), 200);
    let body: Value = get_res.json().await.expect("json body");
    assert_eq!(
        body["using_default"].as_bool(),
        Some(false),
        "after PUT, using_default must be false"
    );
}

#[tokio::test]
async fn memory_scoring_policy_get_without_put_returns_default() {
    let h = LiveHarness::setup().await;
    let p = project_path(&h);
    let base = &h.base_url;

    let res = h
        .client()
        .get(format!("{base}/v1/projects/{p}/memory-scoring-policy"))
        .bearer_auth(&h.admin_token)
        .send()
        .await
        .expect("get reaches server");
    assert_eq!(res.status().as_u16(), 200);
    let body: Value = res.json().await.expect("json body");
    assert_eq!(
        body["using_default"].as_bool(),
        Some(true),
        "no PUT yet → using_default must be true"
    );
}

#[tokio::test]
async fn memory_scoring_policy_valid_dimensions_reports_cairn_default_set() {
    let h = LiveHarness::setup().await;
    let p = project_path(&h);
    let base = &h.base_url;

    let res = h
        .client()
        .get(format!(
            "{base}/v1/projects/{p}/memory-scoring-policy/valid-dimensions"
        ))
        .bearer_auth(&h.admin_token)
        .send()
        .await
        .expect("get reaches server");
    assert_eq!(res.status().as_u16(), 200);
    let body: Value = res.json().await.expect("json body");
    let dims = body["valid_dimensions"].as_array().expect("array");
    // cairn-default surfaces all five provider-required dimensions.
    assert_eq!(dims.len(), 5);
    let names: Vec<&str> = dims.iter().filter_map(|v| v.as_str()).collect();
    assert!(names.contains(&"semantic_relevance"));
    assert!(names.contains(&"lexical_relevance"));
    assert!(names.contains(&"freshness_decay"));
    assert!(names.contains(&"staleness_penalty"));
    assert!(names.contains(&"recency_of_use"));
}

// ── GET /v1/projects/:project/ingest-jobs ────────────────────────────────

#[tokio::test]
async fn ingest_jobs_returns_empty_list_for_fresh_project() {
    let h = LiveHarness::setup().await;
    let p = project_path(&h);
    let base = &h.base_url;

    let res = h
        .client()
        .get(format!("{base}/v1/projects/{p}/ingest-jobs"))
        .bearer_auth(&h.admin_token)
        .send()
        .await
        .expect("get reaches server");
    assert_eq!(res.status().as_u16(), 200);
    let body: Value = res.json().await.expect("json body");
    assert_eq!(body["family"].as_str(), Some("all"));
    assert_eq!(body["jobs"].as_array().map(|v| v.len()), Some(0));
}

#[tokio::test]
async fn ingest_jobs_family_filter_is_respected() {
    let h = LiveHarness::setup().await;
    let p = project_path(&h);
    let base = &h.base_url;

    let res = h
        .client()
        .get(format!("{base}/v1/projects/{p}/ingest-jobs?family=memory"))
        .bearer_auth(&h.admin_token)
        .send()
        .await
        .expect("get reaches server");
    assert_eq!(res.status().as_u16(), 200);
    let body: Value = res.json().await.expect("json body");
    assert_eq!(body["family"].as_str(), Some("memory"));
}
