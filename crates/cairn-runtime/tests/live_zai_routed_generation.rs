//! #765 reproducer (layer 2): live Z.ai with N=2 concurrent calls
//! through `RoutedGenerationService::generate` (the layer above
//! `OpenAiCompat`). If `cairn-providers/tests/live_zai_concurrent_repro`
//! passes (N=2 ok at the wire layer) and this fails, the bug is in
//! the routing/wrapping layer, not the wire client.
//!
//! NOT run by default. Set `CAIRN_TEST_LIVE_ZAI=1` AND `ZAI_API_KEY=...`
//! to run.

use std::sync::Arc;
use std::time::Duration;

use cairn_domain::providers::{GenerationProvider, ProviderBindingSettings};
use cairn_providers::wire::openai_compat::{OpenAiCompat, ProviderConfig};
use cairn_runtime::services::model_chain::ModelChain;
use cairn_runtime::services::routed_generation::{RoutedBinding, RoutedGenerationService};

const ZAI_BASE_URL: &str = "https://api.z.ai/api/coding/paas/v4";
const ZAI_MODEL: &str = "glm-5.1";

fn live_zai_enabled() -> bool {
    std::env::var("CAIRN_TEST_LIVE_ZAI")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

#[tokio::test]
async fn live_zai_two_concurrent_routed_calls() {
    if !live_zai_enabled() {
        eprintln!("CAIRN_TEST_LIVE_ZAI not set; skipping");
        return;
    }
    let api_key = std::env::var("ZAI_API_KEY").expect("ZAI_API_KEY must be set");

    let openai_compat = OpenAiCompat::new(
        ProviderConfig::default(),
        api_key,
        Some(ZAI_BASE_URL.to_owned()),
        Some(ZAI_MODEL.to_owned()),
        None,
        None,
        Some(60),
    )
    .expect("OpenAiCompat::new");

    // OpenAiCompat impls GenerationProvider directly (cairn-providers/src/bridge.rs:150).
    let gen_provider: Arc<dyn GenerationProvider> = Arc::new(openai_compat);

    // Build a single binding pointing at the gen_provider, with the
    // default per-binding concurrency cap.
    let binding = RoutedBinding::new(
        "live-zai",
        gen_provider,
        ModelChain::single(ZAI_MODEL).with_retry_budget(0, Duration::ZERO),
    );

    let svc = Arc::new(RoutedGenerationService::new(vec![binding]));

    // Build messages mimicking the orchestrator's first-DECIDE shape.
    let big_system =
        "You are a senior orchestrator working on cairn-dogfood-roguelike. ".repeat(120);
    let big_user = "Please consider this issue and propose an action: ".repeat(60);
    let messages = vec![
        serde_json::json!({"role": "system", "content": big_system}),
        serde_json::json!({"role": "user", "content": big_user}),
    ];

    let svc1 = svc.clone();
    let svc2 = svc.clone();
    let m1 = messages.clone();
    let m2 = messages.clone();

    let start = std::time::Instant::now();
    let h1 = tokio::spawn(async move {
        svc1.generate(m1, &ProviderBindingSettings::default(), &[])
            .await
    });
    let h2 = tokio::spawn(async move {
        svc2.generate(m2, &ProviderBindingSettings::default(), &[])
            .await
    });

    let outcome = tokio::time::timeout(Duration::from_secs(120), async {
        let r1 = h1.await;
        let r2 = h2.await;
        (r1, r2)
    })
    .await;

    let elapsed = start.elapsed();
    let (r1, r2) = outcome.expect("120s outer timeout — N=2 LIVE ROUTED WEDGE REPRODUCED");

    let r1 = r1.expect("join 1").expect("call 1 must succeed");
    let r2 = r2.expect("join 2").expect("call 2 must succeed");

    eprintln!("call 1 binding: {}", r1.binding_id);
    eprintln!("call 2 binding: {}", r2.binding_id);
    eprintln!("elapsed: {:?}", elapsed);

    assert!(
        elapsed < Duration::from_secs(60),
        "live N=2 routed should complete well under 60s; got {elapsed:?}"
    );
}

/// #765 reproducer (volume): N=8 concurrent calls — the actual dogfood
/// pattern. The N=2 test above passes; this one matches the production
/// load that the dogfood manager produces (8 root runs in parallel).
///
/// If this wedges and the N=2 test passes, the bug is concurrency-
/// dependent — likely (a) reqwest connection-pool serialisation,
/// (b) rustls handshake serialisation, or (c) Z.ai server-side per-key
/// concurrency limit that queues indefinitely.
#[tokio::test]
async fn live_zai_eight_concurrent_routed_calls() {
    if !live_zai_enabled() {
        eprintln!("CAIRN_TEST_LIVE_ZAI not set; skipping");
        return;
    }
    let api_key = std::env::var("ZAI_API_KEY").expect("ZAI_API_KEY must be set");

    let openai_compat = OpenAiCompat::new(
        ProviderConfig::default(),
        api_key,
        Some(ZAI_BASE_URL.to_owned()),
        Some(ZAI_MODEL.to_owned()),
        None,
        None,
        Some(60),
    )
    .expect("OpenAiCompat::new");

    let gen_provider: Arc<dyn GenerationProvider> = Arc::new(openai_compat);

    let binding = RoutedBinding::new(
        "live-zai",
        gen_provider,
        ModelChain::single(ZAI_MODEL).with_retry_budget(0, Duration::ZERO),
    )
    // Match the dogfood: per-binding cap=8 so N=8 runs all-at-once
    // through the wire layer (no internal serialisation by the cap).
    .with_concurrency(8);

    let svc = Arc::new(RoutedGenerationService::new(vec![binding]));

    let big_system =
        "You are a senior orchestrator working on cairn-dogfood-roguelike. ".repeat(120);
    let big_user = "Please consider this issue and propose an action: ".repeat(60);
    let messages = vec![
        serde_json::json!({"role": "system", "content": big_system}),
        serde_json::json!({"role": "user", "content": big_user}),
    ];

    let start = std::time::Instant::now();
    let mut handles = Vec::with_capacity(8);
    for i in 0..8 {
        let s = svc.clone();
        let m = messages.clone();
        handles.push(tokio::spawn(async move {
            let t0 = std::time::Instant::now();
            let r = s
                .generate(m, &ProviderBindingSettings::default(), &[])
                .await;
            (i, t0.elapsed(), r)
        }));
    }

    let outcome = tokio::time::timeout(Duration::from_secs(180), async {
        let mut out = Vec::with_capacity(handles.len());
        for h in handles {
            out.push(h.await);
        }
        out
    })
    .await;

    let elapsed = start.elapsed();
    let results = outcome
        .expect("180s outer timeout — N=8 LIVE ROUTED WEDGE REPRODUCED at the routing layer");

    let mut succeeded = 0u32;
    let mut failed = 0u32;
    for r in results {
        match r {
            Ok((i, dt, Ok(_))) => {
                eprintln!("call {i}: ok in {:?}", dt);
                succeeded += 1;
            }
            Ok((i, dt, Err(e))) => {
                eprintln!("call {i}: ERR after {:?}: {:?}", dt, e);
                failed += 1;
            }
            Err(e) => {
                eprintln!("join error: {:?}", e);
                failed += 1;
            }
        }
    }
    eprintln!(
        "N=8 summary: {succeeded} ok, {failed} failed, total elapsed {:?}",
        elapsed
    );

    assert_eq!(
        succeeded, 8,
        "all 8 routed calls must succeed; only {succeeded}/8 did"
    );
}

/// #765 reproducer (volume + connection-pool isolation): N=8 concurrent
/// calls each through its own `OpenAiCompat` and its own `reqwest::Client`.
/// This matches what cairn-app actually does: each orchestrate handler
/// constructs its own `RoutedGenerationService` (and therefore its own
/// provider Arc) per request, even though the Arc *contents* (the
/// underlying `OpenAiCompat`) come from a per-tenant cache.
///
/// The previous N=8 test shared one `OpenAiCompat` (one reqwest::Client)
/// across 8 calls. If that passed and this one wedges, the bug is in
/// reqwest connection-pool exhaustion when 8 distinct clients each try
/// to open a new TCP+TLS connection to the same host concurrently.
#[tokio::test]
async fn live_zai_eight_separate_clients() {
    if !live_zai_enabled() {
        eprintln!("CAIRN_TEST_LIVE_ZAI not set; skipping");
        return;
    }
    let api_key = std::env::var("ZAI_API_KEY").expect("ZAI_API_KEY must be set");

    let big_system =
        "You are a senior orchestrator working on cairn-dogfood-roguelike. ".repeat(120);
    let big_user = "Please consider this issue and propose an action: ".repeat(60);
    let messages = vec![
        serde_json::json!({"role": "system", "content": big_system}),
        serde_json::json!({"role": "user", "content": big_user}),
    ];

    let start = std::time::Instant::now();
    let mut handles = Vec::with_capacity(8);
    for i in 0..8 {
        let api_key = api_key.clone();
        let messages = messages.clone();
        handles.push(tokio::spawn(async move {
            // Build a NEW provider per task — its own reqwest::Client.
            let openai_compat = OpenAiCompat::new(
                ProviderConfig::default(),
                api_key,
                Some(ZAI_BASE_URL.to_owned()),
                Some(ZAI_MODEL.to_owned()),
                None,
                None,
                Some(60),
            )
            .expect("OpenAiCompat::new");
            let gen_provider: Arc<dyn GenerationProvider> = Arc::new(openai_compat);
            let binding = RoutedBinding::new(
                "live-zai",
                gen_provider,
                ModelChain::single(ZAI_MODEL).with_retry_budget(0, Duration::ZERO),
            );
            let svc = RoutedGenerationService::new(vec![binding]);
            let t0 = std::time::Instant::now();
            let r = svc
                .generate(messages, &ProviderBindingSettings::default(), &[])
                .await;
            (i, t0.elapsed(), r)
        }));
    }

    let outcome = tokio::time::timeout(Duration::from_secs(180), async {
        let mut out = Vec::with_capacity(handles.len());
        for h in handles {
            out.push(h.await);
        }
        out
    })
    .await;

    let elapsed = start.elapsed();
    let results = outcome.expect("180s outer timeout — N=8 SEPARATE-CLIENT WEDGE REPRODUCED");

    let mut succeeded = 0u32;
    for r in results {
        match r {
            Ok((i, dt, Ok(_))) => {
                eprintln!("call {i}: ok in {:?}", dt);
                succeeded += 1;
            }
            Ok((i, dt, Err(e))) => {
                eprintln!("call {i}: ERR after {:?}: {:?}", dt, e);
            }
            Err(e) => {
                eprintln!("join error: {:?}", e);
            }
        }
    }
    eprintln!(
        "N=8 separate clients: {succeeded} ok, total elapsed {:?}",
        elapsed
    );

    assert_eq!(succeeded, 8, "all 8 separate-client calls must succeed");
}
