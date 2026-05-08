//! #765 reproducer: live Z.ai with N=2 concurrent calls through the
//! same `OpenAiCompat` instance.
//!
//! NOT run by default. Set `CAIRN_TEST_LIVE_ZAI=1` AND `ZAI_API_KEY=...`
//! to run. Otherwise skipped (early-return) so CI stays green and
//! deterministic.
//!
//! What this proves / disproves:
//! - If N=2 wedges here: bug is in cairn-providers' reqwest config.
//! - If N=2 succeeds: bug is somewhere ABOVE this layer (orchestrator
//!   loop, tokio task wrapping, axum context, span propagation).

use std::sync::Arc;
use std::time::Duration;

use cairn_providers::{
    ChatMessage, ChatProvider,
    wire::openai_compat::{OpenAiCompat, ProviderConfig},
};

const ZAI_BASE_URL: &str = "https://api.z.ai/api/coding/paas/v4";
const ZAI_MODEL: &str = "glm-5.1";

fn live_zai_enabled() -> bool {
    std::env::var("CAIRN_TEST_LIVE_ZAI")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn live_zai_two_concurrent_calls() {
    if !live_zai_enabled() {
        eprintln!("CAIRN_TEST_LIVE_ZAI not set; skipping live-Z.ai repro");
        return;
    }
    let api_key =
        std::env::var("ZAI_API_KEY").expect("ZAI_API_KEY must be set when CAIRN_TEST_LIVE_ZAI=1");

    let provider = Arc::new(
        OpenAiCompat::new(
            ProviderConfig::default(),
            api_key,
            Some(ZAI_BASE_URL.to_owned()),
            Some(ZAI_MODEL.to_owned()),
            None,
            None,
            Some(60), // 60s timeout
        )
        .expect("OpenAiCompat::new"),
    );

    // Realistic-ish prompt mimicking cairn's orchestrator (8k system + 4k user).
    let big_system =
        "You are a senior orchestrator working on cairn-dogfood-roguelike. ".repeat(150);
    let big_user = "Please consider the following GitHub issue and propose an action: ".repeat(100);
    let messages = vec![ChatMessage::system(big_system), ChatMessage::user(big_user)];

    let p1 = provider.clone();
    let m1 = messages.clone();
    let p2 = provider.clone();
    let m2 = messages.clone();

    let start = std::time::Instant::now();
    let h1 = tokio::spawn(async move { p1.chat_with_tools(&m1, None, None).await });
    let h2 = tokio::spawn(async move { p2.chat_with_tools(&m2, None, None).await });

    // Per-call envelope: 30s. Z.ai responds in ~3-15s for tiny prompts.
    // If reqwest is fine, both finish in <30s. If wedge reproduces, the
    // join below will hang and the outer 90s timeout will catch it.
    let outcome = tokio::time::timeout(Duration::from_secs(90), async {
        let r1 = h1.await;
        let r2 = h2.await;
        (r1, r2)
    })
    .await;

    let elapsed = start.elapsed();
    let (r1, r2) =
        outcome.expect("90s outer timeout — N=2 LIVE WEDGE REPRODUCED in cairn-providers");

    let r1 = r1.expect("join 1").expect("call 1 must succeed");
    let r2 = r2.expect("join 2").expect("call 2 must succeed");

    eprintln!("call 1 text: {:?}", r1.text());
    eprintln!("call 2 text: {:?}", r2.text());
    eprintln!("elapsed: {:?}", elapsed);

    assert!(
        elapsed < Duration::from_secs(60),
        "live N=2 should complete well under 60s; got {elapsed:?}"
    );
}
