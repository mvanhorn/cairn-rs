//! #765 reproducer (layer 3): live Z.ai with N=2 concurrent calls
//! through `LlmDecidePhase::decide` (the layer above
//! `RoutedGenerationService`). If layers 1 and 2 pass and this fails,
//! the bug is in the prompt-building / decide-phase wrapping path.
//!
//! NOT run by default. Set `CAIRN_TEST_LIVE_ZAI=1` AND `ZAI_API_KEY=...`
//! to run.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use cairn_domain::providers::GenerationProvider;
use cairn_orchestrator::context::{GatherOutput, OrchestrationContext};
use cairn_orchestrator::decide::DecidePhase;
use cairn_orchestrator::decide_impl::LlmDecidePhase;
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

fn ctx() -> OrchestrationContext {
    OrchestrationContext {
        project: cairn_domain::ProjectKey::new("t", "w", "p"),
        session_id: cairn_domain::SessionId::new("sess_decide_live"),
        run_id: cairn_domain::RunId::new("run_decide_live"),
        task_id: None,
        iteration: 0,
        goal: "Decide on the next action for the cairn-rs orchestrator dogfood test.".to_owned(),
        agent_type: "orchestrator".to_owned(),
        run_started_at_ms: 0,
        working_dir: PathBuf::from("."),
        run_mode: cairn_domain::decisions::RunMode::Direct,
        discovered_tool_names: vec![],
        step_history: vec![],
        is_recovery: false,
        approval_timeout: None,
        visibility: None,
        parent_context: None,
        declared_but_missing: OrchestrationContext::empty_declared_but_missing(),
        agent_role_list_cache: OrchestrationContext::empty_agent_role_list_cache(),
    }
}

#[tokio::test]
async fn live_zai_two_concurrent_decide_calls() {
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
    );

    let routed = RoutedGenerationService::new(vec![binding]);
    let phase = Arc::new(LlmDecidePhase::from_routed(routed));

    let phase1 = phase.clone();
    let phase2 = phase.clone();

    let start = std::time::Instant::now();
    let h1 = tokio::spawn(async move {
        let ctx = ctx();
        let gather = GatherOutput::default();
        phase1.decide(&ctx, &gather).await
    });
    let h2 = tokio::spawn(async move {
        let ctx = ctx();
        let gather = GatherOutput::default();
        phase2.decide(&ctx, &gather).await
    });

    let outcome = tokio::time::timeout(Duration::from_secs(120), async {
        let r1 = h1.await;
        let r2 = h2.await;
        (r1, r2)
    })
    .await;

    let elapsed = start.elapsed();
    let (r1, r2) = outcome.expect("120s outer timeout — N=2 LIVE DECIDE WEDGE REPRODUCED");

    let r1 = r1.expect("join 1");
    let r2 = r2.expect("join 2");

    eprintln!("call 1 result: {:?}", r1.is_ok());
    eprintln!("call 2 result: {:?}", r2.is_ok());
    eprintln!("elapsed: {:?}", elapsed);

    let _r1 = r1.expect("call 1 must succeed");
    let _r2 = r2.expect("call 2 must succeed");

    assert!(
        elapsed < Duration::from_secs(60),
        "live N=2 decide-phase should complete well under 60s; got {elapsed:?}"
    );
}
