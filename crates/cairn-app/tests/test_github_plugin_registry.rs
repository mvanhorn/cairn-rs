//! Integration test for the GitHub-plugin registry migration (#557).
//!
//! The legacy shim `AppState.github: Option<Arc<GitHubIntegration>>`
//! was deleted in favour of recovering the concrete `GitHubPlugin`
//! from the integration registry via
//! `state.integrations.get_typed::<GitHubPlugin>("github")`. This file
//! proves, end-to-end against a real `AppState`, that:
//!
//! 1. `get_typed` returns `Some(Arc<GitHubPlugin>)` when the plugin is
//!    registered (the handler's happy path).
//! 2. `get_typed` returns `None` before registration (the
//!    `github_not_configured` 503 path the handlers preserve).
//! 3. Downcast to the wrong type returns `None` — a misuse of
//!    `get_typed` must not succeed silently.
//! 4. The plugin survives across registry-lock drops — the Arc
//!    returned by `get_typed` keeps the plugin alive even after the
//!    read lock is dropped.
//!
//! Covers the four handler-family surfaces (webhook, queue, scan,
//! installation) by exercising the single registry lookup they all
//! now share, rather than spinning up a full HTTP round-trip per
//! handler (which would require valid GitHub credentials anyway).

mod support;

use std::sync::Arc;

use axum::http::HeaderMap;
use cairn_api::bootstrap::BootstrapConfig;
use cairn_integrations::github::{GitHubPlugin, IssueQueueEntry, IssueQueueStatus};
use cairn_integrations::{Integration, IntegrationRegistry};

use crate::support::build_test_router_fake_fabric;

/// A distinct plugin type used to verify that `get_typed` rejects a
/// downcast to the wrong concrete type — if the registry happily handed
/// back `Arc<WrongPlugin>` when `GitHubPlugin` was registered, the
/// handlers would silently break at runtime.
struct WrongPlugin;

#[async_trait::async_trait]
impl Integration for WrongPlugin {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn id(&self) -> &str {
        "wrong"
    }
    fn display_name(&self) -> &str {
        "Wrong"
    }
    fn is_configured(&self) -> bool {
        true
    }
    fn default_agent_prompt(&self) -> &str {
        ""
    }
    fn default_event_actions(&self) -> Vec<cairn_integrations::EventActionMapping> {
        Vec::new()
    }
    async fn verify_webhook(
        &self,
        _headers: &HeaderMap,
        _body: &[u8],
    ) -> Result<(), cairn_integrations::IntegrationError> {
        Ok(())
    }
    async fn parse_event(
        &self,
        _headers: &HeaderMap,
        _body: &[u8],
    ) -> Result<cairn_integrations::IntegrationEvent, cairn_integrations::IntegrationError> {
        unimplemented!()
    }
    async fn build_goal(
        &self,
        _item: &cairn_integrations::WorkItem,
    ) -> Result<String, cairn_integrations::IntegrationError> {
        Ok(String::new())
    }
    async fn prepare_tool_registry(
        &self,
        base: &cairn_tools::BuiltinToolRegistry,
        _item: &cairn_integrations::WorkItem,
    ) -> Arc<cairn_tools::BuiltinToolRegistry> {
        Arc::new(cairn_tools::BuiltinToolRegistry::from_existing(base))
    }
    fn auth_exempt_paths(&self) -> Vec<String> {
        Vec::new()
    }
    async fn queue_stats(&self) -> cairn_integrations::QueueStats {
        cairn_integrations::QueueStats::default()
    }
}

/// Minimal GitHubPlugin fixture — the test-only RSA key lives in the
/// integrations crate's test fixtures so we do not need a real GitHub
/// App to exercise the registry plumbing.
fn make_github_plugin() -> GitHubPlugin {
    // Reuse the same 2048-bit RSA key used in cairn-integrations unit
    // tests. We do not call out to GitHub here; we only construct a
    // valid plugin so the registry has something to hand back.
    let rsa_pem = include_bytes!("../../cairn-integrations/tests/fixtures/test_rsa_key.pem");
    let credentials =
        cairn_github::AppCredentials::new(12_345, rsa_pem).expect("test RSA key must parse");
    GitHubPlugin::new(credentials, "test-secret".into(), 3)
}

#[tokio::test]
async fn get_typed_returns_some_when_registered() {
    let registry = IntegrationRegistry::new();
    registry.register(Arc::new(make_github_plugin())).await;

    let plugin = registry.get_typed::<GitHubPlugin>("github").await;
    assert!(
        plugin.is_some(),
        "registered GitHubPlugin must be recoverable"
    );
    let plugin = plugin.expect("Some(Arc<GitHubPlugin>)");
    assert_eq!(plugin.webhook_secret, "test-secret");
    assert_eq!(plugin.credentials.app_id, 12_345);
}

#[tokio::test]
async fn get_typed_returns_none_before_registration() {
    let registry = IntegrationRegistry::new();
    let plugin = registry.get_typed::<GitHubPlugin>("github").await;
    assert!(
        plugin.is_none(),
        "empty registry must return None for any id — preserves the \
         github_not_configured 503 contract"
    );
}

#[tokio::test]
async fn get_typed_rejects_wrong_concrete_type() {
    let registry = IntegrationRegistry::new();
    registry.register(Arc::new(make_github_plugin())).await;

    // Asking for a different concrete type at the "github" id must not
    // silently succeed. This protects the handlers from bugs where
    // the wrong plugin is registered under a given id.
    let wrong = registry.get_typed::<WrongPlugin>("github").await;
    assert!(
        wrong.is_none(),
        "downcast to a different concrete plugin type must return None"
    );
}

#[tokio::test]
async fn get_typed_after_unregister_returns_none() {
    let registry = IntegrationRegistry::new();
    registry.register(Arc::new(make_github_plugin())).await;
    assert!(registry.get_typed::<GitHubPlugin>("github").await.is_some());

    // unregister lives on the config module but operates on the same
    // registry. After unregister both the trait-object slot AND the
    // typed slot must clear in lock-step — otherwise `get` could
    // report the plugin as gone while `get_typed` still resurrected it.
    registry.unregister("github").await.expect("unregister");
    assert!(
        registry.get_typed::<GitHubPlugin>("github").await.is_none(),
        "unregister must clear the typed slot alongside the trait-object slot"
    );
    assert!(
        registry.get("github").await.is_none(),
        "unregister must clear the trait-object slot (sanity check)"
    );
}

#[tokio::test]
async fn registered_plugin_survives_lock_drop() {
    // Handlers pattern: `state.integrations.get_typed::<GitHubPlugin>("github")`
    // hands back an `Arc<GitHubPlugin>` and the lock drops inside the
    // helper. The handler then mutates `plugin.event_actions`,
    // `plugin.issue_queue`, etc. outside the registry's read guard.
    // Prove the Arc keeps the plugin alive across that boundary.
    let registry = IntegrationRegistry::new();
    registry.register(Arc::new(make_github_plugin())).await;

    let plugin = registry
        .get_typed::<GitHubPlugin>("github")
        .await
        .expect("registered");

    // Mutate through the returned Arc — exercises the same code path
    // set_webhook_actions_handler and github_scan_handler use.
    {
        let mut queue = plugin.issue_queue.write().await;
        queue.push_back(IssueQueueEntry {
            repo: "owner/repo".into(),
            installation_id: 42,
            issue_number: 1,
            title: "Test issue".into(),
            session_id: "sess-1".into(),
            run_id: "run-1".into(),
            status: IssueQueueStatus::Pending,
        });
    }

    // Re-fetch through the registry — must see the mutation because
    // `register` + `get_typed` return Arcs pointing at the same
    // allocation.
    let plugin2 = registry
        .get_typed::<GitHubPlugin>("github")
        .await
        .expect("still registered");
    let queue = plugin2.issue_queue.read().await;
    assert_eq!(queue.len(), 1);
    assert_eq!(queue[0].issue_number, 1);
    assert_eq!(queue[0].status, IssueQueueStatus::Pending);
}

/// End-to-end against a real `AppState`: bootstrap the router the same
/// way the binary does, drop a `GitHubPlugin` into the registry via
/// the async `register` path (`register_sync` needs `&mut` which is
/// not available after bootstrap has already widened the Arc), and
/// prove a handler-shaped recovery works against the same
/// `state.integrations` field the production handlers hit.
#[tokio::test]
async fn app_state_registry_round_trip() {
    let (_router, state) = build_test_router_fake_fabric(BootstrapConfig::default()).await;

    // Before registration: handlers see `None` and respond with
    // `github_not_configured` (503 on the write path, empty-list JSON
    // on the read paths). Prove the baseline.
    assert!(state
        .integrations
        .get_typed::<GitHubPlugin>("github")
        .await
        .is_none());

    // `register` takes `&self` and mutates the inner RwLock maps, so
    // it works through any Arc clone — this is the same path the
    // runtime `POST /v1/integrations` API uses.
    state
        .integrations
        .register(Arc::new(make_github_plugin()))
        .await;

    // After registration the plugin is recoverable from any Arc to
    // the registry, including the AppState field the handlers hit.
    let plugin = state
        .integrations
        .get_typed::<GitHubPlugin>("github")
        .await
        .expect("registered plugin must be recoverable from AppState");
    assert_eq!(plugin.webhook_secret, "test-secret");

    // Also verify the trait-object path still works — other callers
    // (e.g. `build_integration_tool_registry_from_base`) use `get()`.
    let as_trait = state.integrations.get("github").await;
    assert!(
        as_trait.is_some(),
        "trait-object slot must stay in lock-step with typed slot"
    );
    assert_eq!(as_trait.as_ref().unwrap().id(), "github");
}
