//! Integration plugin framework for Cairn.
//!
//! Defines the `Integration` trait that any external service (GitHub, Linear,
//! Slack, Jira, etc.) implements to plug into Cairn's orchestration pipeline.
//!
//! Each integration provides:
//! - A default agent prompt and tool set
//! - Event→action mappings for webhook-driven automation
//! - HTTP routes for webhooks, scanning, and queue management
//! - Auth-exempt paths for incoming webhook receivers
//!
//! The operator can override any of these via the API. The core orchestrator
//! is integration-agnostic — it takes whatever the plugin/operator provides.

pub mod config;
pub mod github;
pub mod linear;
pub mod local_fs;
pub mod notion;
pub mod obsidian;
pub mod types;
pub mod webhook;

pub use config::{IntegrationConfig, ToolConfig};
pub use types::*;

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use thiserror::Error;
use tokio::sync::RwLock;

/// Errors returned by integration operations.
#[derive(Debug, Error)]
pub enum IntegrationError {
    #[error("webhook verification failed: {0}")]
    VerificationFailed(String),
    #[error("event parsing failed: {0}")]
    ParseError(String),
    #[error("integration not configured: {0}")]
    NotConfigured(String),
    #[error("external API error: {0}")]
    ApiError(String),
    #[error("invalid config: {0}")]
    ConfigInvalid(String),
    #[error("invalid key/credential format: {0}")]
    KeyFormatInvalid(String),
    #[error("{0}")]
    Other(String),
}

/// A configured, active integration that can receive events, queue work,
/// and trigger agent orchestration.
///
/// Implementations live in this crate (e.g. `github::GitHubPlugin`) and are
/// registered at startup via `IntegrationRegistry::register()`.
#[async_trait]
pub trait Integration: Send + Sync + 'static {
    /// Downcast escape hatch.
    ///
    /// Returns a reference to `self` as `&dyn Any` so callers that need the
    /// concrete plugin type (e.g. the cairn-app GitHub handlers reaching
    /// into `GitHubPlugin`'s webhook secret, installation-token cache, and
    /// issue queue) can recover it via
    /// `IntegrationRegistry::get_typed::<GitHubPlugin>("github")`. Every
    /// impl just returns `self`.
    ///
    /// Prefer trait methods for anything reusable across plugins — this is
    /// the intentional escape hatch for plugin-specific concerns that do
    /// not generalise.
    fn as_any(&self) -> &dyn std::any::Any;

    /// Unique identifier (e.g. "github", "linear", "slack").
    fn id(&self) -> &str;

    /// Display name for the UI (e.g. "GitHub", "Linear").
    fn display_name(&self) -> &str;

    /// Whether this integration is currently configured and ready to process events.
    fn is_configured(&self) -> bool;

    /// Default system prompt for agents working on tasks from this integration.
    /// The operator can override this via `IntegrationOverrides`.
    fn default_agent_prompt(&self) -> &str;

    /// Default event→action mappings for this integration.
    fn default_event_actions(&self) -> Vec<EventActionMapping>;

    /// Verify an incoming webhook request (e.g. HMAC-SHA256 signature check).
    async fn verify_webhook(
        &self,
        headers: &http::HeaderMap,
        body: &[u8],
    ) -> Result<(), IntegrationError>;

    /// Parse a webhook payload into a normalised `IntegrationEvent`.
    async fn parse_event(
        &self,
        headers: &http::HeaderMap,
        body: &[u8],
    ) -> Result<IntegrationEvent, IntegrationError>;

    /// Build the goal prompt for a specific work item.
    /// This is the text the orchestrator sends to the LLM as the task.
    async fn build_goal(&self, item: &WorkItem) -> Result<String, IntegrationError>;

    /// Prepare the tool registry for a run triggered by this integration.
    ///
    /// Clones the base registry (which has all system tools) and adds
    /// integration-specific tools (e.g. `create_pr`, `merge_pr` for GitHub).
    async fn prepare_tool_registry(
        &self,
        base: &cairn_tools::BuiltinToolRegistry,
        item: &WorkItem,
    ) -> Arc<cairn_tools::BuiltinToolRegistry>;

    /// Paths that should be exempt from auth middleware
    /// (e.g. "/v1/webhooks/github" which uses its own HMAC verification).
    fn auth_exempt_paths(&self) -> Vec<String>;

    /// Current work queue statistics for this integration.
    async fn queue_stats(&self) -> QueueStats;
}

/// Registry slot — holds both views of the same underlying plugin
/// allocation so a single lock acquisition atomically updates or
/// reads the trait-object view and the typed-Any view.
///
/// The two `Arc`s point at the same heap allocation (the concrete
/// plugin `T`); the types just differ so `get` can hand back
/// `Arc<dyn Integration>` while `get_typed` can `Arc::downcast` back
/// to `Arc<T>`.
pub(crate) struct RegistrySlot {
    pub(crate) as_integration: Arc<dyn Integration>,
    pub(crate) as_any: Arc<dyn std::any::Any + Send + Sync>,
}

/// Registry of active integrations, keyed by their ID.
///
/// Also holds per-integration operator overrides that take precedence
/// over the integration's defaults.
///
/// Internally stores each integration as a `RegistrySlot` — both the
/// trait-object view and the typed-`Any` view under the same
/// `RwLock`, so `get` and `get_typed` can never observe a
/// half-registered or half-unregistered plugin. Before this change
/// the two views lived under independent locks with an `await`
/// boundary between them; that race is closed.
pub struct IntegrationRegistry {
    pub(crate) integrations: RwLock<HashMap<String, RegistrySlot>>,
    pub(crate) overrides: RwLock<HashMap<String, IntegrationOverrides>>,
    /// Stored configs for retrieval via the API.
    pub(crate) configs: RwLock<HashMap<String, IntegrationConfig>>,
}

impl IntegrationRegistry {
    pub fn new() -> Self {
        Self {
            integrations: RwLock::new(HashMap::new()),
            overrides: RwLock::new(HashMap::new()),
            configs: RwLock::new(HashMap::new()),
        }
    }

    /// Register an integration. Replaces any existing integration with
    /// the same ID in a single atomic write — no observer can see the
    /// trait-object view and the typed view disagree.
    ///
    /// Callers pass a concrete `Arc<T>` — *not* a pre-widened
    /// `Arc<dyn Integration>` — because the typed view is keyed on
    /// the concrete type. The `T: Integration + Send + Sync + 'static`
    /// bound keeps the call sites simple:
    /// `registry.register(Arc::new(GitHubPlugin::new(...)))`.
    pub async fn register<T: Integration + Send + Sync + 'static>(&self, integration: Arc<T>) {
        let id = integration.id().to_owned();
        let slot = RegistrySlot {
            as_integration: integration.clone(),
            as_any: integration,
        };
        self.integrations.write().await.insert(id, slot);
    }

    /// Synchronous registration for startup (before the async runtime is entered).
    /// Only safe when you have exclusive `&mut` access to the registry.
    ///
    /// Same atomic-slot guarantee as [`register`](Self::register): both
    /// the trait-object and typed-`Any` views land in the same slot in
    /// the same insertion.
    pub fn register_sync<T: Integration + Send + Sync + 'static>(&mut self, integration: Arc<T>) {
        let id = integration.id().to_owned();
        let slot = RegistrySlot {
            as_integration: integration.clone(),
            as_any: integration,
        };
        self.integrations.get_mut().insert(id, slot);
    }

    /// Get an integration by ID.
    pub async fn get(&self, id: &str) -> Option<Arc<dyn Integration>> {
        self.integrations
            .read()
            .await
            .get(id)
            .map(|slot| slot.as_integration.clone())
    }

    /// Get an integration by ID and downcast to a concrete plugin type.
    ///
    /// Returns `Some(Arc<T>)` when an integration is registered under
    /// `id` AND its concrete type is `T`; `None` otherwise (no matching
    /// id, or registered under that id with a different type).
    ///
    /// This is the bridge cairn-app handlers use when they need
    /// plugin-specific state that the `Integration` trait does not
    /// surface (e.g. `GitHubPlugin`'s webhook secret, installation
    /// token cache, `issue_queue`, `event_actions`). Call shape:
    /// `registry.get_typed::<GitHubPlugin>("github").await`. Returning
    /// `Arc<T>` — not `&T` — matches how the integrations are stored
    /// internally and keeps the plugin alive for the duration of the
    /// handler even after the registry lock drops.
    ///
    /// Implementation: every slot holds both a trait-object Arc and a
    /// typed-`Any` Arc over the same allocation, under a single
    /// `RwLock`. [`std::sync::Arc::downcast`] recovers the concrete
    /// type without raw pointers or `unsafe`.
    pub async fn get_typed<T: Integration + Send + Sync + 'static>(
        &self,
        id: &str,
    ) -> Option<Arc<T>> {
        let guard = self.integrations.read().await;
        let any = guard.get(id)?.as_any.clone();
        drop(guard);
        any.downcast::<T>().ok()
    }

    /// List all registered integrations.
    pub async fn list(&self) -> Vec<Arc<dyn Integration>> {
        self.integrations
            .read()
            .await
            .values()
            .map(|slot| slot.as_integration.clone())
            .collect()
    }

    /// Get the effective agent prompt for an integration (override or default).
    pub async fn effective_prompt(&self, id: &str) -> Option<String> {
        let overrides = self.overrides.read().await;
        if let Some(o) = overrides.get(id)
            && let Some(ref prompt) = o.agent_prompt
        {
            return Some(prompt.clone());
        }
        let integrations = self.integrations.read().await;
        integrations
            .get(id)
            .map(|slot| slot.as_integration.default_agent_prompt().to_owned())
    }

    /// Get the effective event→action mappings for an integration.
    pub async fn effective_event_actions(&self, id: &str) -> Vec<EventActionMapping> {
        let overrides = self.overrides.read().await;
        if let Some(o) = overrides.get(id)
            && let Some(ref actions) = o.event_actions
        {
            return actions.clone();
        }
        let integrations = self.integrations.read().await;
        integrations
            .get(id)
            .map(|slot| slot.as_integration.default_event_actions())
            .unwrap_or_default()
    }

    /// Get the operator overrides for an integration.
    pub async fn get_overrides(&self, id: &str) -> IntegrationOverrides {
        self.overrides
            .read()
            .await
            .get(id)
            .cloned()
            .unwrap_or_default()
    }

    /// Set operator overrides for an integration.
    pub async fn set_overrides(&self, id: &str, overrides: IntegrationOverrides) {
        self.overrides
            .write()
            .await
            .insert(id.to_owned(), overrides);
    }

    /// Reset operator overrides for an integration (revert to defaults).
    pub async fn clear_overrides(&self, id: &str) {
        self.overrides.write().await.remove(id);
    }

    /// Get the effective tool config for an integration.
    ///
    /// Priority: operator overrides → registration config → default (include all Core).
    pub async fn effective_tool_config(&self, id: &str) -> config::ToolConfig {
        let overrides = self.overrides.read().await;
        if let Some(o) = overrides.get(id)
            && let Some(ref tc) = o.tools
        {
            return tc.clone();
        }
        let configs = self.configs.read().await;
        if let Some(c) = configs.get(id)
            && let Some(ref tc) = c.tools
        {
            return tc.clone();
        }
        config::ToolConfig::default()
    }

    /// Collect all auth-exempt paths from all registered integrations.
    pub async fn all_auth_exempt_paths(&self) -> Vec<String> {
        let integrations = self.integrations.read().await;
        integrations
            .values()
            .flat_map(|slot| slot.as_integration.auth_exempt_paths())
            .collect()
    }

    /// Get status summaries for all integrations (used by GET /v1/integrations).
    pub async fn all_statuses(&self) -> Vec<IntegrationStatus> {
        let integrations = self.integrations.read().await;
        let overrides = self.overrides.read().await;
        let mut statuses = Vec::new();
        for slot in integrations.values() {
            let integration = &slot.as_integration;
            let id = integration.id().to_owned();
            let o = overrides.get(&id).cloned().unwrap_or_default();
            statuses.push(IntegrationStatus {
                id: id.clone(),
                display_name: integration.display_name().to_owned(),
                configured: integration.is_configured(),
                overrides: o,
                queue_stats: integration.queue_stats().await,
            });
        }
        statuses
    }
}

impl Default for IntegrationRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Minimal test integration for unit tests.
    struct MockIntegration;

    #[async_trait]
    impl Integration for MockIntegration {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
        fn id(&self) -> &str {
            "mock"
        }
        fn display_name(&self) -> &str {
            "Mock"
        }
        fn is_configured(&self) -> bool {
            true
        }
        fn default_agent_prompt(&self) -> &str {
            "You are a test agent."
        }
        fn default_event_actions(&self) -> Vec<EventActionMapping> {
            vec![EventActionMapping {
                event_pattern: "test.*".into(),
                label_filter: None,
                repo_filter: None,
                action: EventAction::CreateAndOrchestrate,
            }]
        }
        async fn verify_webhook(
            &self,
            _headers: &http::HeaderMap,
            _body: &[u8],
        ) -> Result<(), IntegrationError> {
            Ok(())
        }
        async fn parse_event(
            &self,
            _headers: &http::HeaderMap,
            _body: &[u8],
        ) -> Result<IntegrationEvent, IntegrationError> {
            Ok(IntegrationEvent {
                integration_id: "mock".into(),
                event_key: "test.created".into(),
                source_id: "1".into(),
                repository: None,
                title: Some("Test event".into()),
                body: None,
                labels: vec![],
                raw: serde_json::json!({}),
            })
        }
        async fn build_goal(&self, item: &WorkItem) -> Result<String, IntegrationError> {
            Ok(format!("Process: {}", item.title))
        }
        async fn prepare_tool_registry(
            &self,
            base: &cairn_tools::BuiltinToolRegistry,
            _item: &WorkItem,
        ) -> Arc<cairn_tools::BuiltinToolRegistry> {
            Arc::new(cairn_tools::BuiltinToolRegistry::from_existing(base))
        }
        fn auth_exempt_paths(&self) -> Vec<String> {
            vec!["/v1/webhooks/mock".into()]
        }
        async fn queue_stats(&self) -> QueueStats {
            QueueStats::default()
        }
    }

    #[tokio::test]
    async fn register_and_retrieve_integration() {
        let registry = IntegrationRegistry::new();
        registry.register(Arc::new(MockIntegration)).await;

        let mock = registry.get("mock").await;
        assert!(mock.is_some());
        assert_eq!(mock.unwrap().display_name(), "Mock");
    }

    #[tokio::test]
    async fn list_returns_all_registered() {
        let registry = IntegrationRegistry::new();
        registry.register(Arc::new(MockIntegration)).await;

        let all = registry.list().await;
        assert_eq!(all.len(), 1);
    }

    #[tokio::test]
    async fn effective_prompt_returns_default_when_no_override() {
        let registry = IntegrationRegistry::new();
        registry.register(Arc::new(MockIntegration)).await;

        let prompt = registry.effective_prompt("mock").await.unwrap();
        assert_eq!(prompt, "You are a test agent.");
    }

    #[tokio::test]
    async fn effective_prompt_returns_override_when_set() {
        let registry = IntegrationRegistry::new();
        registry.register(Arc::new(MockIntegration)).await;
        registry
            .set_overrides(
                "mock",
                IntegrationOverrides {
                    agent_prompt: Some("Custom prompt".into()),
                    ..Default::default()
                },
            )
            .await;

        let prompt = registry.effective_prompt("mock").await.unwrap();
        assert_eq!(prompt, "Custom prompt");
    }

    #[tokio::test]
    async fn clear_overrides_reverts_to_default() {
        let registry = IntegrationRegistry::new();
        registry.register(Arc::new(MockIntegration)).await;
        registry
            .set_overrides(
                "mock",
                IntegrationOverrides {
                    agent_prompt: Some("Custom".into()),
                    ..Default::default()
                },
            )
            .await;
        registry.clear_overrides("mock").await;

        let prompt = registry.effective_prompt("mock").await.unwrap();
        assert_eq!(prompt, "You are a test agent.");
    }

    #[tokio::test]
    async fn auth_exempt_paths_collects_from_all_integrations() {
        let registry = IntegrationRegistry::new();
        registry.register(Arc::new(MockIntegration)).await;

        let paths = registry.all_auth_exempt_paths().await;
        assert!(paths.contains(&"/v1/webhooks/mock".to_owned()));
    }

    #[tokio::test]
    async fn all_statuses_returns_configured_integration() {
        let registry = IntegrationRegistry::new();
        registry.register(Arc::new(MockIntegration)).await;

        let statuses = registry.all_statuses().await;
        assert_eq!(statuses.len(), 1);
        assert!(statuses[0].configured);
        assert_eq!(statuses[0].id, "mock");
    }

    #[tokio::test]
    async fn effective_event_actions_returns_default() {
        let registry = IntegrationRegistry::new();
        registry.register(Arc::new(MockIntegration)).await;

        let actions = registry.effective_event_actions("mock").await;
        assert_eq!(actions.len(), 1);
        assert_eq!(actions[0].event_pattern, "test.*");
    }

    #[tokio::test]
    async fn effective_event_actions_returns_override() {
        let registry = IntegrationRegistry::new();
        registry.register(Arc::new(MockIntegration)).await;
        registry
            .set_overrides(
                "mock",
                IntegrationOverrides {
                    event_actions: Some(vec![EventActionMapping {
                        event_pattern: "custom.*".into(),
                        label_filter: None,
                        repo_filter: None,
                        action: EventAction::Acknowledge,
                    }]),
                    ..Default::default()
                },
            )
            .await;

        let actions = registry.effective_event_actions("mock").await;
        assert_eq!(actions.len(), 1);
        assert_eq!(actions[0].event_pattern, "custom.*");
    }

    #[tokio::test]
    async fn get_typed_matches_registered_concrete_type() {
        let registry = IntegrationRegistry::new();
        registry.register(Arc::new(MockIntegration)).await;

        // Same-type lookup succeeds.
        let typed = registry.get_typed::<MockIntegration>("mock").await;
        assert!(typed.is_some());
    }

    #[tokio::test]
    async fn get_and_get_typed_observe_same_slot_atomically() {
        // Every `register` / `unregister` is a single atomic write on
        // the slot map, so `get` and `get_typed` can never disagree on
        // whether an id is present. This test exercises the common
        // cases; stress-testing under contention is out of scope for a
        // unit test (the invariant is structural, not probabilistic).
        let registry = IntegrationRegistry::new();

        // Before any registration: both report None.
        assert!(registry.get("mock").await.is_none());
        assert!(
            registry
                .get_typed::<MockIntegration>("mock")
                .await
                .is_none()
        );

        // After register: both report Some.
        registry.register(Arc::new(MockIntegration)).await;
        assert!(registry.get("mock").await.is_some());
        assert!(
            registry
                .get_typed::<MockIntegration>("mock")
                .await
                .is_some()
        );

        // After unregister: both report None.
        registry.unregister("mock").await.expect("unregister");
        assert!(registry.get("mock").await.is_none());
        assert!(
            registry
                .get_typed::<MockIntegration>("mock")
                .await
                .is_none()
        );
    }
}
