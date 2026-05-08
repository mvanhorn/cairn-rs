//! Shared visibility and access context types for Phase 2 plugin/sandbox/trigger work.
//!
//! These types are declared in RFCs 015, 016, 017, and 022 and live in `cairn-domain`
//! so that downstream crates (`cairn-tools`, `cairn-workspace`, `cairn-runtime`) can
//! import exactly the projection they need without pulling in plugin internals.

use crate::events::ResolvedProviderSnapshot;
use crate::ids::RunId;
use crate::tenancy::ProjectKey;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

// ── Per-Run Tool Visibility (RFC 015 §"Per-Run Tool Visibility") ─────────

/// Context carried into prompt building and tool-search filtering so that a
/// run only sees tools from plugins enabled for its project.
///
/// Constructed by the runtime when a run starts; passed to
/// `BuiltinToolRegistry::prompt_tools_for` and the deferred-tier `tool_search`
/// filter.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct VisibilityContext {
    /// The project this run belongs to.
    pub project: ProjectKey,
    /// The run that is requesting tool visibility (None during project-level queries).
    pub run_id: Option<RunId>,
    /// Plugin IDs enabled for this project (from `PluginEnablement` projections).
    pub enabled_plugins: HashSet<String>,
    /// Per-plugin tool allowlist.
    /// Key = plugin_id.  Value = `Some(tool_names)` if the project restricts
    /// which tools are visible, `None` if all of the plugin's tools are allowed.
    pub allowlisted_tools: HashMap<String, Option<HashSet<String>>>,
    /// RFC 029: snapshot of the project's resolved knowledge provider, used
    /// for `knowledge_search` routing + per-project provider detail in
    /// operator UI. `None` when the project has not yet been
    /// configured — the amended RFC 015 filter treats `None` as equivalent
    /// to cairn-default for tool visibility.
    ///
    /// Under RFC 030 this slot *only* covers the knowledge family. The
    /// memory family is a separate slot (`resolved_memory_provider`).
    /// `memory_store` visibility is now driven by the memory snapshot, not
    /// this one — see [`crate::events::ResolvedProviderSnapshot::auto_extract`].
    #[serde(default)]
    pub resolved_knowledge_provider: Option<ResolvedProviderSnapshot>,
    /// RFC 030: snapshot of the project's resolved memory provider. Used
    /// to gate the `memory_store` built-in (hidden when the backend is
    /// auto-extract — `auto_extract = Some(true)` — because the provider
    /// picks up context from conversation turns itself; also hidden when
    /// `ingest_capable = false` for memory backends that don't accept
    /// explicit stores). `None` when the project has not been
    /// configured — treated as equivalent to cairn-default for tool
    /// visibility.
    ///
    /// Serde default: `None`. Pre-RFC-030 payloads lack this field and
    /// deserialize cleanly — the tool-visibility filter falls back to the
    /// knowledge-snapshot-based rule when the memory slot is missing, so
    /// legacy runs see the same `memory_store` visibility they did before
    /// the split. PR-G lands the producer side (projection → snapshot).
    #[serde(default)]
    pub resolved_memory_provider: Option<ResolvedProviderSnapshot>,
}

// ── Repo Access (RFC 016 §"Access Layer") ────────────────────────────────

/// Minimal context for repo-access checks in `cairn-workspace`.
///
/// `cairn-workspace` imports only this type — never `VisibilityContext` — so
/// plugin/tool concerns stay out of the workspace crate.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepoAccessContext {
    pub project: ProjectKey,
}

/// Thin projection: callers that already hold a `VisibilityContext` can
/// cheaply obtain a `RepoAccessContext` without importing workspace internals.
impl From<&VisibilityContext> for RepoAccessContext {
    fn from(vc: &VisibilityContext) -> Self {
        Self {
            project: vc.project.clone(),
        }
    }
}

// ── Signal Capture Override (RFC 015 §"Per-Project Enable State") ─────────

/// Per-project override of a plugin's declared knowledge-capture behaviour.
///
/// `None` on a field means "inherit the `SignalSource` capability default"
/// (graph projection defaults to `true`, memory ingest defaults to `false`).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignalCaptureOverride {
    /// Override automatic graph projection of received signals.
    pub graph_project: Option<bool>,
    /// Override memory ingestion of signal payloads.
    pub memory_ingest: Option<bool>,
}

// ── Plugin Category (RFC 015 §"Canonical Model") ─────────────────────────

/// Marketplace filter category for plugin descriptors.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PluginCategory {
    IssueTracker,
    ChatOps,
    Calendar,
    Files,
    CustomerSupport,
    Observability,
    DataSource,
    CommunicationChannel,
    /// Reserved for forward-compat; not surfaced as a marketplace filter in v1.
    EvalScorer,
    Other(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tenancy::ProjectKey;

    #[test]
    fn visibility_context_round_trips_through_serde() {
        let mut enabled = HashSet::new();
        enabled.insert("github".to_string());

        let mut allowed = HashMap::new();
        let mut tools = HashSet::new();
        tools.insert("github.get_issue".to_string());
        allowed.insert("github".to_string(), Some(tools));

        let ctx = VisibilityContext {
            project: ProjectKey::new("t1", "w1", "p1"),
            run_id: Some(RunId::new("run-1")),
            enabled_plugins: enabled,
            allowlisted_tools: allowed,
            resolved_knowledge_provider: None,
            resolved_memory_provider: None,
        };

        let json = serde_json::to_string(&ctx).unwrap();
        let back: VisibilityContext = serde_json::from_str(&json).unwrap();
        assert_eq!(ctx, back);
    }

    #[test]
    fn visibility_context_serde_back_compat_for_missing_provider_field() {
        // Legacy payload written before RFC 029 or RFC 030 added their
        // provider-snapshot fields deserializes with both slots `None`
        // thanks to `#[serde(default)]`.
        let legacy = r#"{
            "project": {"tenant_id": "t1", "workspace_id": "w1", "project_id": "p1"},
            "run_id": null,
            "enabled_plugins": [],
            "allowlisted_tools": {}
        }"#;
        let ctx: VisibilityContext = serde_json::from_str(legacy).unwrap();
        assert!(ctx.resolved_knowledge_provider.is_none());
        assert!(ctx.resolved_memory_provider.is_none());
    }

    #[test]
    fn visibility_context_serde_back_compat_for_rfc029_only_payload() {
        // A run that was in-flight during the RFC-030 rollout may have a
        // visibility context serialized with the RFC 029 field but without
        // the RFC 030 memory slot. It must still deserialize.
        let partial = r#"{
            "project": {"tenant_id": "t1", "workspace_id": "w1", "project_id": "p1"},
            "run_id": null,
            "enabled_plugins": [],
            "allowlisted_tools": {},
            "resolved_knowledge_provider": {
                "provider_id": "cairn-default",
                "ingest_capable": true,
                "retrieval_modes": ["hybrid"],
                "scoring_dimensions_surfaced": ["semantic_relevance"]
            }
        }"#;
        let ctx: VisibilityContext = serde_json::from_str(partial).unwrap();
        assert!(ctx.resolved_knowledge_provider.is_some());
        assert!(ctx.resolved_memory_provider.is_none());
    }

    #[test]
    fn repo_access_context_from_visibility_context() {
        let ctx = VisibilityContext {
            project: ProjectKey::new("t1", "w1", "p1"),
            run_id: Some(RunId::new("run-1")),
            enabled_plugins: HashSet::new(),
            allowlisted_tools: HashMap::new(),
            resolved_knowledge_provider: None,
            resolved_memory_provider: None,
        };

        let access: RepoAccessContext = RepoAccessContext::from(&ctx);
        assert_eq!(access.project, ctx.project);
    }

    #[test]
    fn signal_capture_override_defaults_to_none() {
        let sco = SignalCaptureOverride::default();
        assert_eq!(sco.graph_project, None);
        assert_eq!(sco.memory_ingest, None);
    }

    #[test]
    fn plugin_category_serde_round_trip() {
        let cases = vec![
            PluginCategory::IssueTracker,
            PluginCategory::ChatOps,
            PluginCategory::Calendar,
            PluginCategory::Files,
            PluginCategory::CustomerSupport,
            PluginCategory::Observability,
            PluginCategory::DataSource,
            PluginCategory::CommunicationChannel,
            PluginCategory::EvalScorer,
            PluginCategory::Other("custom_plugin".to_string()),
        ];

        for cat in &cases {
            let json = serde_json::to_string(cat).unwrap();
            let back: PluginCategory = serde_json::from_str(&json).unwrap();
            assert_eq!(&back, cat);
        }
    }
}
