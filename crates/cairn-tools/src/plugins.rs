use cairn_domain::policy::ExecutionClass;
use serde::{Deserialize, Serialize};

use crate::mcp_client::McpEndpoint;
use crate::permissions::DeclaredPermissions;

/// Plugin capability families per RFC 007.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PluginCapability {
    ToolProvider {
        tools: Vec<String>,
    },
    SignalSource {
        signals: Vec<String>,
    },
    ChannelProvider {
        channels: Vec<String>,
    },
    PostTurnHook,
    PolicyHook,
    EvalScorer,
    /// RFC 029: this plugin provides knowledge retrieval and optionally ingest.
    ///
    /// The manifest entry is intentionally empty — effective capability
    /// detail (retrieval modes, ingest capability, per-dimension scoring
    /// support) is negotiated at the `initialize` handshake via
    /// [`cairn_plugin_proto::knowledge::KnowledgeProviderCapability`],
    /// because that detail legitimately depends on runtime state
    /// (credentials, backend reachability) that is not available at
    /// manifest-parse time. See RFC 029 §"Capability declaration".
    KnowledgeProvider,
    /// This plugin connects to an external MCP server.
    ///
    /// When registered, cairn-tools will connect to the server via `McpClient`
    /// and expose its tools under the `mcp.<server_id>.<tool>` namespace.
    McpServer {
        endpoint: McpEndpoint,
    },
}

impl PluginCapability {
    /// Return the capability family this variant belongs to, per RFC 007.
    ///
    /// Returns `None` for `McpServer`, which is a cairn-internal capability
    /// variant and not one of the RFC 007 families.
    pub fn family(&self) -> Option<cairn_plugin_proto::CapabilityFamily> {
        use cairn_plugin_proto::CapabilityFamily;
        Some(match self {
            PluginCapability::ToolProvider { .. } => CapabilityFamily::ToolProvider,
            PluginCapability::SignalSource { .. } => CapabilityFamily::SignalSource,
            PluginCapability::ChannelProvider { .. } => CapabilityFamily::ChannelProvider,
            PluginCapability::PostTurnHook => CapabilityFamily::PostTurnHook,
            PluginCapability::PolicyHook => CapabilityFamily::PolicyHook,
            PluginCapability::EvalScorer => CapabilityFamily::EvalScorer,
            PluginCapability::KnowledgeProvider => CapabilityFamily::KnowledgeProvider,
            PluginCapability::McpServer { .. } => return None,
        })
    }
}

/// Manifest-validation error emitted when a plugin declares two capability
/// families that cairn considers mutually exclusive.
///
/// The only v1 rule, per RFC 029, rejects a manifest that declares both
/// `knowledge_provider` and `signal_source`. The rationale is that
/// knowledge-provider plugins rely on RFC 015's lazy-spawn rule (tool-only
/// plugins spawn on first invocation), which requires absence of
/// `signal_source`. A plugin author who needs both behaviors ships two
/// separate plugins.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CapabilityConflict {
    pub families: Vec<cairn_plugin_proto::CapabilityFamily>,
    pub reason: &'static str,
}

impl std::fmt::Display for CapabilityConflict {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "capability conflict (families: {:?}): {}",
            self.families.iter().map(|f| f.as_str()).collect::<Vec<_>>(),
            self.reason
        )
    }
}

impl std::error::Error for CapabilityConflict {}

/// Concurrency and timeout limits declared in the plugin manifest.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PluginLimits {
    pub max_concurrency: Option<u32>,
    pub default_timeout_ms: Option<u64>,
}

/// Declarative plugin manifest loaded before process spawn.
/// Shape follows RFC 007 canonical manifest.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PluginManifest {
    pub id: String,
    pub name: String,
    pub version: String,
    pub command: Vec<String>,
    pub capabilities: Vec<PluginCapability>,
    pub permissions: DeclaredPermissions,
    pub limits: Option<PluginLimits>,
    pub execution_class: ExecutionClass,
    /// RFC 007: human-readable description of what the plugin does.
    #[serde(default)]
    pub description: Option<String>,
    /// RFC 007: URL for plugin documentation or source repository.
    #[serde(default)]
    pub homepage: Option<String>,
}

impl PluginManifest {
    /// Validate cross-capability rules at Discover time (RFC 007 step 1).
    ///
    /// v1 rule (RFC 029): a manifest MUST NOT declare both `knowledge_provider`
    /// and `signal_source` in its capabilities array. Knowledge providers rely
    /// on RFC 015's lazy-spawn rule (tool-only plugins spawn on first
    /// invocation), which requires absence of `signal_source`.
    ///
    /// Per-capability inner-shape validation (e.g., tri-state scoring
    /// dimensions for `knowledge_provider`) happens at Handshake, not here —
    /// it legitimately depends on runtime state and is negotiated via
    /// `initialize`.
    pub fn validate(&self) -> Result<(), CapabilityConflict> {
        use cairn_plugin_proto::CapabilityFamily;
        let mut has_knowledge = false;
        let mut has_signal = false;
        for cap in &self.capabilities {
            if let Some(fam) = cap.family() {
                match fam {
                    CapabilityFamily::KnowledgeProvider => has_knowledge = true,
                    CapabilityFamily::SignalSource => has_signal = true,
                    _ => {}
                }
            }
        }
        if has_knowledge && has_signal {
            return Err(CapabilityConflict {
                families: vec![
                    CapabilityFamily::KnowledgeProvider,
                    CapabilityFamily::SignalSource,
                ],
                reason: "knowledge providers rely on lazy-spawn; declaring signal_source forces eager spawn (RFC 015). Ship two plugins instead.",
            });
        }
        Ok(())
    }
}

/// Plugin lifecycle states managed by the host.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PluginState {
    Discovered,
    Spawning,
    Handshaking,
    Ready,
    Draining,
    Stopped,
    Failed,
}

impl PluginState {
    pub fn is_operational(self) -> bool {
        matches!(self, PluginState::Ready)
    }

    pub fn is_terminal(self) -> bool {
        matches!(self, PluginState::Stopped | PluginState::Failed)
    }
}

/// Seam for plugin host lifecycle management.
/// The host discovers, spawns, handshakes, and shuts down plugins.
pub trait PluginHost {
    type Error;

    fn discover(&self, manifest: &PluginManifest) -> Result<(), Self::Error>;
    fn spawn(&mut self, plugin_id: &str) -> Result<(), Self::Error>;
    fn shutdown(&mut self, plugin_id: &str) -> Result<(), Self::Error>;
    fn state(&self, plugin_id: &str) -> Option<PluginState>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::permissions::{DeclaredPermissions, Permission};
    use cairn_domain::policy::ExecutionClass;

    #[test]
    fn manifest_carries_capabilities_and_permissions() {
        let manifest = PluginManifest {
            id: "com.example.git-tools".to_owned(),
            name: "Git Tools".to_owned(),
            version: "0.1.0".to_owned(),
            command: vec!["plugin-binary".to_owned(), "--serve".to_owned()],
            capabilities: vec![PluginCapability::ToolProvider {
                tools: vec!["git.status".to_owned(), "git.diff".to_owned()],
            }],
            permissions: DeclaredPermissions::new(vec![
                Permission::FsRead,
                Permission::ProcessExec,
            ]),
            limits: Some(PluginLimits {
                max_concurrency: Some(4),
                default_timeout_ms: Some(30_000),
            }),
            execution_class: ExecutionClass::SupervisedProcess,
            description: None,
            homepage: None,
        };

        assert_eq!(manifest.capabilities.len(), 1);
        assert!(manifest.permissions.contains(&Permission::FsRead));
        assert_eq!(manifest.execution_class, ExecutionClass::SupervisedProcess);
    }

    #[test]
    fn plugin_state_lifecycle() {
        assert!(!PluginState::Discovered.is_operational());
        assert!(!PluginState::Spawning.is_operational());
        assert!(PluginState::Ready.is_operational());
        assert!(PluginState::Stopped.is_terminal());
        assert!(PluginState::Failed.is_terminal());
        assert!(!PluginState::Draining.is_terminal());
    }

    #[test]
    fn multi_capability_manifest() {
        let manifest = PluginManifest {
            id: "com.example.multi".to_owned(),
            name: "Multi Plugin".to_owned(),
            version: "0.2.0".to_owned(),
            command: vec!["multi-plugin".to_owned()],
            capabilities: vec![
                PluginCapability::ToolProvider {
                    tools: vec!["tool.a".to_owned()],
                },
                PluginCapability::PolicyHook,
                PluginCapability::EvalScorer,
            ],
            permissions: DeclaredPermissions::default(),
            limits: None,
            execution_class: ExecutionClass::SandboxedProcess,
            description: None,
            homepage: None,
        };

        assert_eq!(manifest.capabilities.len(), 3);
        assert_eq!(manifest.execution_class, ExecutionClass::SandboxedProcess);
    }

    fn manifest_with_capabilities(caps: Vec<PluginCapability>) -> PluginManifest {
        PluginManifest {
            id: "com.example.test".to_owned(),
            name: "Test".to_owned(),
            version: "0.1.0".to_owned(),
            command: vec!["plugin".to_owned()],
            capabilities: caps,
            permissions: DeclaredPermissions::default(),
            limits: None,
            execution_class: ExecutionClass::SupervisedProcess,
            description: None,
            homepage: None,
        }
    }

    #[test]
    fn knowledge_provider_variant_has_empty_shape() {
        // RFC 029: the manifest-layer KnowledgeProvider variant carries no
        // data — all detail is negotiated at the initialize handshake.
        let cap = PluginCapability::KnowledgeProvider;
        let json = serde_json::to_value(&cap).unwrap();
        assert_eq!(json, serde_json::json!({"type": "knowledge_provider"}));
        let back: PluginCapability = serde_json::from_value(json).unwrap();
        assert_eq!(back, cap);
    }

    #[test]
    fn knowledge_provider_family_mapping() {
        use cairn_plugin_proto::CapabilityFamily;
        assert_eq!(
            PluginCapability::KnowledgeProvider.family(),
            Some(CapabilityFamily::KnowledgeProvider)
        );
    }

    #[test]
    fn validate_accepts_knowledge_provider_alone() {
        let m = manifest_with_capabilities(vec![PluginCapability::KnowledgeProvider]);
        assert!(m.validate().is_ok());
    }

    #[test]
    fn validate_accepts_signal_source_alone() {
        let m = manifest_with_capabilities(vec![PluginCapability::SignalSource {
            signals: vec!["x.y".to_owned()],
        }]);
        assert!(m.validate().is_ok());
    }

    #[test]
    fn validate_rejects_knowledge_provider_plus_signal_source() {
        let m = manifest_with_capabilities(vec![
            PluginCapability::KnowledgeProvider,
            PluginCapability::SignalSource {
                signals: vec!["x.y".to_owned()],
            },
        ]);
        let err = m.validate().unwrap_err();
        assert!(err
            .families
            .contains(&cairn_plugin_proto::CapabilityFamily::KnowledgeProvider));
        assert!(err
            .families
            .contains(&cairn_plugin_proto::CapabilityFamily::SignalSource));
    }

    #[test]
    fn validate_accepts_knowledge_provider_plus_tool_provider() {
        // Only knowledge_provider + signal_source is forbidden. Other combos
        // (knowledge + tool, knowledge + eval_scorer, etc.) are allowed.
        let m = manifest_with_capabilities(vec![
            PluginCapability::KnowledgeProvider,
            PluginCapability::ToolProvider {
                tools: vec!["foo.bar".to_owned()],
            },
        ]);
        assert!(m.validate().is_ok());
    }
}
