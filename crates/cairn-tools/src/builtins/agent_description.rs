//! `agent_description` built-in tool — orchestrator-only deep-read of
//! a registered sub-agent role.
//!
//! # Why it exists (#776)
//!
//! `list_agents` returns the short-form summary (display_name +
//! one-line description) for every role; that's enough for
//! routine routing decisions. When the orchestrator needs more
//! detail before delegating — the role's full tool allowlist, its
//! response shape, the specialty overlay text — `agent_description`
//! returns the full role record (minus the shared
//! `BASE_SUBAGENT_PROMPT`, which is not role-specific and not
//! useful for routing decisions).
//!
//! # What it returns
//!
//! `{ role_id, display_name, description, specialty_prompt, tools,
//!   forbid_all_tools, max_context_tokens, tier, response_shape }`.
//! `specialty_prompt` is the role's `system_prompt` field —
//! specialty overlay only for sub-agent roles, full prompt for
//! orchestrator. Read-only.
//!
//! The `tools` key matches the domain field name (RFC 031 §D10
//! renamed `allowed_tools` → `tools` on the struct; the LLM-visible
//! JSON key renames in lockstep so the wire contract matches the
//! struct contract).
//!
//! # Failure modes
//!
//! Unknown role_id returns a `Permanent` ToolError (not a silent
//! fallback) so the orchestrator gets a loud "no such role"
//! signal instead of the wrong description.

use async_trait::async_trait;
use cairn_domain::{policy::ExecutionClass, ProjectKey};
use serde_json::Value;

use super::{ToolEffect, ToolError, ToolHandler, ToolResult, ToolTier};
use cairn_domain::recovery::RetrySafety;

/// Look up a single role's full record.
#[derive(Default)]
pub struct AgentDescriptionTool;

impl AgentDescriptionTool {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl ToolHandler for AgentDescriptionTool {
    fn name(&self) -> &str {
        "agent_description"
    }
    fn tier(&self) -> ToolTier {
        ToolTier::Registered
    }
    fn tool_effect(&self) -> ToolEffect {
        ToolEffect::Observational
    }
    fn retry_safety(&self) -> RetrySafety {
        RetrySafety::IdempotentSafe
    }
    fn description(&self) -> &str {
        "Read the full record for one registered sub-agent role: \
         display name, description, specialty overlay prompt, allowed \
         tools, context-token cap, tier, and response shape. Call this \
         after `list_agents` when you need more detail than the short \
         summary provides — typically to confirm a role's tool set \
         covers the goal before spawning."
    }
    fn parameters_schema(&self) -> Value {
        serde_json::json!({
            "type": "object",
            "required": ["role_id"],
            "properties": {
                "role_id": {
                    "type": "string",
                    "description": "The role to look up. Use `list_agents` \
                                    first if you don't know the role_id."
                }
            },
            "additionalProperties": false
        })
    }
    fn execution_class(&self) -> ExecutionClass {
        // Read-only lookup of in-memory registry data; no external
        // execution. Per Gemini review on PR #784.
        ExecutionClass::SupervisedProcess
    }

    async fn execute(&self, _project: &ProjectKey, args: Value) -> Result<ToolResult, ToolError> {
        let role_id = args
            .get("role_id")
            .and_then(|v| v.as_str())
            .ok_or_else(|| ToolError::InvalidArgs {
                field: "role_id".into(),
                message: "required string".into(),
            })?;

        let roles = cairn_domain::agent_roles::default_roles();
        let role = roles.iter().find(|r| r.role_id == role_id).ok_or_else(|| {
            ToolError::Permanent(format!(
                "no registered role with role_id={role_id:?}; \
                 call `list_agents` to see available roles"
            ))
        })?;

        // `tier` and `response_shape` are inlined into the `json!`
        // macro directly: their `Serialize` derives rename to
        // snake_case so no manual `to_value` is needed (Gemini
        // review on PR #784).
        // RFC 031 §D10: `tools` is the field name; `allowed_tools`
        // is accepted as a serde alias on deserialise for pre-rename
        // event replay, but emitted events and LLM-facing tools use
        // the new key.
        Ok(ToolResult::ok(serde_json::json!({
            "role_id":            role.role_id,
            "display_name":       role.display_name,
            "description":        role.description,
            "specialty_prompt":   role.system_prompt,
            "tools":              role.tools,
            "forbid_all_tools":   role.forbid_all_tools,
            "max_context_tokens": role.max_context_tokens,
            "tier":               role.tier,
            "response_shape":     role.response_shape,
        })))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_domain::ProjectKey;

    fn project() -> ProjectKey {
        ProjectKey::new("t", "w", "p")
    }

    #[tokio::test]
    async fn agent_description_returns_full_record_for_executor() {
        let tool = AgentDescriptionTool::new();
        let result = tool
            .execute(&project(), serde_json::json!({"role_id": "executor"}))
            .await
            .expect("execute");
        let out = &result.output;
        assert_eq!(
            out.get("role_id").and_then(|v| v.as_str()),
            Some("executor")
        );
        assert!(
            out.get("description")
                .and_then(|v| v.as_str())
                .map(|s| !s.is_empty())
                .unwrap_or(false),
            "executor description must be non-empty"
        );
        let tools = out
            .get("tools")
            .and_then(|v| v.as_array())
            .expect("tools array");
        // Guard against accidental regression to the legacy key.
        assert!(
            out.get("allowed_tools").is_none(),
            "LLM-visible JSON must use the RFC 031 `tools` key, not legacy `allowed_tools`"
        );
        // Executor must have write tools (its specialty).
        let names: Vec<&str> = tools.iter().filter_map(|v| v.as_str()).collect();
        assert!(names.contains(&"write"), "executor must include `write`");
        assert!(names.contains(&"bash"), "executor must include `bash`");
        // ProceduralArtifact response shape.
        assert_eq!(
            out.get("response_shape").and_then(|v| v.as_str()),
            Some("procedural_artifact"),
        );
    }

    #[tokio::test]
    async fn agent_description_works_for_generic() {
        // #775 introduced the generic role; this contract pins
        // that agent_description handles it.
        let tool = AgentDescriptionTool::new();
        let result = tool
            .execute(&project(), serde_json::json!({"role_id": "generic"}))
            .await
            .expect("execute");
        assert_eq!(
            result.output.get("role_id").and_then(|v| v.as_str()),
            Some("generic")
        );
        assert_eq!(
            result.output.get("tier").and_then(|v| v.as_str()),
            Some("generic")
        );
    }

    #[tokio::test]
    async fn agent_description_unknown_role_returns_permanent_error() {
        // #776 contract: unknown role_id → Permanent (not silent
        // fallback). The orchestrator must learn it picked a bad
        // name, not silently get back a description for some other
        // role.
        let tool = AgentDescriptionTool::new();
        let err = tool
            .execute(
                &project(),
                serde_json::json!({"role_id": "not-a-real-role-zzz"}),
            )
            .await
            .expect_err("unknown role must error");
        match err {
            ToolError::Permanent(msg) => {
                assert!(
                    msg.contains("not-a-real-role-zzz"),
                    "error message must name the missing role: {msg}"
                );
                assert!(
                    msg.contains("list_agents"),
                    "error message must point at list_agents for recovery: {msg}"
                );
            }
            other => panic!("expected Permanent, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn agent_description_missing_role_id_field_invalid_args() {
        let tool = AgentDescriptionTool::new();
        let err = tool
            .execute(&project(), serde_json::json!({}))
            .await
            .expect_err("missing role_id must error");
        match err {
            ToolError::InvalidArgs { field, .. } => {
                assert_eq!(field, "role_id");
            }
            other => panic!("expected InvalidArgs, got {other:?}"),
        }
    }
}
