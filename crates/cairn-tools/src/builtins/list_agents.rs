//! `list_agents` built-in tool — orchestrator-only enumeration of
//! registered sub-agent roles.
//!
//! # Why it exists (#776)
//!
//! Pre-#776 the orchestrator's only knowledge of which roles exist
//! was the literal text in `spawn_subagent`'s schema description
//! (a hardcoded "Known roles: researcher, executor, reviewer,
//! generic" string). The orchestrator could not enumerate roles,
//! could not read a role's purpose without spawning it, and could
//! not check whether a candidate role is registered before
//! delegating. R19 dogfood symptom: the orchestrator picked
//! `executor` for goals that should have gone to `researcher`
//! because the role names looked similar enough at the prompt
//! layer; with a real description-introspection path the wrong
//! choice becomes visible.
//!
//! # What this returns
//!
//! `{ "agents": [{ role_id, display_name, description, tier,
//! response_shape }, ...] }` — one entry per role in
//! `default_roles()`. The orchestrator typically calls this once,
//! reads the descriptions, and picks the right `role` for
//! `spawn_subagent`. Read-only; no side effects.
//!
//! # Why orchestrator-only
//!
//! Sub-agents already know their role (it's their identity); they
//! don't spawn peers. Adding the tool to specialist allowlists
//! would just bloat their prompt-tool catalog with no actionable
//! use. Per the orchestrator-doctrine carve-outs (state-reads /
//! sub-agent verification / synthesis / planning / cross-output
//! decisions), `list_agents` fits **planning** — the orchestrator
//! is choosing how to decompose the goal across sub-agents.

use async_trait::async_trait;
use cairn_domain::{policy::ExecutionClass, ProjectKey};
use serde_json::Value;

use super::{ToolEffect, ToolError, ToolHandler, ToolResult, ToolTier};
use cairn_domain::recovery::RetrySafety;

/// Enumerate registered sub-agent roles. See module docs.
#[derive(Default)]
pub struct ListAgentsTool;

impl ListAgentsTool {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl ToolHandler for ListAgentsTool {
    fn name(&self) -> &str {
        "list_agents"
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
        "Enumerate registered sub-agent roles available to spawn_subagent. \
         Returns each role's id, display name, short description, tier, and \
         expected response shape. Call this when planning a delegation and \
         you are not sure which role best fits the goal."
    }
    fn parameters_schema(&self) -> Value {
        // No parameters — the registry is global and the
        // orchestrator just wants the full list.
        serde_json::json!({
            "type": "object",
            "properties": {},
            "additionalProperties": false
        })
    }
    fn execution_class(&self) -> ExecutionClass {
        // Read-only lookup of in-memory registry data; no external
        // execution. Per Gemini review on PR #784, SandboxedProcess
        // is reserved for tools that execute external code.
        ExecutionClass::SupervisedProcess
    }

    async fn execute(&self, _project: &ProjectKey, _args: Value) -> Result<ToolResult, ToolError> {
        let roles = cairn_domain::agent_roles::default_roles();
        // Filter the orchestrator role out of the result. The
        // orchestrator is the parent — it does not delegate to
        // itself. Listing it would invite a self-spawn loop.
        //
        // `tier` and `response_shape` are inlined into the `json!`
        // macro directly: their `Serialize` derives already rename
        // to snake_case, so no manual `to_value` is needed (Gemini
        // review on PR #784).
        let agents: Vec<Value> = roles
            .iter()
            .filter(|r| r.role_id != "orchestrator")
            .map(|r| {
                serde_json::json!({
                    "role_id":        r.role_id,
                    "display_name":   r.display_name,
                    "description":    r.description,
                    "tier":           r.tier,
                    "response_shape": r.response_shape,
                })
            })
            .collect();
        Ok(ToolResult::ok(serde_json::json!({
            "agents": agents,
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
    async fn list_agents_returns_default_roles_minus_orchestrator() {
        let tool = ListAgentsTool::new();
        let result = tool
            .execute(&project(), serde_json::json!({}))
            .await
            .expect("execute");
        let agents = result
            .output
            .get("agents")
            .and_then(|v| v.as_array())
            .expect("agents array");
        // #775 added `generic` (5 defaults). #806 added `status-checker`
        // (6 defaults). Orchestrator is always filtered out.
        assert_eq!(
            agents.len(),
            5,
            "expected 5 agents (6 default - orchestrator)"
        );

        let ids: Vec<&str> = agents
            .iter()
            .filter_map(|a| a.get("role_id").and_then(|v| v.as_str()))
            .collect();
        for expected in [
            "status-checker",
            "executor",
            "researcher",
            "reviewer",
            "generic",
        ] {
            assert!(ids.contains(&expected), "missing {expected}");
        }
        assert!(
            !ids.contains(&"orchestrator"),
            "orchestrator must be filtered out — it does not delegate to itself"
        );
    }

    #[tokio::test]
    async fn list_agents_each_entry_carries_description() {
        // #776 contract: every returned agent has a non-empty
        // description so the LLM can choose without spawning. If a
        // future role lands without a description, this test fails
        // loud rather than the orchestrator silently picking by
        // role_id heuristic.
        let tool = ListAgentsTool::new();
        let result = tool
            .execute(&project(), serde_json::json!({}))
            .await
            .expect("execute");
        let agents = result
            .output
            .get("agents")
            .and_then(|v| v.as_array())
            .unwrap();
        for agent in agents {
            let desc = agent
                .get("description")
                .and_then(|v| v.as_str())
                .expect("description present");
            assert!(
                !desc.is_empty(),
                "agent {agent} must have non-empty description"
            );
        }
    }

    #[tokio::test]
    async fn list_agents_each_entry_includes_response_shape() {
        let tool = ListAgentsTool::new();
        let result = tool
            .execute(&project(), serde_json::json!({}))
            .await
            .unwrap();
        let agents = result
            .output
            .get("agents")
            .and_then(|v| v.as_array())
            .unwrap();
        for agent in agents {
            let shape = agent
                .get("response_shape")
                .and_then(|v| v.as_str())
                .expect("response_shape present");
            assert!(
                matches!(shape, "direct_answer" | "procedural_artifact"),
                "unexpected response_shape value: {shape}"
            );
        }
    }
}
