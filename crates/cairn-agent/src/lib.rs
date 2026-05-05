//! Agent runtime primitives: ReAct loop types, streaming shapes,
//! reflection advisories.
//!
//! This crate historically also hosted a higher-level agent-executor
//! plus subagent-spawning surface, but those code paths were never
//! wired to production. The real subagent spawning contract is
//! implemented in cairn-orchestrator (decide/execute phases) plus
//! the cairn-app fabric adapter's spawn_subagent path via
//! epic #670 (G3/G5/G6/G7). The dead execute/hook/subagent
//! scaffolding in this crate was removed in G8 (see RFC 027).
//!
//! What remains:
//!
//! - **React**: ReAct (Reason + Act) loop step types. Referenced by
//!   `cairn_orchestrator::context` via doc-comment only — the
//!   orchestrator keeps its own `LoopSignal` to avoid the
//!   cross-crate dependency, but this crate keeps the shapes as the
//!   authoritative contract definitions.
//! - **Orchestrator** (this crate's sub-module, NOT
//!   `cairn-orchestrator`): high-level `AgentConfig` / `AgentType` /
//!   `StepContext` types. Carried forward for cross-crate evaluator
//!   hooks that reference `AgentType`.
//! - **Reflection**: self-inspection advisories.
//! - **Streaming**: the SSE / streaming-output variant types used
//!   by `cairn-api::sse_payloads` to assemble the public `/stream`
//!   surface. Actively used — this is the crate's current
//!   load-bearing export.

pub mod orchestrator;
pub mod react;
pub mod reflection;
pub mod streaming;

pub use orchestrator::{AgentConfig, AgentType, ResolvedPrompt, StepContext, StepOutcome};
pub use react::{LoopSignal, ReactPhase};
pub use reflection::ReflectionAdvisory;
pub use streaming::{
    AssistantDelta, AssistantEnd, AssistantReasoning, StopReason, StreamingOutput,
};

#[cfg(test)]
mod tests {
    #[test]
    fn crate_compiles_with_domain_dependency() {
        let id = cairn_domain::SessionId::new("test");
        assert_eq!(id.as_str(), "test");
    }
}
