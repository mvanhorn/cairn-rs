//! RFC 031 PR-A: wire-shape + append tests for the three new
//! operator-defined agent-role events.
//!
//! Pins the event shape so downstream implementers (PR-B HTTP, PR-C
//! orchestrator pivot) build against a stable contract. PR-A itself
//! doesn't emit these events in production code — the HTTP handler
//! emits `AgentRoleDefined` / `AgentRoleRetracted` in PR-B, and
//! `ToolDeclaredButMissing` at the allowlist-filter site in PR-C.

use cairn_domain::agent_roles::{AgentRole, AgentRoleTier, ResponseShape};
use cairn_domain::{
    AgentRoleDefined, AgentRoleRetracted, EventEnvelope, EventId, EventSource, OperatorId,
    ProjectId, ProjectKey, RunId, RuntimeEvent, TenantId, ToolDeclaredButMissing, WorkspaceId,
};
use cairn_store::{EventLog, InMemoryStore};

fn project(tenant: &str) -> ProjectKey {
    ProjectKey {
        tenant_id: TenantId::new(tenant),
        workspace_id: WorkspaceId::new("w"),
        project_id: ProjectId::new(format!("p_{tenant}")),
    }
}

fn evt(id: &str, payload: RuntimeEvent) -> EventEnvelope<RuntimeEvent> {
    EventEnvelope::for_runtime_event(EventId::new(id), EventSource::Runtime, payload)
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn sample_role() -> AgentRole {
    AgentRole::new(
        "pr-reviewer-valkey",
        "Valkey PR Reviewer",
        AgentRoleTier::Standard,
    )
    .with_description("Reviews pull requests on valkey-io/valkey.")
    .with_tools(vec!["post_inline_comment", "post_summary_comment"])
    .with_response_shape(ResponseShape::ProceduralArtifact)
}

#[tokio::test]
async fn agent_role_defined_appends_without_error() {
    let store = InMemoryStore::new();
    let proj = project("t1");
    let role = sample_role();

    let event = RuntimeEvent::AgentRoleDefined(AgentRoleDefined {
        project: proj.clone(),
        role,
        shadows_builtin: None,
        defined_by: OperatorId::new("op-alice"),
        at_ms: now_ms(),
    });

    // The PR-A InMemory projection treats the event as a no-op (see
    // in_memory.rs RFC 031 arm). Append must not fail; projection
    // writers land in follow-up PRs.
    store.append(&[evt("e1", event)]).await.expect("append");
}

#[tokio::test]
async fn agent_role_defined_round_trips_through_serde() {
    // Event-log persistence on pg / sqlite goes through
    // `serde_json::to_value` → JSONB → back. Pin the wire shape so
    // replay on a future upgrade stays stable.
    let role = sample_role();
    let role_id = role.role_id.clone();
    let event = RuntimeEvent::AgentRoleDefined(AgentRoleDefined {
        project: project("t1"),
        role,
        shadows_builtin: Some("reviewer".to_owned()),
        defined_by: OperatorId::new("op-a"),
        at_ms: 12345,
    });

    let json = serde_json::to_value(&event).expect("serialize");
    let back: RuntimeEvent = serde_json::from_value(json).expect("deserialize");
    match back {
        RuntimeEvent::AgentRoleDefined(e) => {
            assert_eq!(e.role.role_id, role_id);
            assert_eq!(e.shadows_builtin.as_deref(), Some("reviewer"));
            assert_eq!(e.defined_by, OperatorId::new("op-a"));
            assert_eq!(e.at_ms, 12345);
        }
        other => panic!("unexpected variant after round-trip: {other:?}"),
    }
}

#[tokio::test]
async fn agent_role_retracted_round_trips_through_serde() {
    let event = RuntimeEvent::AgentRoleRetracted(AgentRoleRetracted {
        project: project("t1"),
        role_id: "pr-reviewer-valkey".to_owned(),
        retracted_by: OperatorId::new("op-b"),
        at_ms: 67890,
    });
    let json = serde_json::to_value(&event).unwrap();
    let back: RuntimeEvent = serde_json::from_value(json).unwrap();
    match back {
        RuntimeEvent::AgentRoleRetracted(e) => {
            assert_eq!(e.role_id, "pr-reviewer-valkey");
            assert_eq!(e.retracted_by, OperatorId::new("op-b"));
            assert_eq!(e.at_ms, 67890);
        }
        other => panic!("unexpected variant: {other:?}"),
    }
}

#[tokio::test]
async fn tool_declared_but_missing_round_trips_through_serde() {
    let event = RuntimeEvent::ToolDeclaredButMissing(ToolDeclaredButMissing {
        project: project("t1"),
        run_id: RunId::new("run_1"),
        role_id: "pr-reviewer-valkey".to_owned(),
        tool_id: "post_inline_commment".to_owned(),
        at_ms: 99_999,
    });
    let json = serde_json::to_value(&event).unwrap();
    let back: RuntimeEvent = serde_json::from_value(json).unwrap();
    match back {
        RuntimeEvent::ToolDeclaredButMissing(e) => {
            assert_eq!(e.run_id, RunId::new("run_1"));
            assert_eq!(e.role_id, "pr-reviewer-valkey");
            assert_eq!(e.tool_id, "post_inline_commment");
        }
        other => panic!("unexpected variant: {other:?}"),
    }
}

#[tokio::test]
async fn role_carries_rfc031_forbid_all_tools_field_through_replay() {
    // §D3 introduced `AgentRole.forbid_all_tools: bool`. Events
    // persisted pre-RFC won't have the field; the serde default must
    // fall through to `false` on replay.
    // RuntimeEvent is `#[serde(tag = "event", rename_all = "snake_case")]`
    // so the wire carries `"event": "agent_role_defined"` at the top level
    // with the payload fields inlined alongside.
    let legacy_json = serde_json::json!({
        "event": "agent_role_defined",
        "project": {
            "tenant_id": "t",
            "workspace_id": "w",
            "project_id": "p"
        },
        "role": {
            "role_id": "legacy",
            "display_name": "Legacy",
            "description": "",
            "system_prompt": null,
            "allowed_tools": ["read"],
            "max_context_tokens": null,
            "tier": "standard",
            "response_shape": "procedural_artifact"
        },
        "shadows_builtin": null,
        "defined_by": "op-a",
        "at_ms": 0
    });
    let back: RuntimeEvent = serde_json::from_value(legacy_json).expect("legacy replay");
    match back {
        RuntimeEvent::AgentRoleDefined(e) => {
            // Alias accepted; renamed field populated.
            assert_eq!(e.role.tools, vec!["read".to_owned()]);
            // New field defaults to false.
            assert!(!e.role.forbid_all_tools);
        }
        other => panic!("unexpected variant: {other:?}"),
    }
}

#[tokio::test]
async fn agent_role_events_are_project_scoped() {
    // Every RFC 031 event carries a ProjectKey — verify the
    // `RuntimeEvent::project()` accessor handles all three.
    let proj = project("t_scope");

    let defined = RuntimeEvent::AgentRoleDefined(AgentRoleDefined {
        project: proj.clone(),
        role: sample_role(),
        shadows_builtin: None,
        defined_by: OperatorId::new("op-a"),
        at_ms: 0,
    });
    assert_eq!(defined.project(), &proj);

    let retracted = RuntimeEvent::AgentRoleRetracted(AgentRoleRetracted {
        project: proj.clone(),
        role_id: "x".to_owned(),
        retracted_by: OperatorId::new("op-b"),
        at_ms: 0,
    });
    assert_eq!(retracted.project(), &proj);

    let advisory = RuntimeEvent::ToolDeclaredButMissing(ToolDeclaredButMissing {
        project: proj.clone(),
        run_id: RunId::new("r"),
        role_id: "x".to_owned(),
        tool_id: "y".to_owned(),
        at_ms: 0,
    });
    assert_eq!(advisory.project(), &proj);
}
