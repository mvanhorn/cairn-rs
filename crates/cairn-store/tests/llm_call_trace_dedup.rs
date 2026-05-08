//! #741 regression: every LLM call must produce exactly ONE
//! `LlmCallTrace` row, not two.
//!
//! Pre-fix, `crates/cairn-app/src/tracing_emitter.rs` appended a
//! `ProviderCallCompleted` event AND explicitly called
//! `store.insert_trace(...)`. The InMemory projection's
//! `apply_projection` arm for `ProviderCallCompleted` (in_memory.rs:2635 —
//! \"GAP-010: derive LlmCallTrace from every ProviderCallCompleted\")
//! already pushes a derived trace from the same event payload, so the
//! explicit insert was a duplicate write. Net: 1 model call → 2 trace
//! rows in `/v1/sessions/:id/llm-traces`.
//!
//! R13 dogfood (2026-05-08) reproduced this: 14 trace rows for 7
//! logical model calls. Filed as #741.
//!
//! This test pins the post-fix contract by exercising the projection
//! path directly: append exactly one `ProviderCallCompleted` event and
//! assert `LlmCallTraceReadModel::list_by_session` returns a single
//! row.

use cairn_domain::providers::{OperationKind, ProviderCallStatus};
use cairn_domain::{
    EventEnvelope, EventId, EventSource, ProjectId, ProjectKey, ProviderBindingId,
    ProviderCallCompleted, ProviderCallId, ProviderConnectionId, ProviderModelId, RouteAttemptId,
    RouteDecisionId, RunId, RuntimeEvent, SessionId, TenantId, WorkspaceId,
};
use cairn_store::{projections::LlmCallTraceReadModel, EventLog, InMemoryStore};

fn project() -> ProjectKey {
    ProjectKey {
        tenant_id: TenantId::new("t_741"),
        workspace_id: WorkspaceId::new("w_741"),
        project_id: ProjectId::new("p_741"),
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn provider_call_event(
    evt_id: &str,
    call_id: &str,
    session_id: &str,
    run_id: &str,
    ts: u64,
) -> EventEnvelope<RuntimeEvent> {
    EventEnvelope::for_runtime_event(
        EventId::new(evt_id),
        EventSource::Runtime,
        RuntimeEvent::ProviderCallCompleted(ProviderCallCompleted {
            project: project(),
            provider_call_id: ProviderCallId::new(call_id),
            route_decision_id: RouteDecisionId::new(format!("rd_{call_id}")),
            route_attempt_id: RouteAttemptId::new(format!("ra_{call_id}")),
            provider_binding_id: ProviderBindingId::new("binding_741"),
            provider_connection_id: ProviderConnectionId::new("conn_741"),
            provider_model_id: ProviderModelId::new("test-model"),
            operation_kind: OperationKind::Generate,
            status: ProviderCallStatus::Succeeded,
            latency_ms: Some(100),
            input_tokens: Some(50),
            output_tokens: Some(30),
            cost_micros: Some(200),
            completed_at: ts,
            session_id: Some(SessionId::new(session_id)),
            run_id: Some(RunId::new(run_id)),
            error_class: None,
            raw_error_message: None,
            retry_count: 0,
            task_id: None,
            prompt_release_id: None,
            fallback_position: 0,
            started_at: ts.saturating_sub(100),
            finished_at: ts,
        }),
    )
}

/// One `ProviderCallCompleted` event ⇒ exactly one `LlmCallTrace`
/// row. Pre-fix this would land 2 rows because both the projection
/// derivation AND the explicit `store.insert_trace` from
/// `tracing_emitter.rs` wrote the same trace.
#[tokio::test]
async fn one_provider_call_completed_yields_exactly_one_trace_row() {
    let store = InMemoryStore::new();
    let session_id = "sess_741_one";
    let run_id = "run_741_one";

    store
        .append(&[provider_call_event(
            "evt_741_1",
            "call_741_1",
            session_id,
            run_id,
            now_ms(),
        )])
        .await
        .expect("append");

    let traces = LlmCallTraceReadModel::list_by_session(&store, &SessionId::new(session_id), 100)
        .await
        .expect("list_by_session");

    assert_eq!(
        traces.len(),
        1,
        "exactly one LlmCallTrace row per ProviderCallCompleted event \
         (#741: pre-fix tracing_emitter wrote a second row explicitly)"
    );
    assert_eq!(traces[0].trace_id, "call_741_1");
    assert_eq!(traces[0].run_id.as_ref().map(|r| r.as_str()), Some(run_id));
    assert_eq!(traces[0].prompt_tokens, 50);
    assert_eq!(traces[0].completion_tokens, 30);
}

/// Multiple distinct calls land as distinct rows (pin the
/// non-coalescing-on-trace-id contract — the projection should
/// neither de-dup nor double-write).
#[tokio::test]
async fn multiple_provider_calls_yield_one_trace_per_call() {
    let store = InMemoryStore::new();
    let session_id = "sess_741_multi";

    let now = now_ms();
    store
        .append(&[
            provider_call_event("evt_741_a", "call_741_a", session_id, "run_a", now),
            provider_call_event("evt_741_b", "call_741_b", session_id, "run_b", now + 1),
            provider_call_event("evt_741_c", "call_741_c", session_id, "run_c", now + 2),
        ])
        .await
        .expect("append");

    let traces = LlmCallTraceReadModel::list_by_session(&store, &SessionId::new(session_id), 100)
        .await
        .expect("list_by_session");

    assert_eq!(
        traces.len(),
        3,
        "three calls ⇒ three rows; pre-fix would have produced six"
    );
    let mut ids: Vec<&str> = traces.iter().map(|t| t.trace_id.as_str()).collect();
    ids.sort();
    assert_eq!(ids, vec!["call_741_a", "call_741_b", "call_741_c"]);
}
