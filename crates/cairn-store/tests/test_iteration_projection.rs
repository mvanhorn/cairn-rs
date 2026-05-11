use cairn_domain::{events::*, *};
use cairn_store::*;

#[tokio::test]
async fn iteration_increments_on_resume_transition() {
    let store = std::sync::Arc::new(in_memory::InMemoryStore::new());
    let project = ProjectKey::new("t1", "w1", "p1");
    let session = SessionId::new("s1");
    let run_id = RunId::new("r1");

    // Create run.
    let create = EventEnvelope::for_runtime_event(
        EventId::new("evt1"),
        EventSource::Runtime,
        RuntimeEvent::RunCreated(RunCreated {
            project: project.clone(),
            session_id: session.clone(),
            run_id: run_id.clone(),
            parent_run_id: None,
            prompt_release_id: None,
            agent_role_id: None,
        }),
    );
    store.append(&[create]).await.unwrap();

    let r = projections::RunReadModel::get(&*store, &run_id)
        .await
        .unwrap()
        .unwrap();
    println!("after create: iteration={}", r.iteration);
    assert_eq!(r.iteration, 0);

    // Transition pending → running.
    let trans1 = EventEnvelope::for_runtime_event(
        EventId::new("evt2"),
        EventSource::Runtime,
        RuntimeEvent::RunStateChanged(RunStateChanged {
            project: project.clone(),
            run_id: run_id.clone(),
            transition: StateTransition {
                from: Some(RunState::Pending),
                to: RunState::Running,
            },
            failure_class: None,
            pause_reason: None,
            resume_trigger: None,
        }),
    );
    store.append(&[trans1]).await.unwrap();

    let r = projections::RunReadModel::get(&*store, &run_id)
        .await
        .unwrap()
        .unwrap();
    println!("after pending→running: iteration={}", r.iteration);
    assert_eq!(r.iteration, 0);

    // Transition running → waiting_approval.
    let trans2 = EventEnvelope::for_runtime_event(
        EventId::new("evt3"),
        EventSource::Runtime,
        RuntimeEvent::RunStateChanged(RunStateChanged {
            project: project.clone(),
            run_id: run_id.clone(),
            transition: StateTransition {
                from: Some(RunState::Running),
                to: RunState::WaitingApproval,
            },
            failure_class: None,
            pause_reason: None,
            resume_trigger: None,
        }),
    );
    store.append(&[trans2]).await.unwrap();

    // Transition waiting_approval → running. SHOULD INCREMENT.
    let trans3 = EventEnvelope::for_runtime_event(
        EventId::new("evt4"),
        EventSource::Runtime,
        RuntimeEvent::RunStateChanged(RunStateChanged {
            project: project.clone(),
            run_id: run_id.clone(),
            transition: StateTransition {
                from: Some(RunState::WaitingApproval),
                to: RunState::Running,
            },
            failure_class: None,
            pause_reason: None,
            resume_trigger: None,
        }),
    );
    store.append(&[trans3]).await.unwrap();

    let r = projections::RunReadModel::get(&*store, &run_id)
        .await
        .unwrap()
        .unwrap();
    println!("after waiting_approval→running: iteration={}", r.iteration);
    assert_eq!(
        r.iteration, 1,
        "iteration should be 1 after one approval-resume cycle"
    );

    // #795: waiting_dependency → running is also a resume boundary.
    // Transition running → waiting_dependency, then waiting_dependency
    // → running (parent-resume-after-subagent-completion shape).
    let trans4 = EventEnvelope::for_runtime_event(
        EventId::new("evt5"),
        EventSource::Runtime,
        RuntimeEvent::RunStateChanged(RunStateChanged {
            project: project.clone(),
            run_id: run_id.clone(),
            transition: StateTransition {
                from: Some(RunState::Running),
                to: RunState::WaitingDependency,
            },
            failure_class: None,
            pause_reason: None,
            resume_trigger: None,
        }),
    );
    store.append(&[trans4]).await.unwrap();
    let trans5 = EventEnvelope::for_runtime_event(
        EventId::new("evt6"),
        EventSource::Runtime,
        RuntimeEvent::RunStateChanged(RunStateChanged {
            project: project.clone(),
            run_id: run_id.clone(),
            transition: StateTransition {
                from: Some(RunState::WaitingDependency),
                to: RunState::Running,
            },
            failure_class: None,
            pause_reason: None,
            resume_trigger: None,
        }),
    );
    store.append(&[trans5]).await.unwrap();
    let r = projections::RunReadModel::get(&*store, &run_id)
        .await
        .unwrap()
        .unwrap();
    println!(
        "after waiting_dependency→running: iteration={}",
        r.iteration
    );
    assert_eq!(
        r.iteration, 2,
        "#795: iteration should be 2 after one approval-resume + one \
         waiting_dependency-resume cycle"
    );

    // #795: paused → running is also a resume boundary (operator-paced).
    let trans6 = EventEnvelope::for_runtime_event(
        EventId::new("evt7"),
        EventSource::Runtime,
        RuntimeEvent::RunStateChanged(RunStateChanged {
            project: project.clone(),
            run_id: run_id.clone(),
            transition: StateTransition {
                from: Some(RunState::Running),
                to: RunState::Paused,
            },
            failure_class: None,
            pause_reason: None,
            resume_trigger: None,
        }),
    );
    store.append(&[trans6]).await.unwrap();
    let trans7 = EventEnvelope::for_runtime_event(
        EventId::new("evt8"),
        EventSource::Runtime,
        RuntimeEvent::RunStateChanged(RunStateChanged {
            project: project.clone(),
            run_id: run_id.clone(),
            transition: StateTransition {
                from: Some(RunState::Paused),
                to: RunState::Running,
            },
            failure_class: None,
            pause_reason: None,
            resume_trigger: None,
        }),
    );
    store.append(&[trans7]).await.unwrap();
    let r = projections::RunReadModel::get(&*store, &run_id)
        .await
        .unwrap()
        .unwrap();
    println!("after paused→running: iteration={}", r.iteration);
    assert_eq!(
        r.iteration, 3,
        "#795: iteration should be 3 after approval-resume + \
         waiting_dependency-resume + paused-resume cycles"
    );
}

#[tokio::test]
async fn reasoning_step_apply_dedups_on_iteration_value() {
    // #796: a duplicate RunReasoningStepRecorded for the same
    // (run_id, iteration) should REPLACE the existing entry rather
    // than appending. The semantic invariant is "at most one
    // reasoning step per iteration" — R21 dogfood saw double-emits
    // (audit trail had two consecutive WaitingApproval transitions)
    // that the projection naively appended, leaving operators
    // staring at duplicate iter=N rows in the trajectory replay.
    use cairn_domain::events::{ProposedActionSummary, RunReasoningStep};
    let store = std::sync::Arc::new(in_memory::InMemoryStore::new());
    let project = ProjectKey::new("t1", "w1", "p1");
    let session = SessionId::new("s1");
    let run_id = RunId::new("r1");
    store
        .append(&[EventEnvelope::for_runtime_event(
            EventId::new("evt_create"),
            EventSource::Runtime,
            RuntimeEvent::RunCreated(RunCreated {
                project: project.clone(),
                session_id: session.clone(),
                run_id: run_id.clone(),
                parent_run_id: None,
                prompt_release_id: None,
                agent_role_id: None,
            }),
        )])
        .await
        .unwrap();

    // First reasoning step at iter=2.
    let event1 = EventEnvelope::for_runtime_event(
        EventId::new("evt_reasoning_first"),
        EventSource::Runtime,
        RuntimeEvent::RunReasoningStepRecorded(RunReasoningStep {
            project: project.clone(),
            run_id: run_id.clone(),
            session_id: session.clone(),
            iteration: 2,
            recorded_at_ms: 1000,
            model_id: "test".to_owned(),
            reasoning_compact: "first emit".to_owned(),
            proposed_action: ProposedActionSummary::Other {
                action_type: "tool_call".to_owned(),
            },
            proposal_count: 1,
            step_history_snapshot: "snap1".to_owned(),
            confidence: 0.5,
        }),
    );
    store.append(&[event1]).await.unwrap();

    // Second reasoning step at SAME iter=2 (#796 double-emit shape).
    let event2 = EventEnvelope::for_runtime_event(
        EventId::new("evt_reasoning_second"),
        EventSource::Runtime,
        RuntimeEvent::RunReasoningStepRecorded(RunReasoningStep {
            project: project.clone(),
            run_id: run_id.clone(),
            session_id: session.clone(),
            iteration: 2,
            recorded_at_ms: 2000,
            model_id: "test".to_owned(),
            reasoning_compact: "second emit overrides".to_owned(),
            proposed_action: ProposedActionSummary::Other {
                action_type: "tool_call".to_owned(),
            },
            proposal_count: 1,
            step_history_snapshot: "snap2".to_owned(),
            confidence: 0.7,
        }),
    );
    store.append(&[event2]).await.unwrap();

    use projections::reasoning_step::ReasoningStepReadModel;
    let count = ReasoningStepReadModel::count_for_run(&*store, &run_id)
        .await
        .unwrap();
    assert_eq!(
        count, 1,
        "#796: two emits at same (run_id, iteration) must collapse to one entry; got {count}"
    );
    let items = ReasoningStepReadModel::list_by_run(&*store, &run_id, 100, 0)
        .await
        .unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(
        items[0].reasoning_compact, "second emit overrides",
        "#796: last-write-wins on dedup; got {:?}",
        items[0].reasoning_compact
    );
    assert_eq!(items[0].confidence, 0.7);
    assert_eq!(items[0].recorded_at_ms, 2000);
}
