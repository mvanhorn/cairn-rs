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
}
