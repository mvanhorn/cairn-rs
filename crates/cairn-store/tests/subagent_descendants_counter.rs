//! #670 G4 PR-1b-1: `RunDescendantsCounter` primitive tests.
//!
//! Exercises the atomic compare-and-increment primitive specified in
//! RFC 027 §`in_flight_descendants` counter. Covers:
//!
//! 1. Fresh `RunCreated` on a root initialises `root_run_id =
//!    run_id` and `in_flight_descendants = 0`.
//! 2. A non-root `RunCreated` initialises `root_run_id = None` —
//!    the subagent spawn path (G4 PR-1b-3) will set it via a
//!    separate projection shape change (the spawn emits a
//!    `RunCreated` event carrying the resolved root id, and the
//!    `RunCreated` projection writes it atomically); the counter
//!    primitive itself only mutates the ROOT row, never the child.
//! 3. `try_increment_descendants` admits below the cap, returning
//!    the post-increment count.
//! 4. `try_increment_descendants` rejects at the cap with
//!    `CapReached`.
//! 5. `try_increment_descendants` returns `RootNotFound` for an
//!    unknown run id (bug-surface, not business logic).
//! 6. `decrement_descendants` subtracts one, returning the post-
//!    decrement count.
//! 7. Underflow (decrement past zero) returns a negative count —
//!    this is the auditable signal the adapter layer WARN-logs on
//!    per RFC 027, not a panic.
//! 8. Concurrent increments against the same root with `cap=3`
//!    admit at most three even with many concurrent callers.
//!
//! **Coverage scope**: `InMemoryStore` only. The pg + sqlite impls
//! ship the same contract (identical atomic-SQL shape `UPDATE ...
//! WHERE counter < :cap RETURNING` on both, guaranteed atomic by
//! sqlite 3.35+ and pg natively), but are **not** exercised by any
//! test in PR-1b-1 — the existing `projection_parity.rs` harness
//! asserts event-log byte-parity, not counter primitives. Live-DB
//! coverage lands in PR-1b-3 alongside the spawn path that actually
//! calls this primitive in anger. Until then, the durable backends
//! ride on: (a) the identical Rust contract asserted here, (b) the
//! SQL atomicity guarantees of the underlying engines, and (c) the
//! fact that the primitive is unreachable in production code until
//! PR-1b-3 wires it in.

use std::sync::Arc;

use cairn_domain::{
    EventEnvelope, EventId, EventSource, ProjectKey, RunCreated, RunId, RuntimeEvent,
    SessionCreated, SessionId,
};
use cairn_store::{
    projections::{DescendantsCapOutcome, RunDescendantsCounter, RunReadModel},
    EventLog, InMemoryStore,
};

fn project() -> ProjectKey {
    ProjectKey::new("tenant_desc", "ws_desc", "proj_desc")
}

/// Monotonic event-id counter. Using a counter (rather than uuid) keeps
/// the test's crate graph narrow — cairn-store tests don't need a uuid
/// dep just to mint event ids.
static EVENT_ID_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

fn envelope(event: RuntimeEvent) -> EventEnvelope<RuntimeEvent> {
    let n = EVENT_ID_COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    EventEnvelope::for_runtime_event(
        EventId::new(format!("evt_desc_counter_{n}")),
        EventSource::Runtime,
        event,
    )
}

async fn seed_session(store: &InMemoryStore, session_id: &SessionId) {
    store
        .append(&[envelope(RuntimeEvent::SessionCreated(SessionCreated {
            project: project(),
            session_id: session_id.clone(),
        }))])
        .await
        .unwrap();
}

async fn seed_root_run(store: &InMemoryStore, session_id: &SessionId, run_id: &RunId) {
    store
        .append(&[envelope(RuntimeEvent::RunCreated(RunCreated {
            project: project(),
            run_id: run_id.clone(),
            session_id: session_id.clone(),
            parent_run_id: None,
            prompt_release_id: None,
            agent_role_id: None,
        }))])
        .await
        .unwrap();
}

#[tokio::test]
async fn run_created_initialises_root_run_id_on_roots_and_zero_counter() {
    let store = InMemoryStore::new();
    let session = SessionId::new("sess_init");
    let root = RunId::new("run_root_init");
    seed_session(&store, &session).await;
    seed_root_run(&store, &session, &root).await;

    let record = RunReadModel::get(&store, &root).await.unwrap().unwrap();
    assert_eq!(record.in_flight_descendants, 0);
    assert_eq!(record.root_run_id.as_ref(), Some(&root));
}

#[tokio::test]
async fn run_created_leaves_root_run_id_none_on_non_roots() {
    let store = InMemoryStore::new();
    let session = SessionId::new("sess_child_none");
    let parent = RunId::new("run_parent_none");
    let child = RunId::new("run_child_none");
    seed_session(&store, &session).await;
    seed_root_run(&store, &session, &parent).await;

    // Non-root RunCreated — G3 path creates children this way.
    store
        .append(&[envelope(RuntimeEvent::RunCreated(RunCreated {
            project: project(),
            run_id: child.clone(),
            session_id: session.clone(),
            parent_run_id: Some(parent.clone()),
            prompt_release_id: None,
            agent_role_id: None,
        }))])
        .await
        .unwrap();

    let record = RunReadModel::get(&store, &child).await.unwrap().unwrap();
    assert_eq!(record.in_flight_descendants, 0);
    assert_eq!(
        record.root_run_id, None,
        "non-root RunCreated must leave root_run_id NULL in PR-1b-1 — \
         PR-1b-3 will ship the projection shape change that resolves \
         the root at spawn time and writes it here in the same \
         transaction as the RunCreated event"
    );
}

#[tokio::test]
async fn try_increment_admits_below_cap() {
    let store = InMemoryStore::new();
    let session = SessionId::new("sess_admit");
    let root = RunId::new("run_root_admit");
    seed_session(&store, &session).await;
    seed_root_run(&store, &session, &root).await;

    let out = store.try_increment_descendants(&root, 3).await.unwrap();
    assert_eq!(out, DescendantsCapOutcome::Admitted { new_count: 1 });
    let out = store.try_increment_descendants(&root, 3).await.unwrap();
    assert_eq!(out, DescendantsCapOutcome::Admitted { new_count: 2 });
    let out = store.try_increment_descendants(&root, 3).await.unwrap();
    assert_eq!(out, DescendantsCapOutcome::Admitted { new_count: 3 });
}

/// Copilot review on #676: the counter primitive must also bump
/// `version` + `updated_at` on every mutation, or stale-run
/// detection (and every other version-watching consumer) would
/// miss descendant-counter changes and see roots with active
/// descendants as idle.
#[tokio::test]
async fn try_increment_bumps_version_and_updated_at() {
    let store = InMemoryStore::new();
    let session = SessionId::new("sess_ver_inc");
    let root = RunId::new("run_ver_inc");
    seed_session(&store, &session).await;
    seed_root_run(&store, &session, &root).await;

    let before = RunReadModel::get(&store, &root).await.unwrap().unwrap();
    // Sleep briefly so updated_at can observably advance.
    // std::thread::sleep rather than tokio::time::sleep because
    // cairn-store's tokio dev-dep is built without the `time` feature.
    // Blocking ~5ms inside a #[tokio::test] is fine here.
    std::thread::sleep(std::time::Duration::from_millis(5));

    store.try_increment_descendants(&root, 3).await.unwrap();

    let after = RunReadModel::get(&store, &root).await.unwrap().unwrap();
    assert!(
        after.version > before.version,
        "try_increment_descendants must bump version (before={}, after={})",
        before.version,
        after.version,
    );
    assert!(
        after.updated_at > before.updated_at,
        "try_increment_descendants must bump updated_at (before={}, after={})",
        before.updated_at,
        after.updated_at,
    );
}

#[tokio::test]
async fn decrement_bumps_version_and_updated_at() {
    let store = InMemoryStore::new();
    let session = SessionId::new("sess_ver_dec");
    let root = RunId::new("run_ver_dec");
    seed_session(&store, &session).await;
    seed_root_run(&store, &session, &root).await;
    store.try_increment_descendants(&root, 3).await.unwrap();

    let before = RunReadModel::get(&store, &root).await.unwrap().unwrap();
    // std::thread::sleep rather than tokio::time::sleep because
    // cairn-store's tokio dev-dep is built without the `time` feature.
    // Blocking ~5ms inside a #[tokio::test] is fine here.
    std::thread::sleep(std::time::Duration::from_millis(5));

    store.decrement_descendants(&root).await.unwrap();

    let after = RunReadModel::get(&store, &root).await.unwrap().unwrap();
    assert!(
        after.version > before.version,
        "decrement_descendants must bump version (before={}, after={})",
        before.version,
        after.version,
    );
    assert!(
        after.updated_at > before.updated_at,
        "decrement_descendants must bump updated_at (before={}, after={})",
        before.updated_at,
        after.updated_at,
    );
}

#[tokio::test]
async fn try_increment_rejects_at_cap() {
    let store = InMemoryStore::new();
    let session = SessionId::new("sess_cap");
    let root = RunId::new("run_root_cap");
    seed_session(&store, &session).await;
    seed_root_run(&store, &session, &root).await;

    // Fill to cap.
    for _ in 0..3 {
        let out = store.try_increment_descendants(&root, 3).await.unwrap();
        assert!(matches!(out, DescendantsCapOutcome::Admitted { .. }));
    }
    // Cap reached — next increment is rejected.
    let out = store.try_increment_descendants(&root, 3).await.unwrap();
    assert_eq!(out, DescendantsCapOutcome::CapReached);
    // And the counter is NOT incremented on a CapReached.
    let record = RunReadModel::get(&store, &root).await.unwrap().unwrap();
    assert_eq!(
        record.in_flight_descendants, 3,
        "counter must not exceed cap even by transient overshoot"
    );
}

#[tokio::test]
async fn try_increment_returns_root_not_found_for_unknown_run() {
    let store = InMemoryStore::new();
    let unknown = RunId::new("run_unknown");
    let out = store.try_increment_descendants(&unknown, 3).await.unwrap();
    assert_eq!(out, DescendantsCapOutcome::RootNotFound);
}

#[tokio::test]
async fn decrement_returns_post_decrement_count() {
    let store = InMemoryStore::new();
    let session = SessionId::new("sess_dec");
    let root = RunId::new("run_root_dec");
    seed_session(&store, &session).await;
    seed_root_run(&store, &session, &root).await;

    store.try_increment_descendants(&root, 3).await.unwrap();
    store.try_increment_descendants(&root, 3).await.unwrap();
    let out = store.decrement_descendants(&root).await.unwrap();
    assert_eq!(out, DescendantsCapOutcome::Admitted { new_count: 1 });
    let out = store.decrement_descendants(&root).await.unwrap();
    assert_eq!(out, DescendantsCapOutcome::Admitted { new_count: 0 });
}

#[tokio::test]
async fn underflow_returns_negative_count_not_panic() {
    let store = InMemoryStore::new();
    let session = SessionId::new("sess_under");
    let root = RunId::new("run_root_under");
    seed_session(&store, &session).await;
    seed_root_run(&store, &session, &root).await;

    // No prior increment — decrement underflows.
    let out = store.decrement_descendants(&root).await.unwrap();
    assert_eq!(
        out,
        DescendantsCapOutcome::Admitted { new_count: -1 },
        "underflow must return Admitted{{new_count: -1}}, not panic. \
         Negative counts are the auditable signal RFC 027 specifies \
         (`child_run_driver_descendant_underflow_total` metric on the \
         adapter layer)."
    );
}

#[tokio::test]
async fn decrement_returns_root_not_found_for_unknown_run() {
    let store = InMemoryStore::new();
    let unknown = RunId::new("run_unknown_dec");
    let out = store.decrement_descendants(&unknown).await.unwrap();
    assert_eq!(
        out,
        DescendantsCapOutcome::RootNotFound,
        "decrement against an unknown run is a no-op (RootNotFound). \
         Per RFC 027 this covers the pre-V069 descendant case where \
         root_run_id is NULL and callers resolve None to no-op here."
    );
}

/// Concurrency: many parallel increments against the same root with
/// cap=3 must admit at most three. Rest must see `CapReached`.
#[tokio::test]
async fn concurrent_increments_respect_cap() {
    let store = Arc::new(InMemoryStore::new());
    let session = SessionId::new("sess_conc");
    let root = RunId::new("run_root_conc");
    seed_session(&store, &session).await;
    seed_root_run(&store, &session, &root).await;

    const WORKERS: usize = 20;
    const CAP: i64 = 3;
    let mut handles = Vec::with_capacity(WORKERS);
    for _ in 0..WORKERS {
        let s = store.clone();
        let r = root.clone();
        handles.push(tokio::spawn(async move {
            s.try_increment_descendants(&r, CAP).await.unwrap()
        }));
    }
    let mut admitted = 0;
    let mut rejected = 0;
    for h in handles {
        match h.await.unwrap() {
            DescendantsCapOutcome::Admitted { .. } => admitted += 1,
            DescendantsCapOutcome::CapReached => rejected += 1,
            DescendantsCapOutcome::RootNotFound => {
                panic!("root was seeded — RootNotFound is a bug")
            }
        }
    }
    assert_eq!(
        admitted as i64, CAP,
        "exactly CAP (={CAP}) increments must succeed even under \
         concurrent pressure; {admitted} did"
    );
    assert_eq!(
        rejected,
        WORKERS - CAP as usize,
        "the remaining {remaining} must see CapReached",
        remaining = WORKERS - CAP as usize
    );
    let record = RunReadModel::get(store.as_ref(), &root)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(record.in_flight_descendants, CAP);
}
