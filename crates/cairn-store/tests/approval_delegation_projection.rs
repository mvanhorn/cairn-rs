//! RFC-025 Phase 2a.2 milestone 1: approval_delegations projection
//! integration tests.
//!
//! Validates that `ApprovalDelegated` events project identically across
//! the three backends (in-memory, sqlite, pg via separate parity harness)
//! and that restart-durability is preserved on sqlite: the projection
//! table contains the audit trail after a fresh adapter is built against
//! the same file-backed pool.
//!
//! Parity with pg + sqlite is covered by the shared parity harness at
//! `crates/cairn-store/tests/projection_parity.rs`; this file focuses on
//! semantic contracts (idempotency, multi-delegation sequencing, cross-
//! approval isolation, restart durability).

use cairn_domain::{
    ApprovalDelegated, ApprovalId, EventEnvelope, EventId, EventSource, RuntimeEvent,
};
use cairn_store::{
    projections::ApprovalDelegationReadModel, sqlite::SqliteAdapter, EventLog, InMemoryStore,
};

fn evt(id: &str, payload: RuntimeEvent) -> EventEnvelope<RuntimeEvent> {
    EventEnvelope::for_runtime_event(EventId::new(id), EventSource::Runtime, payload)
}

fn delegation(approval_id: &str, delegated_to: &str, delegated_at_ms: u64) -> RuntimeEvent {
    // Test helper: `delegation_id` is derived from the event fields so
    // two distinct logical delegations produce distinct ids (lossless
    // PK), and two replays of the same logical delegation produce the
    // same id (idempotent replay). Matches the runtime service's mint
    // shape.
    let delegation_id = format!("deleg_{approval_id}_{delegated_at_ms}_{delegated_to}");
    delegation_with_id(approval_id, delegated_to, delegated_at_ms, &delegation_id)
}

fn delegation_with_id(
    approval_id: &str,
    delegated_to: &str,
    delegated_at_ms: u64,
    delegation_id: &str,
) -> RuntimeEvent {
    RuntimeEvent::ApprovalDelegated(ApprovalDelegated {
        approval_id: ApprovalId::new(approval_id),
        delegated_to: delegated_to.to_owned(),
        delegated_at_ms,
        delegation_id: delegation_id.to_owned(),
    })
}

// ── 1. Basic projection on in-memory ────────────────────────────────────────

#[tokio::test]
async fn single_delegation_lands_in_in_memory_projection() {
    let store = InMemoryStore::new();
    store
        .append(&[evt("e1", delegation("ap_1", "op_mary", 1_000))])
        .await
        .unwrap();

    let rows = ApprovalDelegationReadModel::list_for_approval(&store, &ApprovalId::new("ap_1"))
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].approval_id.as_str(), "ap_1");
    assert_eq!(rows[0].delegated_to, "op_mary");
    assert_eq!(rows[0].delegated_at_ms, 1_000);
}

// ── 2. Multiple delegations of the same approval are preserved ──────────────

#[tokio::test]
async fn multiple_delegations_of_same_approval_preserve_audit_trail() {
    let store = InMemoryStore::new();
    store
        .append(&[
            evt("e1", delegation("ap_chain", "op_alice", 1_000)),
            evt("e2", delegation("ap_chain", "op_bob", 2_000)),
            evt("e3", delegation("ap_chain", "op_carol", 3_000)),
        ])
        .await
        .unwrap();

    let rows = ApprovalDelegationReadModel::list_for_approval(&store, &ApprovalId::new("ap_chain"))
        .await
        .unwrap();
    assert_eq!(rows.len(), 3, "audit trail must retain every delegation");
    // Oldest first — mirrors pg/sqlite `ORDER BY delegated_at_ms ASC`.
    assert_eq!(rows[0].delegated_to, "op_alice");
    assert_eq!(rows[1].delegated_to, "op_bob");
    assert_eq!(rows[2].delegated_to, "op_carol");
}

// ── 3. Cross-approval isolation ─────────────────────────────────────────────

#[tokio::test]
async fn list_for_approval_is_scoped_to_the_requested_approval() {
    let store = InMemoryStore::new();
    store
        .append(&[
            evt("e1", delegation("ap_a", "op_1", 1_000)),
            evt("e2", delegation("ap_b", "op_2", 1_000)),
            evt("e3", delegation("ap_a", "op_3", 2_000)),
        ])
        .await
        .unwrap();

    let a_rows = ApprovalDelegationReadModel::list_for_approval(&store, &ApprovalId::new("ap_a"))
        .await
        .unwrap();
    let b_rows = ApprovalDelegationReadModel::list_for_approval(&store, &ApprovalId::new("ap_b"))
        .await
        .unwrap();
    assert_eq!(a_rows.len(), 2);
    assert_eq!(b_rows.len(), 1);
    assert!(a_rows.iter().all(|r| r.approval_id.as_str() == "ap_a"));
    assert!(b_rows.iter().all(|r| r.approval_id.as_str() == "ap_b"));
}

// ── 4. Idempotency on replay — composite PK (approval_id, delegation_id) ─

#[tokio::test]
async fn replayed_delegation_event_does_not_duplicate_in_memory() {
    let store = InMemoryStore::new();
    // Two envelopes carrying the same logical delegation (same
    // delegation_id) — the composite primary key (approval_id,
    // delegation_id) must collapse them to a single audit row,
    // mirroring pg/sqlite ON CONFLICT DO NOTHING.
    store
        .append(&[
            evt("e1", delegation("ap_idem", "op_mary", 5_000)),
            evt("e2", delegation("ap_idem", "op_mary", 5_000)),
        ])
        .await
        .unwrap();

    let rows = ApprovalDelegationReadModel::list_for_approval(&store, &ApprovalId::new("ap_idem"))
        .await
        .unwrap();
    assert_eq!(
        rows.len(),
        1,
        "replay of the same (approval_id, delegation_id) must be a no-op"
    );
}

// ── 5. Empty list when approval has no delegations ──────────────────────────

#[tokio::test]
async fn list_for_approval_returns_empty_when_no_delegations() {
    let store = InMemoryStore::new();
    let rows = ApprovalDelegationReadModel::list_for_approval(
        &store,
        &ApprovalId::new("ap_never_delegated"),
    )
    .await
    .unwrap();
    assert!(rows.is_empty());
}

// ── 6. Restart durability: sqlite projection survives adapter rebuild ───────

#[tokio::test]
async fn sqlite_delegations_survive_adapter_restart() {
    use cairn_store::db::DbAdapter;
    use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
    use std::str::FromStr;

    // Tempfile-backed sqlite so a second adapter can open the same file.
    // Each `sqlite::memory:` connection is a private DB, so this
    // restart-durability test requires a real file on disk (same pattern
    // as `event_log_batch_append::concurrent_writers_are_linearizable_sqlite`).
    let tmp = tempfile::NamedTempFile::new().expect("tempfile");
    let url = format!("sqlite:{}", tmp.path().display());

    // Boot 1: apply two delegation events through the sync projection,
    // and verify the live adapter sees both rows.
    {
        let opts = SqliteConnectOptions::from_str(&url)
            .expect("sqlite url")
            .create_if_missing(true)
            .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal);
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(opts)
            .await
            .expect("sqlite pool");
        let adapter = SqliteAdapter::new(pool.clone());
        adapter.migrate().await.expect("migrate");
        let log = cairn_store::sqlite::SqliteEventLog::new(pool);
        log.append(&[
            evt("e1", delegation("ap_durable", "op_alpha", 1_000)),
            evt("e2", delegation("ap_durable", "op_beta", 2_000)),
        ])
        .await
        .unwrap();
        let live = ApprovalDelegationReadModel::list_for_approval(
            &adapter,
            &ApprovalId::new("ap_durable"),
        )
        .await
        .unwrap();
        assert_eq!(live.len(), 2);
    }

    // Boot 2: fresh adapter pool against the same file. The event-log
    // append ran the sync projection inside the same tx during boot 1,
    // so the rows are present without any replay step.
    let opts2 = SqliteConnectOptions::from_str(&url)
        .expect("sqlite url")
        .create_if_missing(false)
        .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal);
    let pool2 = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(opts2)
        .await
        .expect("sqlite pool 2");
    let adapter2 = SqliteAdapter::new(pool2);
    adapter2.migrate().await.expect("migrate 2");
    let rows =
        ApprovalDelegationReadModel::list_for_approval(&adapter2, &ApprovalId::new("ap_durable"))
            .await
            .unwrap();
    assert_eq!(
        rows.len(),
        2,
        "delegations must survive a fresh adapter on the same file-backed pool"
    );
    assert_eq!(rows[0].delegated_to, "op_alpha");
    assert_eq!(rows[1].delegated_to, "op_beta");
}

// ── 7. Widened PK: two distinct delegations in the same ms coexist ──────────
//
// Regression guard for the Copilot #571 review — the original PK was
// `(approval_id, delegated_at_ms)`, which silently collapsed legitimate
// rapid re-delegations that landed in the same millisecond into a single
// row. Round 2 widened to include `delegated_to`; round 4 widened further
// to include a monotonic `delegation_id` so two delegations to the SAME
// operator in the same ms also both survive.

#[tokio::test]
async fn two_delegations_at_same_ms_to_different_operators_both_persist_in_memory() {
    let store = InMemoryStore::new();
    store
        .append(&[
            // Different `event_id` per event, same `delegated_at_ms`.
            evt("e1", delegation("ap_race", "op_mary", 5_000)),
            evt("e2", delegation("ap_race", "op_nick", 5_000)),
        ])
        .await
        .unwrap();

    let rows = ApprovalDelegationReadModel::list_for_approval(&store, &ApprovalId::new("ap_race"))
        .await
        .unwrap();
    assert_eq!(
        rows.len(),
        2,
        "distinct delegates at the same ms must both persist — PK widening regression guard"
    );
    let operators: Vec<&str> = rows.iter().map(|r| r.delegated_to.as_str()).collect();
    assert!(operators.contains(&"op_mary"));
    assert!(operators.contains(&"op_nick"));
}

#[tokio::test]
async fn two_delegations_at_same_ms_to_different_operators_both_persist_in_sqlite() {
    let adapter = SqliteAdapter::in_memory().await.unwrap();
    let log = cairn_store::sqlite::SqliteEventLog::new(adapter.pool().clone());
    log.append(&[
        evt("e1", delegation("ap_race_sq", "op_mary", 5_000)),
        evt("e2", delegation("ap_race_sq", "op_nick", 5_000)),
    ])
    .await
    .unwrap();

    let rows =
        ApprovalDelegationReadModel::list_for_approval(&adapter, &ApprovalId::new("ap_race_sq"))
            .await
            .unwrap();
    assert_eq!(
        rows.len(),
        2,
        "sqlite must persist both delegations at the same ms under the widened PK"
    );
    let operators: Vec<&str> = rows.iter().map(|r| r.delegated_to.as_str()).collect();
    assert!(operators.contains(&"op_mary"));
    assert!(operators.contains(&"op_nick"));
}

// ── 8. Round-4 widening: two delegations to the SAME operator at the same ms
//      both persist because delegation_id is now in the PK ──────────────────
//
// Regression guard for Copilot #571 round 4. Prior PK was
// `(approval_id, delegated_to, delegated_at_ms)` which still dropped a
// row when the same delegator hit the same approval with the same
// `delegated_to` twice in the same millisecond (plausible under
// contention). `delegation_id` is now part of the PK and is minted
// monotonically per emit, so both rows survive.

#[tokio::test]
async fn two_delegations_at_same_ms_to_same_operator_both_persist_in_memory() {
    let store = InMemoryStore::new();
    store
        .append(&[
            // Same (approval_id, delegated_to, delegated_at_ms) but
            // distinct delegation_id: two "logical" delegation acts
            // race into the same ms.
            evt(
                "e1",
                delegation_with_id("ap_dup", "op_mary", 5_000, "deleg_ap_dup_5000_001"),
            ),
            evt(
                "e2",
                delegation_with_id("ap_dup", "op_mary", 5_000, "deleg_ap_dup_5000_002"),
            ),
        ])
        .await
        .unwrap();

    let rows = ApprovalDelegationReadModel::list_for_approval(&store, &ApprovalId::new("ap_dup"))
        .await
        .unwrap();
    assert_eq!(
        rows.len(),
        2,
        "distinct delegation_ids for same (approval_id, delegated_to, delegated_at_ms) \
         must both persist — delegation_id PK widening regression guard"
    );
    let ids: Vec<&str> = rows.iter().map(|r| r.delegation_id.as_str()).collect();
    assert!(ids.contains(&"deleg_ap_dup_5000_001"));
    assert!(ids.contains(&"deleg_ap_dup_5000_002"));
}

#[tokio::test]
async fn two_delegations_at_same_ms_to_same_operator_both_persist_in_sqlite() {
    let adapter = SqliteAdapter::in_memory().await.unwrap();
    let log = cairn_store::sqlite::SqliteEventLog::new(adapter.pool().clone());
    log.append(&[
        evt(
            "e1",
            delegation_with_id("ap_dup_sq", "op_mary", 5_000, "deleg_ap_dup_sq_5000_001"),
        ),
        evt(
            "e2",
            delegation_with_id("ap_dup_sq", "op_mary", 5_000, "deleg_ap_dup_sq_5000_002"),
        ),
    ])
    .await
    .unwrap();

    let rows =
        ApprovalDelegationReadModel::list_for_approval(&adapter, &ApprovalId::new("ap_dup_sq"))
            .await
            .unwrap();
    assert_eq!(
        rows.len(),
        2,
        "sqlite must persist both same-operator same-ms delegations under the delegation_id PK"
    );
    let ids: Vec<&str> = rows.iter().map(|r| r.delegation_id.as_str()).collect();
    assert!(ids.contains(&"deleg_ap_dup_sq_5000_001"));
    assert!(ids.contains(&"deleg_ap_dup_sq_5000_002"));
}
