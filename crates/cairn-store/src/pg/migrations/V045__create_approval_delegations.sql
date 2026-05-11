-- RFC-025 Phase 2a.2 milestone 1: approval_delegations projection table.
--
-- Projects `ApprovalDelegated` — a per-event audit trail of approval
-- delegations ("approval X was delegated to operator Y at time T"). Each
-- delegation is a discrete audit entry keyed by `(approval_id,
-- delegation_id)`; replaying the same event never duplicates a row.
--
-- This is an *audit* projection: the row is append-only and never
-- mutates. Unlike `approvals` (where ApprovalResolved mutates an existing
-- row) the delegation record is a new row per delegation action. That
-- mirrors how `credential_rotations` projects `CredentialKeyRotated`
-- (one row per rotation event) rather than mutating the `credentials`
-- row.
--
-- Primary key is `(approval_id, delegation_id)`. `delegation_id` is a
-- monotonic identifier minted by the runtime service at emit time
-- (`approval_impl::next_delegation_id`). Previous revisions used
-- `(approval_id, delegated_at_ms, delegated_to)` as the PK, but that
-- still collapses two rapid delegations of the same approval to the
-- *same* operator in the same millisecond (a plausible race under
-- contention) into one row via `ON CONFLICT DO NOTHING`. Adding
-- `delegation_id` makes the PK lossless for any pair of distinct
-- delegation events (Copilot #571 round 4).
--
-- For backward-compat with pre-Phase-2a.2 event-log entries that
-- predate `delegation_id`, the column defaults to the empty string and
-- the event struct marks the field `#[serde(default)]`. Legacy events
-- that replay coexist with new events via a single catch-all row per
-- approval (all keyed `delegation_id = ""`) which is the correct
-- idempotent behavior for those legacy emissions.
--
-- The in-memory applier stores one `ApprovalDelegationRecord` per event
-- with the same composite-key semantic. Portability: no JSONB, no pg
-- arrays per `feedback_no_db_specific_features.md`.

CREATE TABLE IF NOT EXISTS approval_delegations (
    approval_id       TEXT    NOT NULL,
    delegation_id     TEXT    NOT NULL DEFAULT '',
    delegated_to      TEXT    NOT NULL,
    delegated_at_ms   BIGINT  NOT NULL,
    created_at        BIGINT  NOT NULL,
    PRIMARY KEY (approval_id, delegation_id)
);

-- Read-pattern index: the operator UI lists delegations for a specific
-- approval ordered by `(delegated_at_ms ASC, delegation_id ASC)` so an
-- index leading with `approval_id → delegated_at_ms → delegation_id`
-- lets pg walk the audit trail in read-order without a secondary sort.
-- The PK alone is not a good match because it sorts by `delegation_id`
-- rather than `delegated_at_ms` within an approval.
CREATE INDEX IF NOT EXISTS idx_approval_delegations_read_model
    ON approval_delegations (approval_id, delegated_at_ms, delegation_id);
