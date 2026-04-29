-- RFC-025 Phase 2b.2b milestone 1: resource_shares projection.
--
-- Before this migration the `ResourceShared` and `ResourceShareRevoked`
-- events were routed through `log_stub` on pg/sqlite — the event log
-- recorded the share/revoke fact but no read-model row existed on
-- persistent backends. Every restart wiped the operator's active
-- cross-workspace share catalog even though the events were durable on
-- the log (issue #581).
--
-- Revocation semantics: `ResourceShareRevoked` DELETEs the row (matches
-- the in-memory applier's `state.resource_shares.remove(&share_id)`).
--
-- Replay discipline: `ResourceShared` uses `ON CONFLICT (share_id) DO
-- NOTHING`. The ON CONFLICT path ONLY fires when the row still exists
-- (e.g. a replayed Shared immediately after a Shared). After a
-- `ResourceShareRevoked` the row is DELETEd, so a replayed `Shared`
-- for the same share_id finds no conflict and re-inserts — the
-- in-memory `HashMap::insert` has the same behaviour. Revocation is
-- therefore NOT terminal under event-log replay; both backends agree
-- on the final state (see `resource_share_replay_after_revoke_*`
-- parity test for the cross-backend contract). In practice the
-- service layer allocates a fresh `share_id` per live share call
-- (`next_share_id()`), so the replay path is the only way this
-- matters — a boot-time walk of the event log.
--
-- Scoping: `share_id` is globally unique (sequence-backed); read-model
-- queries filter by `tenant_id` + `target_workspace_id` for the "shares
-- visible to workspace B" hot path. PK is `share_id`; we add a
-- tenant+target index for list_shares_for_workspace.
--
-- Portability: TEXT + BIGINT only. `permissions` is a JSON array stored
-- as TEXT (no pg arrays, no JSONB) — portable to SQLite. Empty permissions
-- default to '[]'.

CREATE TABLE IF NOT EXISTS resource_shares (
    share_id              TEXT    PRIMARY KEY,
    tenant_id             TEXT    NOT NULL,
    source_workspace_id   TEXT    NOT NULL,
    target_workspace_id   TEXT    NOT NULL,
    resource_type         TEXT    NOT NULL,
    resource_id           TEXT    NOT NULL,
    permissions_json      TEXT    NOT NULL DEFAULT '[]',
    shared_at_ms          BIGINT  NOT NULL
);

-- Hot path: list active shares granted to a specific workspace.
-- Sort tiebreak on share_id for deterministic pagination.
CREATE INDEX IF NOT EXISTS idx_resource_shares_target
    ON resource_shares (tenant_id, target_workspace_id, shared_at_ms, share_id);

-- Supports get_share_for_resource's uniqueness lookup (tenant + target +
-- resource_type + resource_id).
CREATE INDEX IF NOT EXISTS idx_resource_shares_resource
    ON resource_shares (tenant_id, target_workspace_id, resource_type, resource_id);
