-- RFC-025 Phase 2a.2 milestone 3: retention_policies projection table.
--
-- Projects `RetentionPolicySet` — one active retention policy per
-- tenant. Keyed by `tenant_id`; `ON CONFLICT (tenant_id) DO UPDATE`
-- replaces the baseline (mirrors the in-memory `HashMap::insert`
-- latest-wins semantic).
--
-- `max_events_per_entity` is the event payload's `Option<u64>`; stored
-- as nullable BIGINT. The read-model maps None → 0 on retrieval to
-- match the in-memory `RetentionPolicy.max_events_per_entity` (u32)
-- contract, which uses 0 as the "no cap" sentinel.
--
-- Portability: no JSONB, no pg arrays per `feedback_no_db_specific_features.md`.

CREATE TABLE IF NOT EXISTS retention_policies (
    tenant_id              TEXT    PRIMARY KEY,
    policy_id              TEXT    NOT NULL,
    full_history_days      INTEGER NOT NULL,
    current_state_days     INTEGER NOT NULL,
    max_events_per_entity  BIGINT,
    created_at             BIGINT  NOT NULL,
    updated_at             BIGINT  NOT NULL
);
