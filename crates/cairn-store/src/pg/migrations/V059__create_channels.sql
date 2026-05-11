-- RFC-025 Phase 2b.3 milestone 3: channels + channel_messages projections.
--
-- Before this migration the `ChannelCreated`, `ChannelMessageSent`, and
-- `ChannelMessageConsumed` events were routed through `log_stub` on
-- pg/sqlite — `ChannelReadModel::{get_channel,list_channels,list_messages}`
-- returned empty on a cold boot and every restart wiped the operator
-- channel catalog.
--
-- Two tables:
-- * `channels`         — one row per channel_id (project-scoped).
-- * `channel_messages` — one row per (channel_id, message_id), with
--                        `consumed_by` + `consumed_at_ms` patched in by
--                        ChannelMessageConsumed.
--
-- Replayed events are idempotent via ON CONFLICT DO NOTHING on
-- Created/Sent; Consumed is an UPDATE so a replay just re-writes the
-- same (consumed_by, consumed_at_ms) tuple.

CREATE TABLE IF NOT EXISTS channels (
    channel_id     TEXT    PRIMARY KEY,
    tenant_id      TEXT    NOT NULL,
    workspace_id   TEXT    NOT NULL,
    project_id     TEXT    NOT NULL,
    name           TEXT    NOT NULL,
    capacity       INTEGER NOT NULL,
    created_at_ms  BIGINT  NOT NULL,
    updated_at_ms  BIGINT  NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_channels_project
    ON channels (tenant_id, workspace_id, project_id, created_at_ms, channel_id);

CREATE TABLE IF NOT EXISTS channel_messages (
    channel_id       TEXT    NOT NULL,
    message_id       TEXT    NOT NULL,
    sender_id        TEXT    NOT NULL,
    body             TEXT    NOT NULL,
    sent_at_ms       BIGINT  NOT NULL,
    consumed_by      TEXT,
    consumed_at_ms   BIGINT,
    PRIMARY KEY (channel_id, message_id)
);

CREATE INDEX IF NOT EXISTS idx_channel_messages_channel
    ON channel_messages (channel_id, sent_at_ms, message_id);
