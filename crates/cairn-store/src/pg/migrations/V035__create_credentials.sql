-- RFC-025 Phase 2a.1: credentials + credential_rotations projection tables.
--
-- Projects three credential lifecycle events into durable read-model
-- tables so operators can query credentials after a process restart
-- without walking the event log.
--
-- Before this migration, pg/sqlite backends logged CredentialStored /
-- CredentialRevoked / CredentialKeyRotated with `log_stub` — the event
-- hit the event_log but no read-model row was written. The InMemoryStore
-- was the only projection layer serving cairn-app reads. Flipping these
-- to real pg/sqlite projections is the precondition for deleting the
-- boot-time legacy-format scanner fall-back path on pg/sqlite.
--
-- Schema kept portable for SQLite parity: no JSONB, no pg arrays. The
-- encrypted credential material is stored as BYTEA on pg / BLOB on sqlite
-- (the `encrypted_value` column). See feedback_no_db_specific_features.md
-- in MEMORY.md.
--
-- ## Idempotency contract
--
-- `credentials.credential_id` is the primary key — replaying the event
-- log re-upserts without creating duplicates. `CredentialStored` is an
-- INSERT ... ON CONFLICT (credential_id) DO UPDATE that refreshes the
-- encrypted material + key bindings + updated_at WITHOUT touching
-- `active` or `revoked_at_ms`. That means a replay (or a live re-store)
-- after a `CredentialRevoked` preserves the revoked state: no
-- reactivation path sneaks in via the storage event. Operators who
-- want to un-revoke must issue the dedicated reactivation flow; a
-- duplicate `Stored` event is a no-op on revocation state. The
-- in-memory applier in `in_memory.rs` mirrors this exact semantic.
--
-- `credential_rotations.rotation_id` is the primary key — the same row
-- survives replay. No UPDATE semantics needed (rotation is append-only).

CREATE TABLE IF NOT EXISTS credentials (
    credential_id     TEXT    PRIMARY KEY,
    tenant_id         TEXT    NOT NULL,
    -- `name` mirrors the in-memory record; provider_id is what the event
    -- carries on the write path, and the in-memory projection sets
    -- `name = provider_id` on CredentialStored. The two columns are kept
    -- distinct here so a future event carrying a separate display name
    -- can land without a migration.
    name              TEXT    NOT NULL,
    provider_id       TEXT    NOT NULL,
    credential_type   TEXT    NOT NULL,
    encrypted_value   BYTEA   NOT NULL,
    -- Envelope-encryption key identity — nullable so events persisted
    -- before the per-credential key_id/version columns existed still
    -- project cleanly.
    key_id            TEXT,
    key_version       TEXT,
    active            BOOLEAN NOT NULL DEFAULT TRUE,
    encrypted_at_ms   BIGINT,
    revoked_at_ms     BIGINT,
    created_at        BIGINT  NOT NULL,
    updated_at        BIGINT  NOT NULL
);

-- Tenant-scoped list queries are the primary read pattern
-- (`GET /v1/credentials?tenant_id=…`) and the boot-time active-credentials
-- scan. Index on (tenant_id, active) lets the partial scan skip revoked
-- rows without a full table walk.
CREATE INDEX IF NOT EXISTS idx_credentials_tenant_active
    ON credentials (tenant_id, active);

CREATE TABLE IF NOT EXISTS credential_rotations (
    rotation_id         TEXT    PRIMARY KEY,
    tenant_id           TEXT    NOT NULL,
    -- `credential_id` is not NOT NULL because the rotation event carries
    -- a list of credential ids rotated; the projection stores one row
    -- per rotation event, not per credential. The in-memory record uses
    -- `CredentialId::new("")` as a placeholder when no single credential
    -- owns the rotation — stored here as empty-string for byte parity.
    credential_id       TEXT    NOT NULL DEFAULT '',
    old_key_id          TEXT    NOT NULL,
    new_key_id          TEXT    NOT NULL,
    rotated_credentials INTEGER NOT NULL DEFAULT 0,
    started_at_ms       BIGINT  NOT NULL,
    completed_at_ms     BIGINT,
    rotated_at          BIGINT  NOT NULL,
    rotated_by          TEXT
);

CREATE INDEX IF NOT EXISTS idx_credential_rotations_tenant
    ON credential_rotations (tenant_id, rotated_at);
