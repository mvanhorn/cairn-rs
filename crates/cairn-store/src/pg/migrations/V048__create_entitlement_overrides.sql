-- RFC-025 Phase 2a.2 milestone 4: entitlement_overrides projection.
--
-- Projects `EntitlementOverrideSet`. Operator-applied override that
-- supplements or restricts the base license entitlements per tenant,
-- keyed by `(tenant_id, feature)`. `ON CONFLICT (tenant_id, feature)
-- DO UPDATE` matches the in-memory `HashMap<{tenant}:{feature},
-- Record>::insert` latest-wins semantic (see
-- `crates/cairn-store/src/in_memory.rs` — the EntitlementOverrideSet
-- applier).
--
-- `allowed` carries the effective enable/disable decision; `reason`
-- is an operator-supplied audit string (nullable).
--
-- The in-memory applier synthesizes `override_id` as
-- `override_{tenant_id}_{feature}` and hardcodes `entitlement` to
-- `Entitlement::AdvancedAdmin` — the record fields `override_id` and
-- `entitlement` on `EntitlementOverrideRecord` are legacy shape carried
-- through for the serde contract, not actual projected data. The pg/sqlite
-- read side mirrors both so `LicenseReadModel::list_overrides` is
-- byte-equal across backends.
--
-- Portability: no JSONB, no pg arrays per
-- `feedback_no_db_specific_features.md`. `allowed` is BOOLEAN on pg;
-- sqlite mirrors with INTEGER 0/1.

CREATE TABLE IF NOT EXISTS entitlement_overrides (
    tenant_id   TEXT    NOT NULL,
    feature     TEXT    NOT NULL,
    allowed     BOOLEAN NOT NULL,
    reason      TEXT,
    set_at_ms   BIGINT  NOT NULL,
    created_at  BIGINT  NOT NULL,
    updated_at  BIGINT  NOT NULL,
    PRIMARY KEY (tenant_id, feature)
);

-- Tenant-scoped list is the hot read (`LicenseReadModel::list_overrides`
-- scans every feature for a tenant). PK already leads with tenant_id so
-- no separate index required.
