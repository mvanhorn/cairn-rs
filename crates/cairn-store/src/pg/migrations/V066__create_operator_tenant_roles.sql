-- RFC 026 PR-A0: operator_tenant_roles projection.
--
-- N-to-M join between operators and tenants carrying a scoped admin role.
-- An operator can hold distinct TenantRoles on multiple tenants (e.g.
-- Admin on tenant T, Member on tenant T'). Emitted by `TenantRoleGranted`
-- (upsert) and updated by `TenantRoleRevoked` (sets `revoked_at_ms` +
-- `revoked_by`; row is not deleted so the audit trail survives).
--
-- Primary key `(tenant_id, operator_id)` — pg V019 owns the companion
-- workspace_members projection with the same (workspace/operator) shape.
--
-- Secondary index on `operator_id` supports fast "which tenants does
-- this operator administer?" queries from the middleware, which runs on
-- every admin-path request.

CREATE TABLE IF NOT EXISTS operator_tenant_roles (
    tenant_id      TEXT    NOT NULL,
    operator_id    TEXT    NOT NULL,
    role           TEXT    NOT NULL,
    granted_at_ms  BIGINT  NOT NULL,
    granted_by     TEXT    NOT NULL,
    revoked_at_ms  BIGINT,
    revoked_by     TEXT,
    PRIMARY KEY (tenant_id, operator_id)
);

CREATE INDEX IF NOT EXISTS idx_operator_tenant_roles_operator
    ON operator_tenant_roles (operator_id);
