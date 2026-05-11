# Admin & Auth

Tenant, workspace, license, credential, retention, audit-log, snapshot/restore, capability, and notification administration — plus bearer-token lifecycle under `/v1/auth/tokens/*`. Most routes here require the admin role (bearer token resolving to `role: admin`).

Source of truth: [`tests/compat/http_routes.tsv`](../../tests/compat/http_routes.tsv). Drift from this table against the live router is enforced by `cargo test -p cairn-api --test compat_catalog_sync`.

**Routes: 58**

| Method | Path | Classification | Notes |
|---|---|---|---|
| `GET` | `/v1/admin/agents/live` | Preserve | #789; live fleet view — every active agent (run state ∈ {pending, running, waiting_approval, waiting_dependency, paused}) joined with the latest reasoning step (current_action + reasoning preview + confidence). Tenant-scoped. |
| `GET` | `/v1/admin/audit-log` | Preserve | audit log entries |
| `GET` | `/v1/admin/audit-log/:resource_type/:resource_id` | Preserve |  |
| `POST` | `/v1/admin/backup` | Preserve |  |
| `GET` | `/v1/admin/capabilities` | Preserve | feature capability map |
| `GET` | `/v1/admin/entitlements` | Preserve | tenant entitlement set |
| `GET` | `/v1/admin/event-count` | Preserve |  |
| `GET` | `/v1/admin/event-log` | Preserve |  |
| `GET` | `/v1/admin/license` | Preserve | active license record |
| `POST` | `/v1/admin/license/activate` | Preserve | body: { license_key }; { ok } |
| `POST` | `/v1/admin/license/override` | Preserve |  |
| `GET` | `/v1/admin/logs` | Preserve | admin logs |
| `GET` | `/v1/admin/models` | Preserve |  |
| `DELETE` | `/v1/admin/models/:id` | Preserve |  |
| `GET` | `/v1/admin/models/:id` | Preserve |  |
| `PUT` | `/v1/admin/models/:id` | Preserve |  |
| `POST` | `/v1/admin/models/import-litellm` | Preserve |  |
| `POST` | `/v1/admin/notifications/:id/retry` | Preserve |  |
| `GET` | `/v1/admin/notifications/failed` | Preserve | failed notification records |
| `GET` | `/v1/admin/operators/:id/notifications` | Preserve |  |
| `POST` | `/v1/admin/operators/:id/notifications` | Preserve |  |
| `DELETE` | `/v1/admin/operators/:id/tenant-roles/:tenant` | Preserve | RFC 026 PR-A0: soft-revoke tenant role grant (row retained with revocation fields). |
| `POST` | `/v1/admin/operators/:id/tenant-roles/:tenant/promote` | Preserve | RFC 026 PR-A0: grant a tenant-scope role (Admin / Member / ReadOnly) to an operator. |
| `POST` | `/v1/admin/rebuild-projections` | Preserve |  |
| `POST` | `/v1/admin/restore` | Preserve |  |
| `POST` | `/v1/admin/rotate-token` | Preserve |  |
| `POST` | `/v1/admin/rotate-waitpoint-hmac` | Preserve |  |
| `POST` | `/v1/admin/snapshot` | Preserve |  |
| `GET` | `/v1/admin/tenants` | Preserve | { items } |
| `POST` | `/v1/admin/tenants` | Preserve |  |
| `GET` | `/v1/admin/tenants/:id` | Preserve |  |
| `PATCH` | `/v1/admin/tenants/:id` | Preserve | RFC-026 PR-A2 tenant edit. |
| `POST` | `/v1/admin/tenants/:id/compact-event-log` | Preserve |  |
| `GET` | `/v1/admin/tenants/:id/overview` | Preserve |  |
| `POST` | `/v1/admin/tenants/:id/restore` | Preserve |  |
| `POST` | `/v1/admin/tenants/:id/snapshot` | Preserve |  |
| `GET` | `/v1/admin/tenants/:id/snapshots` | Preserve |  |
| `POST` | `/v1/admin/tenants/:tenant_id/apply-retention` | Preserve |  |
| `GET` | `/v1/admin/tenants/:tenant_id/credentials` | Preserve |  |
| `POST` | `/v1/admin/tenants/:tenant_id/credentials` | Preserve |  |
| `DELETE` | `/v1/admin/tenants/:tenant_id/credentials/:id` | Preserve |  |
| `POST` | `/v1/admin/tenants/:tenant_id/credentials/rotate-key` | Preserve |  |
| `GET` | `/v1/admin/tenants/:tenant_id/operator-profiles` | Preserve |  |
| `POST` | `/v1/admin/tenants/:tenant_id/operator-profiles` | Preserve |  |
| `PATCH` | `/v1/admin/tenants/:tenant_id/operator-profiles/:id` | Preserve | RFC-026 PR-A2 operator profile edit. |
| `GET` | `/v1/admin/tenants/:tenant_id/operators/:operator_id/tenant-roles` | Preserve | RFC-026 PR-A4: list tenant-role grants for an operator (includes revoked rows with audit fields). |
| `GET` | `/v1/admin/tenants/:tenant_id/quota` | Preserve |  |
| `POST` | `/v1/admin/tenants/:tenant_id/quota` | Preserve |  |
| `GET` | `/v1/admin/tenants/:tenant_id/retention-policy` | Preserve |  |
| `POST` | `/v1/admin/tenants/:tenant_id/retention-policy` | Preserve |  |
| `POST` | `/v1/admin/tenants/:tenant_id/runs/:id/cancel-orphan` | Preserve | #670 G4 / RFC 027 §Orphan-child: operator recovery for child runs wedged in `Pending` after a crash between spawn Phase-1 and Phase-2. Transitions to `Failed(OrphanChild)` and releases the descendant-counter slot on the captured root. Rejects roots (422) and non-Pending state (422); wrong tenant in path returns 404 (not 403). |
| `DELETE` | `/v1/admin/tenants/:tenant_id/sessions/:session_id` | Preserve | Admin-scoped session delete. |
| `GET` | `/v1/admin/tenants/:tenant_id/workspaces` | Preserve |  |
| `POST` | `/v1/admin/tenants/:tenant_id/workspaces` | Preserve |  |
| `DELETE` | `/v1/admin/tenants/:tenant_id/workspaces/:workspace_id` | Preserve | Soft-delete: archives workspace + cascades archival to children. |
| `GET` | `/v1/admin/workspaces` | Preserve | { items } |
| `GET` | `/v1/admin/workspaces/:id/shares` | Preserve |  |
| `POST` | `/v1/admin/workspaces/:id/shares` | Preserve |  |
| `DELETE` | `/v1/admin/workspaces/:id/shares/:share_id` | Preserve |  |
| `GET` | `/v1/admin/workspaces/:workspace_id/members` | Preserve |  |
| `POST` | `/v1/admin/workspaces/:workspace_id/members` | Preserve |  |
| `DELETE` | `/v1/admin/workspaces/:workspace_id/members/:id` | Preserve |  |
| `GET` | `/v1/admin/workspaces/:workspace_id/projects` | Preserve |  |
| `POST` | `/v1/admin/workspaces/:workspace_id/projects` | Preserve |  |
| `GET` | `/v1/auth/tokens` | Preserve |  |
| `POST` | `/v1/auth/tokens` | Preserve |  |
| `DELETE` | `/v1/auth/tokens/:id` | Preserve |  |

<!-- TODO: contract bodies (tracked as follow-up) -->
