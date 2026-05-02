# RFC 026: Admin Surface Scope + v1 Cutoff

Status: draft
Owner: architecture lead
Depends on: [RFC 008](./008-tenant-workspace-profile.md) (scope model), [RFC 010](./010-operator-control-plane-ia.md) (IA rules)

## Summary

Cairn's admin surface is **asymmetrically built**: 43 admin-guarded HTTP
handlers exist in `crates/cairn-app/src/handlers/admin.rs`, but only 3
admin-relevant UI pages ship (`AuditLogPage`, `CredentialsPage`,
`WorkspacesPage`). The UI API client at `ui/src/lib/api.ts` exposes ~14
admin-surface client methods (`listTenants`, `createTenant`, `listProjects`,
`createProject`, `getWorkspaces`, `createWorkspace`, `deleteWorkspace`,
`getAuditLog`, `getRequestLogs`, `getCredentials`, `storeCredential`,
`revokeCredential`, `getFailedNotifications`, `retryNotification`), leaving
~29 backend handlers unreachable from the UI. Operators running self-hosted
cairn cannot currently administer their deployment without shelling `curl`
against the REST API for 70% of admin functions.

This RFC scopes:

1. What admin *means* in cairn (per RFC-010 rules).
2. What exists today (backend + UI coverage matrix).
3. What v1 ships vs what slips to v1.1+.
4. Build order + sequencing dependencies.

The result is a prioritized roadmap that can be split into ~6 stacked PRs
with clear acceptance criteria per slice.

## Why

RFC-010 states admin is **not a separate top-level section**; it is
tenant-scope elevation of `Settings`. Today this promise is half-kept:

- The backend implements tenant-scope admin (tenant CRUD, quota,
  retention, snapshots, etc.) — verified: 43 handlers wired through
  `AdminRoleGuard`.
- The `Settings` UI page covers defaults, but not the tenant-scope
  admin actions RFC-010 §Visibility-Scope-Rule lists as primary
  tenant-level workflows (settings admin, policy-baseline admin,
  credential/provider admin, workspace provisioning).

Two consequences:

- **Operators cannot run their own tenants.** Creating a tenant,
  setting a quota, configuring retention, compacting the event log,
  retrying failed notifications — all are API-only today.
- **"Admin-only" is ill-defined in the product surface.** Some admin
  actions (credentials, workspaces) have UIs; others (tenants,
  operators, quotas, retention, snapshots) don't. Operators
  discovering missing UI conclude "it's not a feature" even though
  the backend is there.

v1 ships cairn as a credible **self-hosted control plane**. Missing
admin UI breaks the credibility of that promise.

## What "Admin" Means in Cairn

Per RFC-010 §Visibility-Scope-Rule (authoritative), tenant-scope is
where admin lives:

> - tenant-level visibility exists primarily for settings, policy
>   inheritance, credential/provider administration, and roll-up
>   health

And per RFC-010 §Tenant-Roll-Up-Mutability-Rule:

> - settings administration
> - policy-baseline administration
> - credential and provider administration
> - workspace or project provisioning actions where those belong in-product

**This RFC does NOT re-scope the IA.** Admin features ship as expanded
coverage under the existing `Settings` top-level view, with role-gated
sections for tenant-scope operations. Non-admin operators see the
project/workspace tabs only; operators with tenant-admin role see the
tenant-scope sections unlocked.

Explicitly out-of-scope for RFC-026:

- A separate `/admin` top-level route (RFC-010 rejected this)
- Cross-tenant operations UI (RFC-010 §Cross-Workspace rule: "not a
  primary v1 workflow")
- A "platform superadmin" role distinct from tenant-admin (RFC-008
  already defines the tenant role hierarchy)

## Current Coverage Matrix

### Backend surface (43 admin-guarded handlers in `admin.rs`)

| Domain | Handlers | Status |
|--------|---------|--------|
| Tenants | `list_tenants`, `create_tenant`, `get_tenant`, `get_tenant_overview` | complete |
| Quotas | `get_tenant_quota`, `set_tenant_quota` | complete |
| Retention | `get_retention_policy`, `set_retention_policy`, `apply_retention` | complete |
| Audit log | `list_audit_log`, `list_audit_log_for_resource`, `list_request_logs` | complete |
| Event log | `compact_event_log` | complete |
| Snapshots | `create_snapshot`, `list_snapshots`, `restore_from_snapshot` | complete |
| Workspaces | `create_workspace`, `list_workspaces`, `delete_workspace` | complete |
| Projects | `create_project`, `list_projects` | complete |
| Workspace members | `add`, `list`, `remove` | complete |
| Workspace shares | `create`, `list`, `revoke` | complete |
| Credentials | `store`, `list`, `revoke`, `rotate_key` | complete |
| Operators | `create_operator_profile`, `list_operator_profiles` | complete |
| Operator notifications | `set_notifications`, `get_notifications`, `list_failed`, `retry` | complete |

### UI surface (admin-relevant pages)

| Domain | Page | Status |
|--------|------|--------|
| Audit log | `AuditLogPage.tsx` | ships |
| Credentials | `CredentialsPage.tsx` | ships |
| Workspaces | `WorkspacesPage.tsx` | ships |
| **Tenants** | — | **missing** |
| **Operators** | — | **missing** (ProfilePage exists but is per-user, not admin list) |
| **Quotas** | — | **missing** |
| **Retention** | — | **missing** |
| **Snapshots + event log compaction** | — | **missing** |
| **Request logs** | — | **missing** (overlaps with AuditLog; merge candidate) |
| **Failed notifications** | `ChannelsPage.tsx` (partial) | **partially covered** (retryNotification wired, but UI lacks admin-specific failed-notification list view) |

### API client (`ui/src/lib/api.ts`)

Exposes: ~14 admin-surface client methods (listTenants, createTenant, listProjects,
createProject, getWorkspaces, createWorkspace, deleteWorkspace, getAuditLog,
getRequestLogs, getCredentials, storeCredential, revokeCredential, getFailedNotifications,
retryNotification). **~29 of the 43 backend handlers are unreachable from the UI.**
Coverage breakdown by domain:

| Domain | Backend handlers | UI client methods | Coverage |
|--------|------------------|-------------------|----------|
| Tenants | 4 (list, create, get, overview) | 1 (listTenants) | **25%** |
| Quotas | 2 (get, set) | 0 | **0%** |
| Retention | 3 (get, set, apply) | 0 | **0%** |
| Workspace membership | 3 (add, list, remove) | 0 | **0%** |
| Workspace shares | 3 (create, list, revoke) | 0 | **0%** |
| Operators | 2 (create, list) | 0 | **0%** |
| Operator notifications | 4 (get, set, list-failed, retry) | 1 (retryNotification) | **25%** |
| Workspaces | 3 (create, list, delete) | 3 (all) | **100%** |
| Credentials | 4 (store, list, revoke, rotate) | 3 (store, list, revoke) | **75%** |
| Projects | 2 (create, list) | 2 (all) | **100%** |
| Audit/Request logs | 3 (list, list-for-resource, request) | 2 (list, request-logs) | **67%** |
| Snapshots | 3 (create, list, restore) | 0 | **0%** |
| Event log | 1 (compact) | 0 | **0%** |
| Models | 4 (list, get, set, delete) + litellm | 0 | **0%** |
| Other | 2 (rotate-waitpoint-hmac, import-litellm) | 0 | **0%** |

Overall: **14/43 (33%) coverage**.

## Backend Gaps (no-endpoint admin actions)

Audit of cairn-app handlers + handlers/admin.rs reveals four missing
backend capabilities operators will need but can't perform today:

1. **Tenant update / rename**. Only `create_tenant` and `get_tenant`
   exist; there is no `PATCH /v1/admin/tenants/:id` for renaming or
   updating metadata. Tenants are effectively immutable after
   creation.
2. **Operator role promotion**. `POST /v1/admin/operators/:id/tenant-roles/:tenant/promote`
   does not exist. Operators cannot be granted `TenantRole::Admin` after creation via the API
   (bootstrap via CAIRN_ADMIN_TOKEN minting is the workaround, but E2E harness tests need
   a reliable endpoint to grant admin roles without god-token). Required for PR-A0a multi-operator
   fixtures.
3. **Quota policy dry-run**. `set_tenant_quota` applies immediately.
   No preview mode to show "this quota would have denied N% of
   last-week's requests." Operators must manually replay audit-log
   to estimate impact.
4. **Operator role edits**. `create_operator_profile` and
   `list_operator_profiles` exist; there is no `PATCH` or role-change
   endpoint. Operators are effectively immutable after creation (except
   TenantRole via the new gap #2 endpoint).

These are **backend gaps that must close before the corresponding UI
pages ship** (a UI that can't mutate the state is pointless).

## v1 Cutoff

### v1 ships (ordered by operator criticality)

1. **Tenants page** — list + create + edit (requires backend PATCH).
   Operator cannot run multi-tenant cairn without this.
2. **Quotas page** — list quotas per tenant, set/edit with current-
   usage preview. Operator cannot enforce commercial limits without
   this.
3. **Operators page** — list + create + edit role (requires backend
   PATCH). Operator cannot delegate to teammates without this.
4. **Retention page** — list + set per-tenant retention policy, run
   apply-retention. Operator cannot comply with data-residency /
   GDPR without this.
5. **API client coverage** — ~29 new exports in `ui/src/lib/api.ts`
   matching the currently-unreachable backend handlers (43 total,
   14 already wired). Prerequisite for any UI page to function.

### v1 defers (slip to v1.1 or later)

6. **Snapshots / event-log compaction UI** — operational tooling;
   workable via `curl` for v1. Low user-facing pain.
7. **Request logs page** — overlaps heavily with AuditLog. Merge into
   AuditLog as a filtered view vs separate page.
8. **Failed notifications retry UI** — retry endpoint exists; for v1
   operators can use the API. Backlog for v1.1.
9. **Quota dry-run preview** — nice-to-have; v1 ships without.
10. **Operator SSO / SAML integration** — out of scope (RFC for this
    separately).

## Build Order + Sequencing

Eight stacked PRs (A0–A6, plus A0a harness prerequisite). **A0 is a prerequisite blocker** for A1–A6. **A0a is a harness prerequisite nested within A0** — lands before A0's acceptance test becomes runnable.

### PR-A0a: Multi-operator Playwright fixtures (prerequisite inside A0)

**Context:** PR-A0's acceptance criteria require proving cross-tenant isolation E2E,
but the Playwright harness has no multi-operator infrastructure. `ui/e2e/helpers.ts:8`
hard-codes `export const TOKEN = "dev-admin-token"`; every `signIn/apiGet/apiPost` uses
that one god token. A0's isolation test ("operator on T gets 403 on T'") needs two
authenticated contexts that don't exist.

**Scope:**

1. **New fixture file** `ui/e2e/fixtures/operators.ts`:
   - `beforeAll` uses god token to mint two operator tokens via `POST /v1/auth/tokens`
   - Promote each to `TenantRole::Admin` on their assigned tenant via the new
     `POST /v1/admin/operators/:id/tenant-roles/:tenant/promote` endpoint (defined in A0).
   - Write two `storageState` files to disk per operator.
   - Export Playwright test-scoped `test.extend` fixtures: `adminOnT` and `adminOnTPrime`.

2. **New E2E spec** `ui/e2e/admin-cross-tenant-isolation.spec.ts`:
   - Assert `adminOnT` receives 200s on `PATCH /v1/admin/tenants/T`.
   - Assert `adminOnT` receives 403s on `PATCH /v1/admin/tenants/T'` (T != T').
   - Land this as A0's gating test — mandatory server-side gate verification (no mocked 403s).

3. **Implementation notes:**
   - Single-token legacy path at `ui/e2e/helpers.ts:8` stays for non-admin specs (~90% of existing tests).
   - Admin specs import from the new `fixtures/operators.ts` exclusively.
   - ~200 LOC total.

**Estimated scope:** ~200 LOC (fixtures + spec).

### PR-A0: Tenant-admin role (prerequisite — blocks A1..A6)

**Context:** RFC-026 assumes a tenant-admin role for the 4 UI pages, but `AdminRoleGuard`
in `extractors.rs:294-300` only passes for `System` and `ServiceAccount{name:"admin"}`.
The `Operator` variant always returns `false`, so UI pages cannot gate themselves to non-god-token
operators. This PR introduces a real tenant-scoped admin role.

#### Upgrade + Backfill

**Problem:** Existing operators in production deployments get zero `TenantRole` entries
and lose all admin UI access on the PR-A0 upgrade. The bootstrapping (below) covers greenfield
("first boot") + god-token promotion, but self-hosted cairn already deployed needs a
backfill from existing `operator_profiles` to preserve operator access on upgrade.

**Solution:**

**Layer A — Data migration backfill (authoritative):**
- When the pg/sqlite migration that adds `operator_tenant_roles` runs, the same migration
  emits one `TenantRoleGranted { tenant_id, operator_id, role: Admin, granted_by: "upgrade-backfill", at_ms: <migration-run-time> }`
  per existing row in `operator_profiles` that holds an admin-capable relationship on that tenant.
- Definition of "admin-capable relationship": every operator with a `workspace_members` role
  `Admin` or `Owner` on any workspace within a tenant gets `TenantRole::Admin` for that tenant.
  (Verify `workspace_members` projection exists: ✓ confirmed in `crates/cairn-store/src/projection_registry.rs` lines 441, 447)
- If no `workspace_members` projection matches an operator but they have a profile, fall back
  to `TenantRole::Member` (not Admin) — preserves presence without granting escalation.
- The backfill emits real `TenantRoleGranted` events to the event log, not raw SQL inserts.
  Projection replay on future boots is correct.

**Layer B — Operator-visible error:**
- When `TenantAdminGuard` rejects a principal that looks like a real operator (not god-token)
  with an empty `operator_tenant_roles` row, the 403 response body must carry a structured error:
  ```json
  {
    "error_code": "tenant_role_missing",
    "tenant_id": "<T>",
    "operator_id": "<O>",
    "hint": "Ask your deployment operator to run `cairn-app admin promote <op> --tenant <T> --role Admin` or set CAIRN_ADMIN_TOKEN and POST /v1/admin/operators/:id/tenant-roles/:tenant/promote"
  }
  ```
- UI `<AdminGate>` when it sees `tenant_role_missing` in a probe response shows an explicit
  "role required" banner, not `<NotFoundPage>`. The nav-hiding only applies when the operator
  genuinely lacks the role after backfill; for upgrade-regression-detection UX, show the clear error.

**Scope:**

1. **Define `TenantRole` enum** in `cairn-domain::tenancy`:
   ```rust
   #[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
   pub enum TenantRole {
       Admin,     // tenant-admin; can perform all admin actions for this tenant
       Member,    // read/member access (no admin actions)
       ReadOnly,  // read-only observer
   }
   ```
   Implementation note: RFC-008 considered elevating `WorkspaceRole::Admin` to tenant-scope
   (option (b)). Option (a) above is preferred for explicitness — `TenantRole` is clearly
   tenant-scoped, avoiding confusion with workspace-scoped roles.

2. **Operator ↔ TenantRole mapping:** Store operator→tenant-role associations in a new
   `operator_tenant_roles` table (N-to-M relationship: one operator can hold distinct
   roles on multiple tenants). This is a separate relational concept from the
   `operator_profiles` read-model (which is per-operator identity). Every TenantRole
   change emits a `TenantRoleGranted` or `TenantRoleRevoked` RuntimeEvent, registered in
   `crates/cairn-store/src/projection_registry.rs` as `Projected { table: "operator_tenant_roles" }`
   to satisfy RFC-025 replay + projection-parity contracts.

3. **Middleware attachment:** Extend `middleware.rs::infer_workspace_role_for_request` to also
   attach `TenantRole` via extensions for every request to `/v1/admin/tenants/:tenant/*` and
   cross-tenant admin routes (e.g., `GET /v1/admin/tenants` — which tenant(s) are eligible?).
   Authorization logic: operator must hold `TenantRole::Admin` for the target tenant.

4. **Extend `AdminRoleGuard`** (or add `TenantAdminGuard` as a new extractor) to accept:
   - `is_admin_principal(principal) == true` (god-token backward-compat), **OR**
   - `TenantRole::Admin` for the target tenant extracted from request context.
   This preserves existing deployments while enabling tenant-admin-only workflows.

5. **Bootstrapping:**
   - On first boot, the system can auto-promote one operator to `TenantRole::Admin`
     for their primary tenant (if an operator profile exists).
   - Alternatively, `CAIRN_ADMIN_TOKEN` can POST to a new endpoint
     `POST /v1/admin/operators/:id/tenant-roles/:tenant/promote` to grant `TenantRole::Admin`
     to a specific operator for a specific tenant. After bootstrapping, the god-token can be rotated.
   - This also addresses Open Question #3 (self-service tenant creation) — solved by "tenant-admin
     creates tenants for their tenant."

6. **Audit log + Event sourcing:** Every `TenantRole` change emits both a `TenantRoleGranted`
   or `TenantRoleRevoked` RuntimeEvent (state-carrying, replayed by the projection system on
   boot) and an `AuditLogEntryRecorded` event (audit trail). The two events are independent:
   RuntimeEvents drive the projection; AuditLogEntryRecorded is the operator-visible audit trail.

7. **RFC-025 compliance (event variants + projection):**
   - Add `TenantRoleGranted` + `TenantRoleRevoked` variants to `crates/cairn-domain/src/events.rs`
     (following the `OperatorProfileCreated/Updated` naming convention)
   - Register both in `crates/cairn-store/src/projection_registry.rs` as `Projected { table: "operator_tenant_roles" }`
   - Create a pg migration (VXX) + sqlite schema.rs update to add the `operator_tenant_roles` table
     (columns: tenant_id PK, operator_id PK, role, granted_at_ms, granted_by, revoked_at_ms, revoked_by)
   - Add pg + sqlite applier methods to insert/update rows in response to the two variants
   - Add in-memory applier to mirror the projection
   - Extend `build.rs` exhaustiveness check to cover the new variants
   - Add projection-parity test in `crates/cairn-store/tests/projection_parity.rs` to prove byte-equality
     across backend reads

**Estimated scope:** ~950–1150 LOC (new domain type, operator-profile attachment, middleware
changes, extractor, event variants + registry entries, migration, projection appliers, backfill migration,
structured error envelopes, UI error-gate banner).

### PR-A1: UI API client admin coverage
- `ui/src/lib/api.ts` — add ~29 functions mapping to currently-unreachable
  admin handlers (closing the 33% → 100% gap)
- Type definitions in `ui/src/lib/types.ts` matching backend response
  shapes
- No new UI pages; this unblocks every downstream PR
- ~350 LOC, mostly type-safe fetch wrappers (less than initially estimated —
  ~29 new exports vs. speculated 42; base set already includes 14)

### PR-A2: Backend gaps (tenant PATCH, operator PATCH)
- `PATCH /v1/admin/tenants/:id` — edit name.
  (tenant `metadata` is deferred — the `tenants` projection table
  has no metadata column, so the event and handler scope is
  name-only until a follow-up migration adds one.)
- `PATCH /v1/admin/tenants/:tenant_id/operator-profiles/:id` —
  edit display_name, email, role.
  (Scoped under `/tenants/:tenant_id/` so `TenantAdminGuard`
  authorizes on the URL tenant without a pre-lookup and the route
  family stays consistent with the existing operator-profile
  create/list routes. A flat `/v1/admin/operators/:id` path was
  considered but rejected because `attach_tenant_role` only
  extracts the target tenant from paths under
  `/v1/admin/tenants/:tenant_id/`.)
- Audit-log both actions
- ~300 LOC

### PR-A3: Tenants page
- `ui/src/pages/TenantsPage.tsx` — list + create + edit
- Role gate: `AdminRoleGuard` equivalent in UI (hide page for
  non-tenant-admins)
- ~500 LOC

### PR-A4: Operators page
- `ui/src/pages/OperatorsPage.tsx` — list + create + role edit
- Same role gate
- ~450 LOC

### PR-A5: Quotas page
- `ui/src/pages/QuotasPage.tsx` — per-tenant quota CRUD + current-
  usage display (backend: `get_tenant_overview` already returns
  usage)
- ~400 LOC

### PR-A6: Retention page
- `ui/src/pages/RetentionPage.tsx` — per-tenant retention policy
  set + manual apply-retention trigger
- ~350 LOC

Total: ~3550–3750 LOC across 8 PRs (A0–A6, with A0 including PR-A0a harness sub-task).
Every PR independently reviewable + shippable.

## Non-Goals

- Cross-tenant operator views (RFC-010 forbids)
- Platform superadmin UI (RFC-008's tenant-admin role is the ceiling)
- Automated admin scripts / CLI (backend API is sufficient for
  scripting; not a UI deliverable)
- Admin-UI internationalization (v1 is English-only per
  `feedback_user_interest_profile.md`)

## Acceptance Criteria

v1 admin is complete when:

- [ ] **PR-A0:** A non-`CAIRN_ADMIN_TOKEN` operator bearing `TenantRole::Admin` for
      tenant T successfully performs all four admin flows against tenant T.
- [ ] **PR-A0 backfill:** An operator running pre-A0 main with `workspace_members.role = Admin`
      on tenant T retains 200-OK access to `PATCH /v1/admin/tenants/T` after PR-A0 upgrade
      (backfill proven via migration test).
- [ ] **PR-A0 backfill E2E test:** Seed `operator_profiles` + `workspace_members` pre-migration;
      run migration; assert operator sees 200 on one admin PATCH against their backfilled tenant.
- [ ] **PR-A0 error-envelope test:** Operator with no `operator_tenant_roles` entry hitting
      any admin endpoint gets `tenant_role_missing` structured error, not a generic 403.
- [ ] **PR-A0a (gating test for A0):** `ui/e2e/admin-cross-tenant-isolation.spec.ts` proves
      cross-tenant isolation E2E. The same operator is rejected with 403 when the same requests
      target tenant T' (T != T'). Server-side gate is mandatory — Playwright route() mocks
      forbidden.
- [ ] A tenant-admin can create a tenant from the UI, edit its
      metadata, and see it in the list.
- [ ] A tenant-admin can create an operator, assign a role, edit the
      role, see the operator in the list.
- [ ] A tenant-admin can set a quota policy for a tenant and see
      current usage against it.
- [ ] A tenant-admin can set a retention policy and trigger apply-
      retention manually.
- [ ] All four pages respect the role gate — non-admin operators see
      404 / no-nav-link, not a 403 after click.
- [ ] E2E Playwright test proves the full admin flow: create tenant
      → add operator → set quota → set retention → apply retention.
- [ ] OpenAPI spec (`openapi_spec.rs`) updated for new PATCH handlers.
- [ ] `ui/src/lib/api.ts` covers all 43 admin.rs handlers (currently 14/43 = 33%).
- [ ] **RFC-025 projection parity:** pg + sqlite + in-memory all rebuild `operator_tenant_roles`
      from event-log replay; proven by a projection-parity test in `crates/cairn-store/tests/projection_parity.rs`.

## Open Questions

1. **UI role gate mechanism** — should the 4 new pages be behind a
   single `<AdminGate>` wrapper component or per-route gating in the
   router? **Resolved (PR-A0):** Server-side gate is authoritative via `TenantAdminGuard`
   (new extractor). UI `<AdminGate>` wrapper mirrors server state for nav-hiding only;
   all authorization enforcement is server-side. Single wrapper, rendered as `<NotFoundPage>`
   for non-admins (matches "no-nav-link" from acceptance criteria).
2. **Tenant soft-delete vs hard-delete** — no `DELETE /v1/admin/tenants/:id`
   exists today. Is adding one v1-scope, or deferred? Proposal: deferred
   to v1.1 (operators work around by renaming "archived-TENANT").
3. **"Self-service tenant creation" for non-admin operators** — if a
   new workspace needs a new tenant, does the operator self-provision
   or request from platform-admin? **Resolved (PR-A0):** Tenant creation is
   tenant-admin-only for their tenant. Bootstrapping (PR-A0 scope) enables the first
   tenant-admin via CAIRN_ADMIN_TOKEN promotion, solving the chicken-egg problem.
   Non-tenant-admins get a "contact your tenant-admin" UX path.

4. **Backfill gating strategy** — should Layer-A backfill run automatically, or gated behind
   `CAIRN_TENANT_ROLES_ENFORCED=true` for one release to let operators opt-in?
   **Proposal:** automatic backfill (safer default — preserves access). Add a WARN-on-boot
   log line listing backfilled operator/tenant pairs for auditability and operator visibility.

## Implementation Notes

- All 8 PRs (A0–A6, plus A0a harness) build on `main` at `d4888521` (PR #608 merge tip as of
  2026-05-02).
- **Playwright harness gains multi-operator fixtures as part of PR-A0a.** Single-token legacy
  path at `ui/e2e/helpers.ts:8` stays for non-admin specs (~90% of existing tests); admin specs
  import from the new `fixtures/operators.ts`. Verify `/v1/auth/tokens` exists before minting
  operators in fixtures (confirmed present in `crates/cairn-app/src/handlers/auth_tokens.rs`).
- Existing admin handler tests stay green; new handlers + pages add
  their own coverage.
- **RFC-025 projection impact:** PR-A0 adds two new `RuntimeEvent` variants
  (`TenantRoleGranted`, `TenantRoleRevoked`) registered as `Projected
  { table: "operator_tenant_roles" }` in `crates/cairn-store/src/projection_registry.rs`.
  A new pg/sqlite migration adds the `operator_tenant_roles` table (columns: tenant_id,
  operator_id, role, granted_at_ms, granted_by, revoked_at_ms, revoked_by). Bootstrapping
  a grant via CAIRN_ADMIN_TOKEN emits `TenantRoleGranted` with `granted_by = "system"`.
  On pg/sqlite boot, the standard replay path rebuilds the table from the event log; on
  in-memory the projection lives in the standard InMemoryStore projection.
- **PR-A0's migration backfill:** The migration includes a backfill step that emits
  `TenantRoleGranted` events per existing admin-capable operator. Backfill uses the existing
  `workspace_members` projection (RFC-025 milestone 1, confirmed at `crates/cairn-store/src/projection_registry.rs:441,447`)
  as the source of truth for operator → tenant admin eligibility.
- No FF coupling — admin is cairn-layer only.
- **Recommended follow-up:** Add a CI check that greps for new `/v1/admin/*`
  routes in `router.rs` and verifies corresponding client methods exist in
  `ui/src/lib/api.ts`. This prevents future drift (current state: 29/43
  handlers missing). Gate on PR review for now; automate in PR-A1 if
  implementation is simple.

## References

- RFC-008 (tenant/workspace/profile) — scope model
- RFC-010 (operator control-plane IA) — admin-as-tenant-settings rule
- `crates/cairn-app/src/handlers/admin.rs` — backend surface
- `ui/src/pages/` — UI coverage
- `ui/src/lib/api.ts` — API client gap
