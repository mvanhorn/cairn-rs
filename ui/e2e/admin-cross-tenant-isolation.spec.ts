/**
 * RFC 026 PR-A0a — browser-facing proof of the tenant-admin server-side
 * gate.
 *
 * PR-A0 shipped `TenantRole::Admin` + the `operator_tenant_roles`
 * projection + promote/revoke endpoints. The acceptance criterion
 * "operator bearing TenantRole::Admin on T succeeds against T, fails
 * with 403 against T'" is proven at the Rust layer by
 * `crates/cairn-app/tests/test_tenant_role_promote_revoke.rs`. This
 * spec is the browser-transport mirror: hit the same endpoints through
 * `APIRequestContext` (Playwright's fetch) to catch regressions that
 * would only surface via the HTTP headers/cookies/CORS path the admin
 * UI (PR-A1..A6) will ride on.
 *
 * NO `route()` mocks. Real server-side gate proof — every assertion
 * runs against the live cairn-app managed by `playwright.config.ts`.
 *
 * Scope is intentionally tight: two operators on two tenants, three
 * assertions. Broader coverage lives in the Rust suite; this spec
 * guards the UI wire contract.
 */
import { expect } from "@playwright/test";

import { multiOperatorTest } from "./fixtures/operators";

const TENANT_ROLE_MISSING = "tenant_role_missing";

multiOperatorTest(
  "admin-on-T can promote on T (201 CREATED)",
  async ({ request, adminOnT }) => {
    // Operator A, holding TenantRole::Admin on tenant T, delegates the
    // role to a second operator on the SAME tenant. The server-side
    // gate (`TenantAdminGuard`) admits the request because A's row in
    // `operator_tenant_roles` is active + Admin for T.
    const targetOperator = `${adminOnT.operatorId}_delegate`;
    const res = await request.post(
      `/v1/admin/operators/${targetOperator}/tenant-roles/${adminOnT.tenantId}/promote`,
      {
        headers: { Authorization: `Bearer ${adminOnT.token}` },
        data: { role: "member" },
      },
    );
    expect(
      res.status(),
      `same-tenant delegate promote must 201; body=${await res.text()}`,
    ).toBe(201);
  },
);

multiOperatorTest(
  "admin-on-T gets 403 tenant_role_missing on T'",
  async ({ request, adminOnT, adminOnTPrime }) => {
    // Cross-tenant: operator A (admin on T) attempts to promote any
    // operator on T'. The gate rejects with the structured
    // `tenant_role_missing` envelope — distinct from the canonical
    // `{status_code, code, message, request_id}` shape so the UI
    // `<AdminGate>` wrapper can surface an actionable hint rather than a
    // bare 403.
    // Derive the target operator id from `adminOnTPrime.operatorId` (which
    // already carries a per-worker uuid suffix) so a server with persistent
    // state cannot end up with collisions across reruns. Matches the
    // idempotency contract of the fixture itself.
    const targetOperator = `victim_${adminOnTPrime.operatorId}`;
    const res = await request.post(
      `/v1/admin/operators/${targetOperator}/tenant-roles/${adminOnTPrime.tenantId}/promote`,
      {
        headers: { Authorization: `Bearer ${adminOnT.token}` },
        data: { role: "admin" },
      },
    );
    expect(res.status(), "cross-tenant promote must 403").toBe(403);

    const body = await res.json();
    expect(
      body.error_code,
      `cross-tenant 403 body must carry structured error_code; body=${JSON.stringify(body)}`,
    ).toBe(TENANT_ROLE_MISSING);
    expect(
      body.tenant_id,
      `403 body must echo the REQUESTED tenant_id (T', not the caller's home); body=${JSON.stringify(body)}`,
    ).toBe(adminOnTPrime.tenantId);
    expect(
      body.operator_id,
      `403 body must echo the CALLER operator_id; body=${JSON.stringify(body)}`,
    ).toBe(adminOnT.operatorId);
    expect(
      typeof body.hint === "string" && body.hint.length > 0,
      `403 body must include a remediation hint; body=${JSON.stringify(body)}`,
    ).toBe(true);
  },
);

multiOperatorTest(
  "admin-on-T' can promote on T' (201 CREATED) — symmetric proof",
  async ({ request, adminOnTPrime }) => {
    // Mirror of the first assertion from the T' side so a regression
    // that accidentally grants B cross-tenant access would also surface
    // (a symmetric pair is cheaper than listing every pair in a matrix).
    const targetOperator = `${adminOnTPrime.operatorId}_delegate`;
    const res = await request.post(
      `/v1/admin/operators/${targetOperator}/tenant-roles/${adminOnTPrime.tenantId}/promote`,
      {
        headers: { Authorization: `Bearer ${adminOnTPrime.token}` },
        data: { role: "member" },
      },
    );
    expect(
      res.status(),
      `same-tenant delegate promote on T' must 201; body=${await res.text()}`,
    ).toBe(201);
  },
);
