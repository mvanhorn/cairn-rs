/**
 * RFC 026 PR-A0a — multi-operator Playwright fixture.
 *
 * Every spec in this repo has historically shared one god-token
 * (`TOKEN = "dev-admin-token"` in `../helpers.ts`). PR-A0 introduced
 * per-tenant admin roles (`operator_tenant_roles` projection +
 * `TenantAdminGuard`), so proving the acceptance criterion
 *
 *   "operator bearing TenantRole::Admin on T succeeds against T,
 *    fails with 403 against T'"
 *
 * at the browser-transport layer requires at least two operators,
 * each holding Admin on a distinct tenant. This module exposes a
 * `multiOperatorTest` built on `test.extend` that mints exactly that
 * pair on demand.
 *
 * Design:
 *
 *   * Worker-scoped fixtures (`scope: "worker"`) — the tenant, operator
 *     id, and token are created once per Playwright worker process and
 *     reused by every test in that worker. Cheaper than per-test and
 *     safe because every id is uuid-scoped, so even parallel workers
 *     never collide.
 *
 *   * Idempotent on replay: each fixture mints operator ids that embed
 *     a fresh uuid, so re-runs land new rows in `operator_tenant_roles`
 *     rather than stepping on prior ones. The Rust service layer
 *     accepts re-promotes as upserts, so a retry after a failed run
 *     converges too.
 *
 *   * No UI touch — the fixture is API-only (Playwright's
 *     `APIRequestContext`). Specs that need browser-level storageState
 *     can layer on top; PR-A0a does not exercise that path so we keep
 *     the surface minimal per RFC-026 §PR-A0a.
 */
import {
  test as base,
  expect,
  request as requestFactory,
  type APIRequestContext,
} from "@playwright/test";

import { BASE, TOKEN, uid } from "../helpers";

/** Public shape of one operator identity. Consumed by the spec. */
export interface OperatorIdentity {
  /** Bearer token minted off the god-token bootstrap path. */
  readonly token: string;
  /** The `operator_id` encoded in the operator principal. */
  readonly operatorId: string;
  /** The tenant on which this operator holds `TenantRole::Admin`. */
  readonly tenantId: string;
}

/**
 * Mint a fresh operator token via `POST /v1/auth/tokens`.
 *
 * The admin-service-account (god token) is the only principal allowed
 * to create operator tokens — matches
 * `crates/cairn-app/src/handlers/auth_tokens.rs::create_auth_token_handler`.
 * The response `token` field is the raw bearer.
 */
async function mintOperatorToken(
  request: APIRequestContext,
  operatorId: string,
  tenantId: string,
): Promise<string> {
  const res = await request.post(`${BASE}/v1/auth/tokens`, {
    headers: {
      Authorization: `Bearer ${TOKEN}`,
      "Content-Type": "application/json",
    },
    data: {
      operator_id: operatorId,
      tenant_id: tenantId,
      name: `pr-a0a-fixture-${operatorId}`,
    },
  });
  expect(
    res.status(),
    `operator-token mint must 201; body=${await res.text()}`,
  ).toBe(201);
  const body = (await res.json()) as { token?: string };
  const token = body.token;
  if (typeof token !== "string" || token.length === 0) {
    throw new Error(
      `operator-token mint returned empty token; body=${JSON.stringify(body)}`,
    );
  }
  return token;
}

/**
 * Grant `TenantRole::Admin` to `operatorId` on `tenantId`, signed with
 * the god token so the `TenantAdminGuard` bootstrap path is taken.
 *
 * Matches `crates/cairn-app/src/handlers/admin.rs::promote_tenant_role_handler`.
 * 201 on first grant; 201 on re-grant (service-layer upsert) — both
 * shapes are accepted so a re-run after partial setup converges.
 */
async function promoteToAdmin(
  request: APIRequestContext,
  operatorId: string,
  tenantId: string,
): Promise<void> {
  const res = await request.post(
    `${BASE}/v1/admin/operators/${operatorId}/tenant-roles/${tenantId}/promote`,
    {
      headers: {
        Authorization: `Bearer ${TOKEN}`,
        "Content-Type": "application/json",
      },
      data: { role: "admin" },
    },
  );
  expect(
    res.status(),
    `god-token promote must 201; body=${await res.text()}`,
  ).toBe(201);
}

/**
 * Provision one operator identity end-to-end:
 *
 *   1. Mint an operator token scoped to `(operatorId, tenantId)`.
 *   2. Promote that operator to `TenantRole::Admin` on the tenant.
 *
 * Returns the tuple the spec uses to make admin-gated requests on
 * behalf of the operator.
 *
 * Both calls use the god token (`TOKEN`) — the operator's own token is
 * never used for setup, only for the assertions the spec runs.
 */
async function provisionAdmin(
  request: APIRequestContext,
  operatorId: string,
  tenantId: string,
): Promise<OperatorIdentity> {
  // Promote BEFORE minting the token so that when the operator's first
  // request lands at `attach_tenant_role` middleware, the projection
  // row already exists. Order matters: the middleware reads
  // `operator_tenant_roles` synchronously (no retry) and a race where
  // the token is issued before the grant lands would surface as a
  // spurious 403 on the first operator call.
  await promoteToAdmin(request, operatorId, tenantId);
  const token = await mintOperatorToken(request, operatorId, tenantId);
  return { token, operatorId, tenantId };
}

/** Test fixture type signature — exposes the two operator identities. */
export interface MultiOperatorFixtures {
  adminOnT: OperatorIdentity;
  adminOnTPrime: OperatorIdentity;
}

/**
 * Build the per-worker fixture-callback that provisions one operator
 * identity.
 *
 * Playwright fixture signatures require `(deps, use) => ...` where
 * `use` is the yield callback, not a React hook. The
 * `react-hooks/rules-of-hooks` lint rule flags any identifier starting
 * with `use` used inside a try/catch, so we build the callback as a
 * standalone function using an aliased `yieldTo` name, then hand it to
 * `base.extend` with the required `use` destructure at the call site
 * (where the try/catch does not wrap the call).
 *
 * Each worker needs a per-worker `APIRequestContext` — Playwright does
 * not provide `request` at worker scope by default, so we build one
 * from the statically-imported `requestFactory` (aliased from
 * `@playwright/test`'s `request` to avoid shadowing the function
 * parameters that carry `APIRequestContext` values elsewhere in this
 * file).
 */
function makeAdminFixture(operatorPrefix: string, tenantPrefix: string) {
  return async (yieldTo: (id: OperatorIdentity) => Promise<void>) => {
    const api = await requestFactory.newContext({ baseURL: BASE });
    try {
      const suffix = uid();
      const identity = await provisionAdmin(
        api,
        `${operatorPrefix}_${suffix}`,
        `${tenantPrefix}_${suffix}`,
      );
      await yieldTo(identity);
    } finally {
      await api.dispose();
    }
  };
}

/**
 * Drop-in replacement for `test` that additionally provides
 * `adminOnT` and `adminOnTPrime` fixtures. Both are worker-scoped so
 * provisioning cost is paid once per worker process, not once per
 * test.
 *
 * Usage (see `../admin-cross-tenant-isolation.spec.ts`):
 *
 *     multiOperatorTest("...", async ({ request, adminOnT }) => { ... })
 */
export const multiOperatorTest = base.extend<
  Record<string, never>,
  MultiOperatorFixtures
>({
  // Playwright requires the first arg to be an object destructure even
  // when empty — `no-empty-pattern` is disabled on these two lines
  // only, rather than repo-wide.
  adminOnT: [
    // eslint-disable-next-line no-empty-pattern
    async ({}, use) => {
      await makeAdminFixture("pw_admin_t", "pw_tenant_t")(use);
    },
    { scope: "worker" },
  ],
  adminOnTPrime: [
    // eslint-disable-next-line no-empty-pattern
    async ({}, use) => {
      await makeAdminFixture("pw_admin_tp", "pw_tenant_tp")(use);
    },
    { scope: "worker" },
  ],
});
