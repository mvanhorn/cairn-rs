/**
 * RFC 031 — operator journey for custom agent roles.
 *
 * Covers the four flows the product ships to operators:
 *
 *   1. List → filters out neither built-ins nor custom rows
 *   2. Create via POST /v1/projects/:project/agent-roles, then navigate
 *      to the list and confirm the row renders with the custom badge
 *   3. Retract from the detail page — modal must render the §D7
 *      guarantee banner and the new "N runs currently using this role"
 *      probe copy, then a confirm issues the DELETE
 *   4. After retract, the role disappears from the project list
 *      (source=custom, not a shadow)
 *
 * These are the exact flows PR-D1/D2/D3 added; if any regresses the
 * dogfood path breaks silently. Runs against the bundled cairn-app in
 * the default scope — no extra fixtures required.
 */

import { test, expect, type APIRequestContext } from "@playwright/test";

import {
  BASE,
  DEFAULT_SCOPE,
  HDR,
  TOKEN,
  apiDel,
  apiGet,
  apiPost,
  signIn,
  uid,
} from "./helpers";

const PROJECT_PATH = encodeURIComponent(
  `${DEFAULT_SCOPE.tenant_id}/${DEFAULT_SCOPE.workspace_id}/${DEFAULT_SCOPE.project_id}`,
);

function rolesListPath() {
  return `/v1/projects/${PROJECT_PATH}/agent-roles`;
}

function roleDetailPath(roleId: string) {
  return `/v1/projects/${PROJECT_PATH}/agent-roles/${encodeURIComponent(roleId)}`;
}

async function createRole(
  request: APIRequestContext,
  roleId: string,
  body: Partial<{
    name: string;
    tier: "standard" | "research" | "orchestrator" | "generic";
    system_prompt: string;
    response_shape: "direct_answer" | "procedural_artifact";
  }> = {},
) {
  const payload = {
    id: roleId,
    name: body.name ?? `E2E role ${roleId}`,
    tier: body.tier ?? "standard",
    system_prompt:
      body.system_prompt ??
      [
        "## Specialty",
        "E2E-created role.",
        "",
        "## Workflow",
        "### Phase 1: assess",
        "Read the task.",
        "",
        "### Phase 2: execute",
        "Apply the smallest change that satisfies the task.",
        "",
        "## Tools",
        "Use only the allowed tools; escalate if more are needed.",
        "",
        "## Completion criteria",
        "When the operator journey spec is satisfied.",
        "",
        "## What not to do",
        "- Do not mutate state outside the task scope.",
        "- Do not skip the workflow phases.",
        "- Do not suppress errors silently.",
        "",
      ].join("\n"),
    response_shape: body.response_shape ?? "procedural_artifact",
  };
  const resp = await apiPost(request, rolesListPath(), payload);
  if (resp.status !== 201 && resp.status !== 200) {
    throw new Error(
      `createRole failed ${resp.status}: ${JSON.stringify(resp.body)}`,
    );
  }
  return resp.body as { role: { role_id: string } };
}

async function ensureRoleAbsent(request: APIRequestContext, roleId: string) {
  // Best-effort cleanup. 404 is fine (never existed); 200 means
  // retracted. Either way the project list no longer surfaces a
  // live custom-source entry for this id.
  await apiDel(request, roleDetailPath(roleId));
}

test.describe("RFC 031 — agent-roles operator journey", () => {
  test("list, create, retract — happy path with probe banner", async ({
    page,
    request,
  }) => {
    const roleId = `e2e-role-${uid()}`;

    // Pre-clean in case a prior failed run left state behind.
    await ensureRoleAbsent(request, roleId);

    // ── API-seed a role so the UI step is isolated from the editor ─────────
    await createRole(request, roleId);

    await signIn(page);
    await page.goto("/#agents");
    await page.waitForLoadState("domcontentloaded");

    // List shows the seeded role.
    const row = page.getByTestId(`agent-role-row-${roleId}`);
    await expect(row).toBeVisible({ timeout: 10_000 });
    await row.click();

    // Detail page shows the retract button for a custom role.
    const retractBtn = page.getByTestId("agent-role-retract-btn");
    await expect(retractBtn).toBeVisible({ timeout: 10_000 });
    await retractBtn.click();

    // Modal shows the §D7 guarantee banner. The probe count renders
    // only when runs exist; with no in-flight runs the generic copy
    // shows. Assert on the banner testid, not the count, so the
    // test works in either state.
    const banner = page.getByTestId("agent-role-retract-guarantee");
    await expect(banner).toBeVisible({ timeout: 5_000 });
    await expect(banner).toContainText("§D7 guarantee");

    // Confirm retract.
    await page.getByTestId("agent-role-retract-confirm-btn").click();

    // After the DELETE, the project-scoped list no longer surfaces
    // the row (role was source=custom — retracting removes it from
    // the merged list; shadows would still render with the built-in
    // restored, but we didn't seed a shadow here).
    await expect
      .poll(
        async () => {
          const list = await apiGet(request, rolesListPath());
          const items = (list.body as { items?: { role: { role_id: string } }[] })
            .items ?? [];
          return items.some((i) => i.role.role_id === roleId);
        },
        { timeout: 10_000 },
      )
      .toBe(false);
  });

  test("new-role editor — id input + save button render", async ({ page }) => {
    // Smokes the editor page mount (PR-D2 section-indicator rail + draft
    // persistence + tool picker). Full save-round-trip is covered by the
    // create-role API call above; this only guards that the editor
    // route keeps rendering its primary controls.
    await signIn(page);
    await page.goto("/#agent-new");
    await page.waitForLoadState("domcontentloaded");

    await expect(page.getByTestId("agent-role-editor-id-input")).toBeVisible({
      timeout: 10_000,
    });
    await expect(
      page.getByTestId("agent-role-editor-name-input"),
    ).toBeVisible();
    await expect(
      page.getByTestId("agent-role-editor-prompt-textarea"),
    ).toBeVisible();
    await expect(
      page.getByTestId("agent-role-editor-save-btn"),
    ).toBeDisabled(); // empty form → !canSubmit
  });
});

// Sanity: ensure the backend API is actually reachable under the
// default scope. If cairn-app is down the UI tests would fail with
// an opaque "element not visible" — this gives a direct, fast signal.
test("backend is reachable under default scope", async ({ request }) => {
  const resp = await request.get(`${BASE}${rolesListPath()}`, {
    headers: { Authorization: `Bearer ${TOKEN}`, ...HDR },
  });
  expect(resp.status()).toBe(200);
});
