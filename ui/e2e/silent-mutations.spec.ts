/**
 * Silent-failure mutations — closes #373 / #374 / #375 / #376.
 *
 * Four critical "same-shape-as-#253" bugs: mutations that had no `onError`
 * handler, so a 403 / 409 / 500 from the backend produced no operator
 * feedback. These tests lock in the fix by stubbing the mutation endpoint
 * to a realistic failure and asserting:
 *
 *   1. A red error toast (role=alert) appears with a non-empty message.
 *   2. The originating UI element is still actionable — the dialog stays
 *      open, the button re-enables, the form retains its input — so the
 *      operator can retry without re-typing.
 *
 * A happy-path companion test is included for each mutation where feasible
 * to guard against the inverse regression (the error handler accidentally
 * firing on 2xx).
 *
 * All network stubs go through `page.route()`; `serviceWorkers: "block"`
 * is required because `ui/src/lib/registerSW.ts` registers a cache-first
 * SW that would otherwise swallow the mocks (see `rundetail-orchestrate.spec.ts`
 * for the precedent).
 */
import { test, expect, type Page } from "@playwright/test";
import { signIn, nav, apiPost, uid, DEFAULT_SCOPE } from "./helpers";

test.use({ actionTimeout: 10_000, serviceWorkers: "block" });

// Pin scope to DEFAULT_SCOPE in localStorage BEFORE the app boots. Without
// this, a fresh browser context lands on `useBootstrapScope` → `needs-pick`
// when multiple tenants exist in the Valkey-backed dev instance, and the
// TenantSelector auto-opens itself. Its dropdown overlay intercepts clicks
// on plugin-tab-registered / plugin-register-open-btn. Seeding the scope
// short-circuits the bootstrap resolver to the "cached" branch.
test.beforeEach(async ({ page }) => {
  await page.addInitScript((scope) => {
    try {
      localStorage.setItem("cairn_scope", JSON.stringify(scope));
    } catch {
      /* storage quota / private mode — tests will fail loudly downstream */
    }
  }, DEFAULT_SCOPE);
});

// ── Shared fixtures ─────────────────────────────────────────────────────────

/**
 * Create a session + plan-mode run via the real API, then append a
 * synthetic `plan_proposed` event so `PlanArtifactPanel` will render the
 * Approve/Reject/Revise buttons on RunDetailPage.
 *
 * Plan mode is a run-level attribute; we use `mode: "plan"` in the run
 * creation payload, but the panel also renders when any `plan_proposed`
 * event exists (via the `hasPlan` fallback path), so stamping the event
 * is the belt-and-suspenders option and is how the Rust tests prime
 * RFC 018 fixtures too.
 */
async function createPlanRun(request: Parameters<typeof apiPost>[0]): Promise<string> {
  const sid = `plan_sess_${uid()}`;
  const rid = `plan_run_${uid()}`;
  const sess = await apiPost(request, "/v1/sessions", { session_id: sid, ...DEFAULT_SCOPE });
  expect(sess.status, `POST /v1/sessions failed: ${JSON.stringify(sess.body)}`).toBeGreaterThanOrEqual(200);
  expect(sess.status).toBeLessThan(300);
  const run = await apiPost(request, "/v1/runs", {
    session_id: sid,
    run_id: rid,
    mode: { type: "plan" },
    ...DEFAULT_SCOPE,
  });
  expect(run.status, `POST /v1/runs failed: ${JSON.stringify(run.body)}`).toBeGreaterThanOrEqual(200);
  expect(run.status).toBeLessThan(300);

  // Append plan_proposed so the panel renders with Approve/Reject/Revise.
  // Domain shape (`cairn_domain::PlanProposed`) requires session_id; the
  // RuntimeEvent enum serde-renames to snake_case with the "event" tag.
  const envelope = [{
    event_id: `evt_planprop_${uid()}`,
    source: { source_type: "runtime" },
    ownership: { scope: "project", ...DEFAULT_SCOPE },
    causation_id: null,
    correlation_id: null,
    payload: {
      event: "plan_proposed",
      project: { ...DEFAULT_SCOPE },
      plan_run_id: rid,
      session_id: sid,
      plan_markdown: "# Test plan\n- step 1\n- step 2",
      proposed_at: Date.now(),
    },
  }];
  const appended = await apiPost(request, "/v1/events/append", envelope);
  expect(
    appended.status,
    `append plan_proposed failed: ${JSON.stringify(appended.body)}`,
  ).toBe(201);

  return rid;
}

async function openRunDetail(page: Page, runId: string) {
  await nav(page, `run/${runId}`);
  // Plan panel may take a beat to render after events load. Wait for a
  // button inside it rather than the panel itself — the panel has no
  // stable role, but the Approve button is tagged with data-testid.
  const approveBtn = page.getByTestId("plan-approve-btn");
  await expect(approveBtn).toBeVisible({ timeout: 10_000 });
  return approveBtn;
}

/**
 * Dismiss the TenantSelector scope popover if it auto-opened on mount.
 *
 * `useBootstrapScope` resolves to `needs-pick` whenever multiple tenants
 * exist in the backing store AND the cached scope equals DEFAULT_SCOPE —
 * our `addInitScript` hits that second condition exactly, so on a dev
 * instance with extra tenants the popover auto-opens and its <select>
 * overlay intercepts clicks on the PluginsPage toolbar tabs. We press
 * Escape (global listener on the popover) to close it. Idempotent — safe
 * to call when the popover is already closed.
 */
async function dismissScopePopoverIfOpen(page: Page) {
  const popover = page.getByTestId("scope-popover");
  if (await popover.isVisible().catch(() => false)) {
    // Escape closes the popover via the selector's keydown handler. If
    // that ever changes, the explicit close button
    // ("Close scope selector" aria-label) is a fallback.
    await page.keyboard.press("Escape");
    await expect(popover).not.toBeVisible({ timeout: 2_000 });
  }
}

// ── #373 — Plan approve/reject/revise ───────────────────────────────────────

test.describe("RunDetailPage — plan approve/reject/revise (#373)", () => {
  test("approve 403 → error toast surfaces + button re-enabled", async ({ page, request }) => {
    const rid = await createPlanRun(request);

    await page.route(
      (url) => url.pathname.endsWith(`/v1/runs/${rid}/approve`),
      async (route) => {
        await route.fulfill({
          status: 403,
          contentType: "application/json",
          body: JSON.stringify({
            code: "forbidden",
            message: "plan-373-sentinel: insufficient role for plan approval",
          }),
        });
      },
    );

    await signIn(page);
    const approveBtn = await openRunDetail(page, rid);
    await approveBtn.click();

    const toast = page.getByRole("alert").filter({ hasText: /plan-373-sentinel|Failed to approve plan/ });
    await expect(toast).toBeVisible({ timeout: 5_000 });
    // Button re-enables so operator can retry or escalate.
    await expect(approveBtn).toBeEnabled();
    await expect(approveBtn).toHaveAttribute("data-pending", "false");
  });

  test("approve 200 → success toast", async ({ page, request }) => {
    const rid = await createPlanRun(request);

    await page.route(
      (url) => url.pathname.endsWith(`/v1/runs/${rid}/approve`),
      async (route) => {
        await route.fulfill({
          status: 200,
          contentType: "application/json",
          body: JSON.stringify({
            plan_run_id: rid,
            status: "approved",
            next_step: "create_execute_run",
          }),
        });
      },
    );

    await signIn(page);
    const approveBtn = await openRunDetail(page, rid);
    await approveBtn.click();

    const toast = page.getByRole("alert").filter({ hasText: /Plan approved/i });
    await expect(toast).toBeVisible({ timeout: 5_000 });
  });

  test("reject 409 → error toast + reject form stays open with reason preserved", async ({ page, request }) => {
    const rid = await createPlanRun(request);

    await page.route(
      (url) => url.pathname.endsWith(`/v1/runs/${rid}/reject`),
      async (route) => {
        await route.fulfill({
          status: 409,
          contentType: "application/json",
          body: JSON.stringify({
            code: "plan_already_decided",
            message: "plan-reject-sentinel: this plan has already been approved",
          }),
        });
      },
    );

    await signIn(page);
    await openRunDetail(page, rid);

    // Open the reject form, type a reason, submit.
    await page.getByTestId("plan-reject-open-btn").click();
    const reasonField = page.getByTestId("plan-reject-reason");
    await expect(reasonField).toBeVisible();
    await reasonField.fill("plan seems incomplete — need more test coverage detail");
    await page.getByTestId("plan-reject-confirm-btn").click();

    const toast = page.getByRole("alert").filter({ hasText: /plan-reject-sentinel|Failed to reject plan/ });
    await expect(toast).toBeVisible({ timeout: 5_000 });
    // Form still open — operator shouldn't have to retype their reason.
    await expect(page.getByTestId("plan-reject-form")).toBeVisible();
    await expect(reasonField).toHaveValue("plan seems incomplete — need more test coverage detail");
  });

  test("revise 500 → error toast + revise form stays open with comments preserved", async ({ page, request }) => {
    const rid = await createPlanRun(request);

    await page.route(
      (url) => url.pathname.endsWith(`/v1/runs/${rid}/revise`),
      async (route) => {
        await route.fulfill({
          status: 500,
          contentType: "application/json",
          body: JSON.stringify({
            code: "store_error",
            message: "plan-revise-sentinel: event append failed",
          }),
        });
      },
    );

    await signIn(page);
    await openRunDetail(page, rid);

    await page.getByTestId("plan-revise-open-btn").click();
    const commentsField = page.getByTestId("plan-revise-comments");
    await expect(commentsField).toBeVisible();
    await commentsField.fill("please add rollback plan for step 3");
    await page.getByTestId("plan-revise-confirm-btn").click();

    const toast = page.getByRole("alert").filter({ hasText: /plan-revise-sentinel|Failed to request revision/ });
    await expect(toast).toBeVisible({ timeout: 5_000 });
    await expect(page.getByTestId("plan-revise-form")).toBeVisible();
    await expect(commentsField).toHaveValue("please add rollback plan for step 3");
  });
});

// ── #374 — Plugin register + provide credentials ───────────────────────────

test.describe("PluginsPage — register (#374)", () => {
  test("register 403 → error toast + modal stays open", async ({ page }) => {
    await page.route(
      (url) => url.pathname.endsWith("/v1/plugins") && url.pathname.split("/").length === 3,
      async (route, req) => {
        if (req.method() !== "POST") return route.continue();
        await route.fulfill({
          status: 403,
          contentType: "application/json",
          body: JSON.stringify({
            code: "forbidden",
            message: "register-374-sentinel: missing plugin admin role",
          }),
        });
      },
    );

    await signIn(page);
    await nav(page, "plugins");
    await dismissScopePopoverIfOpen(page);
    // Switch to the Registered tab so the "Register Plugin" button is
    // visible in the toolbar (Marketplace tab hides it).
    await page.getByTestId("plugin-tab-registered").click();
    await page.getByTestId("plugin-register-open-btn").click();
    const submit = page.getByTestId("plugin-register-submit-btn");
    await expect(submit).toBeVisible();
    await submit.click();

    const toast = page.getByRole("alert").filter({ hasText: /register-374-sentinel|Failed to register plugin/ });
    await expect(toast).toBeVisible({ timeout: 5_000 });
    // Modal stays open so the operator can edit the manifest and retry.
    await expect(submit).toBeVisible();
    await expect(submit).toBeEnabled();
    await expect(submit).toHaveAttribute("data-pending", "false");
  });
});

// ── #375 — Credential revoke ───────────────────────────────────────────────

test.describe("CredentialsPage — revoke (#375)", () => {
  async function createCredential(request: Parameters<typeof apiPost>[0]): Promise<{
    id: string;
    tenantId: string;
  }> {
    const tenantId = DEFAULT_SCOPE.tenant_id;
    const body = {
      tenant_id: tenantId,
      provider_id: `prov_${uid()}`,
      credential_type: "api_key",
      plaintext_value: "not-a-real-key",
    };
    const resp = await apiPost(
      request,
      `/v1/admin/tenants/${encodeURIComponent(tenantId)}/credentials`,
      body,
    );
    expect(resp.status, `create credential failed: ${JSON.stringify(resp.body)}`).toBeGreaterThanOrEqual(200);
    expect(resp.status).toBeLessThan(300);
    const created = resp.body as { id: string };
    return { id: created.id, tenantId };
  }

  test("revoke 403 → error toast + dialog stays open", async ({ page, request }) => {
    const { id, tenantId } = await createCredential(request);

    await page.route(
      (url) =>
        url.pathname.endsWith(
          `/v1/admin/tenants/${tenantId}/credentials/${id}`,
        ),
      async (route, req) => {
        if (req.method() !== "DELETE") return route.continue();
        await route.fulfill({
          status: 403,
          contentType: "application/json",
          body: JSON.stringify({
            code: "forbidden",
            message: "revoke-375-sentinel: credentials admin role required",
          }),
        });
      },
    );

    await signIn(page);
    await nav(page, "credentials");
    await dismissScopePopoverIfOpen(page);

    const rowBtn = page.getByTestId(`credential-revoke-btn-${id}`);
    await expect(rowBtn).toBeVisible({ timeout: 10_000 });
    await rowBtn.click();

    const confirmBtn = page.getByTestId("credential-revoke-confirm-btn");
    await expect(confirmBtn).toBeVisible();
    await confirmBtn.click();

    const toast = page.getByRole("alert").filter({ hasText: /revoke-375-sentinel|Failed to revoke credential/ });
    await expect(toast).toBeVisible({ timeout: 5_000 });
    // Dialog still open — operator can retry or cancel. Pre-fix, the
    // dialog would stay open because `setRevokeTarget(null)` only ran
    // on success, but there was also no feedback at all.
    await expect(page.getByTestId("credential-revoke-dialog")).toBeVisible();
    await expect(confirmBtn).toBeEnabled();
    await expect(confirmBtn).toHaveAttribute("data-pending", "false");
  });
});

// ── #376 — Plugin unregister ───────────────────────────────────────────────

test.describe("PluginsPage — unregister (#376)", () => {
  test("unregister 409 → error toast + card still present", async ({ page, request }) => {
    // Register a real plugin so the card shows up. We need a manifest
    // with at least `id`, `name`, `version`, `command`, and
    // `execution_class`.
    const pluginId = `test_unreg_${uid()}`;
    const manifest = {
      id: pluginId,
      name: `Test plugin ${pluginId}`,
      version: "0.0.1",
      command: ["/bin/echo"],
      capabilities: [{ type: "tool_provider", tools: [] }],
      permissions: { permissions: [] },
      execution_class: "sandboxed_process",
    };
    const resp = await apiPost(request, "/v1/plugins", manifest);
    expect(resp.status, `register plugin failed: ${JSON.stringify(resp.body)}`).toBeGreaterThanOrEqual(200);
    expect(resp.status).toBeLessThan(300);

    // Stub the DELETE to return 409.
    await page.route(
      (url) => url.pathname.endsWith(`/v1/plugins/${pluginId}`),
      async (route, req) => {
        if (req.method() !== "DELETE") return route.continue();
        await route.fulfill({
          status: 409,
          contentType: "application/json",
          body: JSON.stringify({
            code: "plugin_in_use",
            message: "unreg-376-sentinel: plugin is currently serving tool calls",
          }),
        });
      },
    );

    await signIn(page);
    await nav(page, "plugins");
    await dismissScopePopoverIfOpen(page);
    await page.getByTestId("plugin-tab-registered").click();

    const btn = page.getByTestId(`plugin-unregister-btn-${pluginId}`);
    await expect(btn).toBeVisible({ timeout: 10_000 });
    await btn.click();

    const toast = page.getByRole("alert").filter({ hasText: /unreg-376-sentinel|Failed to unregister plugin/ });
    await expect(toast).toBeVisible({ timeout: 5_000 });
    // Button still there — invalidateQueries didn't wrongly hide the row.
    await expect(btn).toBeVisible();
  });

  test("unregister 200 → success toast", async ({ page, request }) => {
    const pluginId = `test_unreg_ok_${uid()}`;
    const manifest = {
      id: pluginId,
      name: `Test ok ${pluginId}`,
      version: "0.0.1",
      command: ["/bin/echo"],
      capabilities: [{ type: "tool_provider", tools: [] }],
      permissions: { permissions: [] },
      execution_class: "sandboxed_process",
    };
    const resp = await apiPost(request, "/v1/plugins", manifest);
    expect(resp.status).toBeGreaterThanOrEqual(200);
    expect(resp.status).toBeLessThan(300);

    // Stub the DELETE to 200 so the test doesn't depend on the real
    // lifecycle (which may need more fixture setup).
    await page.route(
      (url) => url.pathname.endsWith(`/v1/plugins/${pluginId}`),
      async (route, req) => {
        if (req.method() !== "DELETE") return route.continue();
        await route.fulfill({
          status: 200,
          contentType: "application/json",
          body: JSON.stringify({ ok: true }),
        });
      },
    );

    await signIn(page);
    await nav(page, "plugins");
    await dismissScopePopoverIfOpen(page);
    await page.getByTestId("plugin-tab-registered").click();

    const btn = page.getByTestId(`plugin-unregister-btn-${pluginId}`);
    await expect(btn).toBeVisible({ timeout: 10_000 });
    await btn.click();

    const toast = page.getByRole("alert").filter({ hasText: /Plugin unregistered/i });
    await expect(toast).toBeVisible({ timeout: 5_000 });
  });
});
