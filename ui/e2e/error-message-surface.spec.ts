/**
 * Surface-backend-error-message tests — closes #377 / #378 / #379 / #380 / #381 / #382.
 *
 * Same class of bug as #373-#376 (covered in `silent-mutations.spec.ts`),
 * but the shape is slightly different: these mutations DID have an
 * `onError` handler — they just threw away the backend message and
 * printed an opaque "Failed to X" string. The operator couldn't tell
 * a 403 (missing role) from a 409 (wrong state) from a 500 (engine
 * failure). #380 and #381 add the loading-state axis — bare-promise
 * Export buttons with no disable-while-pending.
 *
 * Each test stubs the mutation endpoint with a 500 and a sentinel
 * string in `message`, then asserts the toast contains the sentinel.
 * For loading-state tests we also assert the button is disabled during
 * pending and re-enabled on error.
 *
 * Pattern matches `silent-mutations.spec.ts` — same `serviceWorkers:
 * "block"` config, same scope pinning, same sentinel-string idiom.
 */
import { test, expect, type Page } from "@playwright/test";
import { signIn, nav, apiPost, uid, DEFAULT_SCOPE } from "./helpers";

test.use({ actionTimeout: 10_000, serviceWorkers: "block" });

test.beforeEach(async ({ page }) => {
  await page.addInitScript((scope) => {
    try {
      localStorage.setItem("cairn_scope", JSON.stringify(scope));
    } catch {
      /* quota / private — downstream assertions will fail loudly */
    }
  }, DEFAULT_SCOPE);
});

// Dismiss the TenantSelector popover if `useBootstrapScope` resolves to
// `needs-pick`. Idempotent — copy-paste from `silent-mutations.spec.ts`.
async function dismissScopePopoverIfOpen(page: Page) {
  const popover = page.getByTestId("scope-popover");
  if (await popover.isVisible().catch(() => false)) {
    await page.keyboard.press("Escape");
    await expect(popover).not.toBeVisible({ timeout: 2_000 });
  }
}

// ── #378 — Session create drops backend message ────────────────────────────

test.describe("SessionsPage — create session (#378)", () => {
  test("500 → toast carries backend message", async ({ page }) => {
    await page.route(
      (url) => {
        // `createSession` hits `POST /v1/sessions` with a scope-wrapped
        // body. We match the exact path rather than endsWith("/sessions")
        // so we don't catch GETs for the list.
        return url.pathname.endsWith("/v1/sessions");
      },
      async (route, req) => {
        if (req.method() !== "POST") return route.continue();
        await route.fulfill({
          status: 500,
          contentType: "application/json",
          body: JSON.stringify({
            code: "store_error",
            message: "session-378-sentinel: event log append failed",
          }),
        });
      },
    );

    await signIn(page);
    await nav(page, "sessions");
    await dismissScopePopoverIfOpen(page);

    const createBtn = page.getByTestId("new-session-btn");
    await expect(createBtn).toBeVisible({ timeout: 10_000 });
    await createBtn.click();

    const toast = page.getByRole("alert").filter({ hasText: /session-378-sentinel|Failed to create session/ });
    await expect(toast).toBeVisible({ timeout: 5_000 });
    // Operator-readable text: the sentinel MUST reach the UI.
    await expect(toast).toContainText("session-378-sentinel");
  });
});

// ── #379 — GitHub scan drops backend message ───────────────────────────────

test.describe("IntegrationsPage — GitHub scan (#379)", () => {
  test("500 → toast carries backend message (e.g. rate-limit detail)", async ({ page }) => {
    // Stub `installations` to report `configured: true` so the Scan Repo
    // button renders. The real dev instance has no GitHub App configured,
    // so without this stub the integrations page shows the onboarding
    // card and the scan flow is unreachable.
    await page.route(
      (url) => url.pathname.endsWith("/v1/webhooks/github/installations"),
      async (route, req) => {
        if (req.method() !== "GET") return route.continue();
        await route.fulfill({
          status: 200,
          contentType: "application/json",
          body: JSON.stringify({
            installations: [{ id: 1, account: "sentinel-org", repository_selection: "all" }],
            configured: true,
          }),
        });
      },
    );

    // Stub queue so the page renders without waiting on backend data.
    await page.route(
      (url) => url.pathname.endsWith("/v1/webhooks/github/queue"),
      async (route, req) => {
        if (req.method() !== "GET") return route.continue();
        await route.fulfill({
          status: 200,
          contentType: "application/json",
          body: JSON.stringify({
            queue: [],
            total: 0,
            max_concurrent: 3,
            dispatcher_running: true,
          }),
        });
      },
    );

    // Stub scan to 500 with the sentinel.
    await page.route(
      (url) => url.pathname.endsWith("/v1/webhooks/github/scan"),
      async (route, req) => {
        if (req.method() !== "POST") return route.continue();
        await route.fulfill({
          status: 500,
          contentType: "application/json",
          body: JSON.stringify({
            code: "rate_limited",
            message: "scan-379-sentinel: rate limited — retry in 60s",
          }),
        });
      },
    );

    await signIn(page);
    await nav(page, "integrations");
    await dismissScopePopoverIfOpen(page);

    const openScanBtn = page.getByTestId("github-scan-open-btn");
    await expect(openScanBtn).toBeVisible({ timeout: 10_000 });
    await openScanBtn.click();

    const repoInput = page.getByTestId("github-scan-repo-input");
    await expect(repoInput).toBeVisible();
    await repoInput.fill("sentinel-org/sentinel-repo");
    await page.getByTestId("github-scan-submit-btn").click();

    const toast = page.getByRole("alert").filter({ hasText: /scan-379-sentinel|Scan failed/ });
    await expect(toast).toBeVisible({ timeout: 5_000 });
    await expect(toast).toContainText("scan-379-sentinel");
  });
});

// ── #380 — Run export loading state + error surface ────────────────────────

async function createRun(request: Parameters<typeof apiPost>[0]): Promise<{ runId: string; sessionId: string }> {
  const sid = `expsess_${uid()}`;
  const rid = `exprun_${uid()}`;
  const sess = await apiPost(request, "/v1/sessions", { session_id: sid, ...DEFAULT_SCOPE });
  expect(sess.status, `session create failed: ${JSON.stringify(sess.body)}`).toBeGreaterThanOrEqual(200);
  expect(sess.status).toBeLessThan(300);
  const run = await apiPost(request, "/v1/runs", {
    session_id: sid,
    run_id: rid,
    ...DEFAULT_SCOPE,
  });
  expect(run.status, `run create failed: ${JSON.stringify(run.body)}`).toBeGreaterThanOrEqual(200);
  expect(run.status).toBeLessThan(300);
  return { runId: rid, sessionId: sid };
}

test.describe("RunDetailPage — export run (#380)", () => {
  test("500 → error toast + button re-enabled", async ({ page, request }) => {
    const { runId } = await createRun(request);

    await page.route(
      (url) => url.pathname.endsWith(`/v1/runs/${runId}/export`),
      async (route, req) => {
        if (req.method() !== "GET") return route.continue();
        await route.fulfill({
          status: 500,
          contentType: "application/json",
          body: JSON.stringify({
            code: "export_failed",
            message: "export-run-380-sentinel: projection unavailable",
          }),
        });
      },
    );

    await signIn(page);
    await nav(page, `run/${runId}`);

    const exportBtn = page.getByTestId("run-export-btn");
    await expect(exportBtn).toBeVisible({ timeout: 10_000 });
    await exportBtn.click();

    const toast = page.getByRole("alert").filter({ hasText: /export-run-380-sentinel|Export failed/ });
    await expect(toast).toBeVisible({ timeout: 5_000 });
    await expect(toast).toContainText("export-run-380-sentinel");
    // Button re-enables on error so the operator can retry.
    await expect(exportBtn).toBeEnabled();
    await expect(exportBtn).toHaveAttribute("data-pending", "false");
  });

  test("loading state — button disabled + data-pending=true while in flight", async ({ page, request }) => {
    const { runId } = await createRun(request);

    // Delay the response so the pending state is observable in the
    // browser. `setTimeout` inside the route handler is enough — the
    // page.route fulfillment only resolves after this promise.
    await page.route(
      (url) => url.pathname.endsWith(`/v1/runs/${runId}/export`),
      async (route, req) => {
        if (req.method() !== "GET") return route.continue();
        await new Promise((resolve) => setTimeout(resolve, 800));
        await route.fulfill({
          status: 500,
          contentType: "application/json",
          body: JSON.stringify({
            code: "export_failed",
            message: "export-run-380-pending-sentinel: delayed failure",
          }),
        });
      },
    );

    await signIn(page);
    await nav(page, `run/${runId}`);

    const exportBtn = page.getByTestId("run-export-btn");
    await expect(exportBtn).toBeVisible({ timeout: 10_000 });
    await exportBtn.click();

    // Button must reflect pending while the request is in-flight. Assert
    // against `data-pending="true"` AND disabled within a short window,
    // before the 800ms timer resolves. Playwright's default polling
    // cadence (~100ms) makes this reliable.
    await expect(exportBtn).toHaveAttribute("data-pending", "true", { timeout: 1_500 });
    await expect(exportBtn).toBeDisabled();

    // Once the error resolves, re-enable.
    await expect(exportBtn).toHaveAttribute("data-pending", "false", { timeout: 5_000 });
    await expect(exportBtn).toBeEnabled();
  });
});

// ── #381 — Session export loading state + error surface ────────────────────

test.describe("SessionDetailPage — export session (#381)", () => {
  test("500 → error toast + button re-enabled", async ({ page, request }) => {
    const { sessionId } = await createRun(request);

    await page.route(
      (url) => url.pathname.endsWith(`/v1/sessions/${sessionId}/export`),
      async (route, req) => {
        if (req.method() !== "GET") return route.continue();
        await route.fulfill({
          status: 500,
          contentType: "application/json",
          body: JSON.stringify({
            code: "export_failed",
            message: "export-session-381-sentinel: session projection lag",
          }),
        });
      },
    );

    await signIn(page);
    await nav(page, `session/${sessionId}`);

    const exportBtn = page.getByTestId("session-export-btn");
    await expect(exportBtn).toBeVisible({ timeout: 10_000 });
    await exportBtn.click();

    const toast = page.getByRole("alert").filter({ hasText: /export-session-381-sentinel|Export failed/ });
    await expect(toast).toBeVisible({ timeout: 5_000 });
    await expect(toast).toContainText("export-session-381-sentinel");
    await expect(exportBtn).toBeEnabled();
    await expect(exportBtn).toHaveAttribute("data-pending", "false");
  });

  test("loading state — button disabled + data-pending=true while in flight", async ({ page, request }) => {
    const { sessionId } = await createRun(request);

    await page.route(
      (url) => url.pathname.endsWith(`/v1/sessions/${sessionId}/export`),
      async (route, req) => {
        if (req.method() !== "GET") return route.continue();
        await new Promise((resolve) => setTimeout(resolve, 800));
        await route.fulfill({
          status: 500,
          contentType: "application/json",
          body: JSON.stringify({
            code: "export_failed",
            message: "export-session-381-pending-sentinel: delayed failure",
          }),
        });
      },
    );

    await signIn(page);
    await nav(page, `session/${sessionId}`);

    const exportBtn = page.getByTestId("session-export-btn");
    await expect(exportBtn).toBeVisible({ timeout: 10_000 });
    await exportBtn.click();

    await expect(exportBtn).toHaveAttribute("data-pending", "true", { timeout: 1_500 });
    await expect(exportBtn).toBeDisabled();

    await expect(exportBtn).toHaveAttribute("data-pending", "false", { timeout: 5_000 });
    await expect(exportBtn).toBeEnabled();
  });
});

// ── #382 — createEval no longer uses String(e) ─────────────────────────────

test.describe("EvalsPage — create eval run (#382)", () => {
  test("500 → toast carries backend message (not `[object Object]`)", async ({ page }) => {
    await page.route(
      (url) => url.pathname.endsWith("/v1/evals/runs"),
      async (route, req) => {
        if (req.method() !== "POST") return route.continue();
        await route.fulfill({
          status: 500,
          contentType: "application/json",
          body: JSON.stringify({
            code: "invalid_subject",
            message: "eval-382-sentinel: rubric_id references a different project",
          }),
        });
      },
    );

    await signIn(page);
    await nav(page, "evals");
    await dismissScopePopoverIfOpen(page);

    const openForm = page.getByTestId("eval-new-open-btn");
    await expect(openForm).toBeVisible({ timeout: 10_000 });
    await openForm.click();

    const submit = page.getByTestId("eval-create-submit-btn");
    await expect(submit).toBeVisible({ timeout: 5_000 });
    await submit.click();

    const toast = page.getByRole("alert").filter({ hasText: /eval-382-sentinel|Failed to create eval run/ });
    await expect(toast).toBeVisible({ timeout: 5_000 });
    await expect(toast).toContainText("eval-382-sentinel");
    // Regression guard: prior code produced "[object Object]" on a non-
    // Error throw via `String(e)`. The shared errorMessage helper
    // falls back to the static string instead.
    await expect(toast).not.toContainText("[object Object]");
  });
});

// ── #377 — PromptsPage release mutations ───────────────────────────────────

// The PromptsPage draw loop requires both prompt-assets AND prompt-releases
// lists to resolve before it renders the per-asset expansion. We stub both
// with a single synthetic draft release so the Request Approval button
// exists; we then stub the request-approval endpoint with a 500 and
// assert the backend message reaches the toast. Uses path-fragment
// matching rather than `endsWith` because the release id contains a
// colon / dollar that must not be URL-encoded in the match.
test.describe("PromptsPage — release mutations (#377)", () => {
  test("request-approval 500 → toast carries backend message", async ({ page }) => {
    const assetId   = `prompt_asset_sentinel_${uid()}`;
    const versionId = `prompt_version_sentinel_${uid()}`;
    const releaseId = `prompt_release_sentinel_${uid()}`;
    const now = Date.now();

    await page.route(
      (url) => url.pathname.endsWith("/v1/prompts/assets"),
      async (route, req) => {
        if (req.method() !== "GET") return route.continue();
        await route.fulfill({
          status: 200,
          contentType: "application/json",
          body: JSON.stringify({
            items: [
              {
                prompt_asset_id: assetId,
                name: `sentinel-prompt-${assetId}`,
                kind: "system",
                project: DEFAULT_SCOPE,
                scope: "project",
                created_at: now,
                updated_at: now,
              },
            ],
            has_more: false,
          }),
        });
      },
    );

    await page.route(
      (url) => url.pathname.endsWith("/v1/prompts/releases") && !url.pathname.includes("request-approval"),
      async (route, req) => {
        if (req.method() !== "GET") return route.continue();
        await route.fulfill({
          status: 200,
          contentType: "application/json",
          body: JSON.stringify({
            items: [
              {
                prompt_release_id: releaseId,
                prompt_asset_id:   assetId,
                prompt_version_id: versionId,
                project: DEFAULT_SCOPE,
                state: "draft",
                rollout_percent: null,
                release_tag: null,
                created_at: now,
                updated_at: now,
              },
            ],
            has_more: false,
          }),
        });
      },
    );

    // Stub the versions GET so the expanded body renders cleanly —
    // otherwise the panel stays in "Loading versions…" and the releases
    // section is hidden behind the loading spinner.
    await page.route(
      (url) => url.pathname.endsWith(`/v1/prompts/assets/${assetId}/versions`),
      async (route, req) => {
        if (req.method() !== "GET") return route.continue();
        await route.fulfill({
          status: 200,
          contentType: "application/json",
          body: JSON.stringify({ items: [], has_more: false }),
        });
      },
    );

    // Stub the mutation with a 500 + sentinel.
    await page.route(
      (url) => url.pathname.endsWith(`/v1/prompts/releases/${releaseId}/request-approval`),
      async (route, req) => {
        if (req.method() !== "POST") return route.continue();
        await route.fulfill({
          status: 500,
          contentType: "application/json",
          body: JSON.stringify({
            code: "invalid_transition",
            message: "prompt-377-sentinel: draft -> proposed requires reviewer",
          }),
        });
      },
    );

    await signIn(page);
    await nav(page, "prompts");
    await dismissScopePopoverIfOpen(page);

    const expandBtn = page.getByTestId(`prompt-asset-expand-btn-${assetId}`);
    await expect(expandBtn).toBeVisible({ timeout: 10_000 });
    await expandBtn.click();

    const reqApprovalBtn = page.getByTestId(`prompt-release-request-approval-btn-${releaseId}`);
    await expect(reqApprovalBtn).toBeVisible({ timeout: 5_000 });
    await reqApprovalBtn.click();

    const toast = page.getByRole("alert").filter({ hasText: /prompt-377-sentinel|Failed to request approval/ });
    await expect(toast).toBeVisible({ timeout: 5_000 });
    await expect(toast).toContainText("prompt-377-sentinel");
  });
});
