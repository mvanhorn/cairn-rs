/**
 * RunDetailPage — orchestrate-next button UX (issue #253).
 *
 * Verifies that the Orchestrate action on RunDetailPage is no longer silent:
 *   1. Happy path — click triggers a loading state then a success toast.
 *   2. Error path — a 409 response surfaces an operator-actionable toast.
 *   3. Disabled state — a terminal-state run disables the button and the
 *      hover title explains why.
 *
 * All three exercise the same rendering path (`OperatorActions` in
 * `ui/src/pages/RunDetailPage.tsx`). The happy/error paths use
 * `page.route()` to intercept `POST /v1/runs/:id/orchestrate` so the test
 * does not depend on a live LLM provider; only the UI wiring is under test.
 */
import { test, expect, type Page } from "@playwright/test";
import { signIn, nav, apiPost, uid, DEFAULT_SCOPE } from "./helpers";

// The UI registers a service worker (see `ui/src/lib/registerSW.ts`). By
// default playwright's `page.route()` does NOT intercept SW-mediated fetch
// calls, so we need to block service workers for this spec — we want to
// test the app's own mutation wiring, not the SW cache layer.
test.use({ actionTimeout: 10_000, serviceWorkers: "block" });

// ── Fixtures ──────────────────────────────────────────────────────────────────

async function createPendingRun(request: Parameters<typeof apiPost>[0]): Promise<string> {
  const sid = `rd253_sess_${uid()}`;
  const rid = `rd253_run_${uid()}`;
  // Assert 2xx on both setup calls so a failure in fixture creation
  // fails fast with the actual HTTP status/body instead of bubbling up
  // later as "button not visible" or "run not found".
  const sess = await apiPost(request, "/v1/sessions", { session_id: sid, ...DEFAULT_SCOPE });
  expect(sess.status, `POST /v1/sessions failed: ${JSON.stringify(sess.body)}`).toBeGreaterThanOrEqual(200);
  expect(sess.status, `POST /v1/sessions failed: ${JSON.stringify(sess.body)}`).toBeLessThan(300);
  const run = await apiPost(request, "/v1/runs", { session_id: sid, run_id: rid, ...DEFAULT_SCOPE });
  expect(run.status, `POST /v1/runs failed: ${JSON.stringify(run.body)}`).toBeGreaterThanOrEqual(200);
  expect(run.status, `POST /v1/runs failed: ${JSON.stringify(run.body)}`).toBeLessThan(300);
  return rid;
}

/**
 * Append a synthetic `run_state_changed` envelope taking a run to a
 * terminal state. Uses the same test-only append endpoint the rest of the
 * e2e suite (operator-journey.spec.ts) relies on.
 */
async function driveToTerminal(
  request: Parameters<typeof apiPost>[0],
  runId: string,
  terminal: "completed" | "failed" | "canceled",
) {
  const envelope = [{
    event_id: `evt_rd253_${uid()}`,
    source: { source_type: "runtime" },
    ownership: { scope: "project", ...DEFAULT_SCOPE },
    causation_id: null,
    correlation_id: null,
    payload: {
      event: "run_state_changed",
      project: { ...DEFAULT_SCOPE },
      run_id: runId,
      transition: { from: "pending", to: terminal },
      failure_class: null,
      pause_reason: null,
      resume_trigger: null,
    },
  }];
  // `/v1/events/append` accepts a JSON-array envelope; `apiPost`'s
  // `data` union (`object | unknown[]`) covers it, so no cast needed.
  // Keeps auth + base-URL consistent with `createPendingRun` above.
  const { status } = await apiPost(request, "/v1/events/append", envelope);
  expect(status).toBe(201);
}

/**
 * Navigate to RunDetailPage for `runId` and wait for the orchestrate button
 * to mount. The button is rendered inside `OperatorActions` which hydrates
 * after the `run-detail` query resolves.
 */
async function openRunDetail(page: Page, runId: string) {
  await nav(page, `run/${runId}`);
  const btn = page.getByTestId("run-orchestrate-btn");
  await expect(btn).toBeVisible({ timeout: 10_000 });
  return btn;
}

// ── Tests ─────────────────────────────────────────────────────────────────────

test.describe("RunDetailPage — orchestrate-next button (#253)", () => {
  test("active run + 200 response → success toast + loading state visible", async ({ page, request }) => {
    const rid = await createPendingRun(request);

    // Intercept the orchestrate POST so the test does not require a real
    // LLM provider. Delay slightly so the pending/loading state is
    // observable before the success toast arrives.
    await page.route(
      (url) => url.pathname.endsWith(`/v1/runs/${rid}/orchestrate`),
      async (route) => {
        // Sleep inside the handler so React has time to render isPending.
        await new Promise((resolve) => setTimeout(resolve, 300));
        await route.fulfill({
          status: 200,
          contentType: "application/json",
          body: JSON.stringify({
            run_id: rid,
            summary: "stub step",
            termination: "pending",
            iterations: 1,
          }),
        });
      },
    );

    await signIn(page);
    const btn = await openRunDetail(page, rid);

    // Kick off the mutation.
    await btn.click();

    // Loading state: the button carries data-pending="true" and the
    // spinner replaces the icon. Asserting the attribute is the most
    // deterministic signal — the spinner itself swaps in <1 frame on a
    // real CPU and races with the screenshot.
    await expect(btn).toHaveAttribute("data-pending", "true");

    // Success toast surfaces the confirmation copy.
    const toast = page.getByRole("alert").filter({ hasText: "Orchestration step triggered" });
    await expect(toast).toBeVisible({ timeout: 5_000 });

    // The pending state unwinds after success.
    await expect(btn).not.toHaveAttribute("data-pending", "true");
  });

  test("409 response → error toast surfaces (friendly copy from mapRunActionError)", async ({ page, request }) => {
    const rid = await createPendingRun(request);

    await page.route(
      (url) => url.pathname.endsWith(`/v1/runs/${rid}/orchestrate`),
      async (route) => {
        await route.fulfill({
          status: 409,
          contentType: "application/json",
          body: JSON.stringify({
            code: "invalid_state_transition",
            message: "invalid run transition: partial_fence_triple -> running",
          }),
        });
      },
    );

    await signIn(page);
    const btn = await openRunDetail(page, rid);

    await btn.click();

    // `mapRunActionError` classifies 409 invalid_state_transition as a
    // friendly "Cannot orchestrate" message. Confirm the error toast
    // renders (red accent + alert role) instead of the request failing
    // silently as before the fix.
    const toast = page.getByRole("alert").filter({ hasText: /Cannot orchestrate/i });
    await expect(toast).toBeVisible({ timeout: 5_000 });

    // Button re-enables after failure so the operator can retry.
    await expect(btn).not.toHaveAttribute("data-pending", "true");
    await expect(btn).toBeEnabled();
  });

  test("non-classified error → raw backend message surfaces verbatim", async ({ page, request }) => {
    // Locks in the second half of the fix: for errors the classifier
    // does NOT recognise (i.e. not `invalid_state_transition`), the
    // operator must still see *something* from the backend instead of a
    // silent no-op. `mapRunActionError` falls through to `err.message`
    // on `ApiError` with an unknown code, so this test drives that path
    // with a deliberately-unique sentinel string.
    const rid = await createPendingRun(request);
    const backendDetail = "rundetail-253-sentinel: tenant quota exhausted";

    await page.route(
      (url) => url.pathname.endsWith(`/v1/runs/${rid}/orchestrate`),
      async (route) => {
        await route.fulfill({
          status: 429,
          contentType: "application/json",
          body: JSON.stringify({ code: "quota_exhausted", message: backendDetail }),
        });
      },
    );

    await signIn(page);
    const btn = await openRunDetail(page, rid);

    await btn.click();

    const toast = page.getByRole("alert").filter({ hasText: backendDetail });
    await expect(toast).toBeVisible({ timeout: 5_000 });

    await expect(btn).toBeEnabled();
  });

  test("terminal-state run → button disabled with explanatory tooltip", async ({ page, request }) => {
    const rid = await createPendingRun(request);
    await driveToTerminal(request, rid, "completed");

    await signIn(page);
    const btn = await openRunDetail(page, rid);

    await expect(btn).toBeDisabled();
    // Tooltip (title attribute) explains why — matches stateGateTooltip("orchestrate", "completed").
    await expect(btn).toHaveAttribute("title", /completed/);
    await expect(btn).toHaveAttribute("title", /new run/i);
  });
});
