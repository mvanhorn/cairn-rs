/**
 * SessionDetailPage — single-run creation from the session page (issue #635).
 *
 * Before this feature, the onboarding path "Start a Run" on the Dashboard
 * dead-ended: Sessions page created an empty session; Session-detail page
 * had no `New Run` button; Runs page only exposed Batch Create with no
 * goal field. This spec exercises the new primary-CTA path end-to-end:
 *
 *   1. Happy path — click New Run, fill goal + iteration budget, submit.
 *      Both `POST /v1/runs` and `POST /v1/runs/:id/orchestrate` fire, a
 *      success toast appears, and the page routes to the run-detail page
 *      for the new run.
 *   2. Validation — an empty goal disables the submit button and surfaces
 *      the field-level error after the operator interacts with the form.
 *   3. Partial failure — run creation succeeds but orchestration kick-off
 *      fails. The toast surfaces BOTH facts (run was created, kick-off
 *      failed) and the operator still lands on the new run's detail page
 *      so they can retry orchestration manually.
 *
 * The orchestrate POST is intercepted with `page.route()` so the spec
 * does not require a live LLM provider — only the UI wiring is under
 * test.
 */
import { test, expect, type Page } from "@playwright/test";
import { signIn, nav, apiPost, uid, DEFAULT_SCOPE } from "./helpers";

// The UI registers a service worker (see `ui/src/lib/registerSW.ts`). By
// default Playwright's `page.route()` does NOT intercept SW-mediated
// fetches, so block service workers for this spec — we test the app's
// own mutation wiring, not the SW cache layer.
test.use({ actionTimeout: 10_000, serviceWorkers: "block" });

// ── Fixtures ──────────────────────────────────────────────────────────────────

async function createOpenSession(
  request: Parameters<typeof apiPost>[0],
): Promise<string> {
  const sid = `s635_sess_${uid()}`;
  const sess = await apiPost(request, "/v1/sessions", {
    session_id: sid,
    ...DEFAULT_SCOPE,
  });
  expect(
    sess.status,
    `POST /v1/sessions failed: ${JSON.stringify(sess.body)}`,
  ).toBeGreaterThanOrEqual(200);
  expect(sess.status).toBeLessThan(300);
  return sid;
}

/**
 * Navigate to the session-detail page and wait for the New Run CTA to
 * mount. The button hydrates after the `sessions` query resolves — the
 * session state pill depends on the same data so the button's disabled
 * gate only goes live once the session record arrives.
 */
async function openSessionDetail(page: Page, sessionId: string) {
  await nav(page, `session/${sessionId}`);
  const btn = page.getByTestId("session-new-run-btn");
  await expect(btn).toBeVisible({ timeout: 10_000 });
  return btn;
}

// ── Tests ─────────────────────────────────────────────────────────────────────

test.describe("SessionDetailPage — New Run dialog (#635)", () => {
  test("happy path: fill dialog, submit, land on run-detail page", async ({
    page,
    request,
  }) => {
    const sid = await createOpenSession(request);

    // Intercept the orchestrate POST so we don't need a live LLM. The
    // POST /v1/runs call is NOT intercepted — it hits the real backend
    // so the run record exists and the session-runs query can see it.
    let orchestrateBody: unknown = null;
    let orchestrateRunId: string | null = null;
    await page.route(
      (url) => /\/v1\/runs\/[^/]+\/orchestrate$/.test(url.pathname),
      async (route, req) => {
        const m = /\/v1\/runs\/([^/]+)\/orchestrate$/.exec(new URL(req.url()).pathname);
        orchestrateRunId = m ? decodeURIComponent(m[1]) : null;
        orchestrateBody = JSON.parse(req.postData() ?? "{}");
        await route.fulfill({
          status: 200,
          contentType: "application/json",
          body: JSON.stringify({
            run_id: orchestrateRunId,
            summary: "stub step",
            termination: "pending",
            iterations: 1,
          }),
        });
      },
    );

    await signIn(page);
    const cta = await openSessionDetail(page, sid);
    await cta.click();

    const dialog = page.getByTestId("new-run-dialog");
    await expect(dialog).toBeVisible();

    // Fill goal (textarea) + max iterations, leave plan-mode off.
    const goalField = page.getByTestId("new-run-goal");
    await goalField.fill("Refactor the session-service dispatcher to use the new typed EngineError.");

    const iterField = page.getByTestId("new-run-max-iter");
    await iterField.fill("25");

    const submit = page.getByTestId("new-run-submit");
    await expect(submit).toBeEnabled();
    await submit.click();

    // The mutation fans out two requests; orchestrate lands on our
    // intercept. Wait for the success toast, then assert the
    // orchestrate body carried the goal the operator typed.
    const successToast = page.getByRole("alert").filter({ hasText: /Run .* started/ });
    await expect(successToast).toBeVisible({ timeout: 10_000 });

    // Navigation to the new run's detail page fires via
    // `window.location.hash = run/...` inside the success handler.
    await expect
      .poll(() => page.url().includes("#run/"), { timeout: 5_000 })
      .toBe(true);

    // The orchestrate body must carry the operator-supplied goal and
    // iteration cap — this is the whole point of the feature; without
    // it the orchestrator falls back to the default goal and the
    // onboarding dead-end regresses silently.
    expect(orchestrateBody).toMatchObject({
      goal: "Refactor the session-service dispatcher to use the new typed EngineError.",
      max_iterations: 25,
    });
    expect(orchestrateRunId).toBeTruthy();
  });

  test("validation: empty goal disables submit + surfaces field error", async ({
    page,
    request,
  }) => {
    const sid = await createOpenSession(request);
    await signIn(page);
    const cta = await openSessionDetail(page, sid);
    await cta.click();

    const submit = page.getByTestId("new-run-submit");
    // Submit is disabled with an empty goal (below the 10-char minimum).
    await expect(submit).toBeDisabled();

    // A 3-char goal is still too short. The `onBlur` handler flips
    // `touched` so the field-level error surfaces. Tab off the
    // textarea to trigger blur — Playwright's Locator has no `.blur()`.
    const goalField = page.getByTestId("new-run-goal");
    await goalField.fill("foo");
    await goalField.press("Tab");

    await expect(page.getByTestId("new-run-goal-err")).toBeVisible();
    await expect(submit).toBeDisabled();

    // Typing a long-enough goal clears the error and enables submit.
    await goalField.fill("Add a regression test for the 0-round decide-phase bug.");
    await expect(page.getByTestId("new-run-goal-err")).not.toBeVisible();
    await expect(submit).toBeEnabled();
  });

  test("partial failure: run created, orchestrate 500s → error toast + navigate to run", async ({
    page,
    request,
  }) => {
    const sid = await createOpenSession(request);

    // Fail the orchestrate call. The run-create call still hits the
    // real backend so the run row is actually persisted.
    await page.route(
      (url) => /\/v1\/runs\/[^/]+\/orchestrate$/.test(url.pathname),
      async (route) => {
        await route.fulfill({
          status: 500,
          contentType: "application/json",
          body: JSON.stringify({
            code: "provider_unavailable",
            message: "upstream provider timeout",
          }),
        });
      },
    );

    await signIn(page);
    const cta = await openSessionDetail(page, sid);
    await cta.click();

    await page.getByTestId("new-run-goal").fill("Diagnose the stuck-runs classifier false-positive.");
    await page.getByTestId("new-run-submit").click();

    // Error toast surfaces BOTH facts: the run was created AND the
    // orchestration kick-off failed. Match on the composite copy.
    const toast = page.getByRole("alert").filter({
      hasText: /created, but orchestration kick-off failed/i,
    });
    await expect(toast).toBeVisible({ timeout: 10_000 });

    // Operator still lands on the new run's detail page so they can
    // retry Orchestrate manually from there.
    await expect
      .poll(() => page.url().includes("#run/"), { timeout: 5_000 })
      .toBe(true);
  });
});
