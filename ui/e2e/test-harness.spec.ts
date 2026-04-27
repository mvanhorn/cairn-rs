/**
 * E2E test for the Test Harness page's task-lifecycle scenario.
 *
 * Covers issue #257: the scenario used to call `pause run` immediately
 * after `create_run`, but a freshly-created run sits in `pending` with
 * no lease — the backend rejected pause with
 * `invalid run transition: partial_fence_triple -> suspended`.
 *
 * PR #262 added a `wait_pausable` polling step before the pause. This
 * test locks that behaviour in:
 *
 * 1. Navigate to `/#test-harness`, click Run on the lifecycle scenario.
 * 2. The run stays `pending` because we never orchestrate it — no LLM
 *    inference is triggered in this smoke path.
 * 3. The `wait_pausable` step must time out with the friendly message
 *    `Run did not reach pausable state in Xs` — NOT a raw
 *    `invalid run transition` error.
 * 4. The downstream `pause_run` step must be skipped (the scenario
 *    aborts on first failure).
 *
 * Requires cairn-app on :3000 with `CAIRN_ADMIN_TOKEN=dev-admin-token`,
 * same as the other Playwright specs.
 */
import { test, expect } from "@playwright/test";
import { nav, signIn } from "./helpers";

test.describe("#257 TestHarness task-lifecycle pause-state guard", () => {
  // wait_pausable polls for 10s, plus preceding steps (~a couple of API
  // calls), plus render slack. 45s gives comfortable headroom without
  // letting a real hang go undetected.
  test.setTimeout(45_000);

  test("lifecycle scenario times out with friendly message, never surfaces 'invalid transition'", async ({ page }) => {
    await signIn(page);
    await nav(page, "test-harness");

    const scenario = page.getByTestId("scenario-lifecycle");
    await expect(scenario).toBeVisible({ timeout: 5_000 });

    // Start the lifecycle scenario.
    await scenario.getByTestId("scenario-lifecycle-run-btn").click();

    // Wait for wait_pausable to settle (pass or fail). On a server with
    // no orchestration path it will time out after ~10s. The poll
    // timeout budget in the card is 10s; give ourselves 20s to observe
    // the settled state.
    const waitStep = scenario.getByTestId("step-wait_pausable");
    await expect(waitStep).toHaveAttribute("data-status", /pass|fail/, { timeout: 20_000 });

    const status = await waitStep.getAttribute("data-status");

    if (status === "fail") {
      // Timeout path — the whole point of #257. Must be the friendly
      // message, never the raw state-machine error.
      const errSpan = scenario.getByTestId("step-wait_pausable-error");
      await expect(errSpan).toBeVisible();
      const errText = (await errSpan.textContent()) ?? "";
      expect(errText).toMatch(/did not reach pausable state|terminal state/i);
      expect(errText.toLowerCase()).not.toContain("invalid run transition");
      expect(errText.toLowerCase()).not.toContain("partial_fence_triple");

      // And the downstream pause step must have been skipped — not
      // executed with a 409.
      const pauseStep = scenario.getByTestId("step-pause_run");
      await expect(pauseStep).toHaveAttribute("data-status", /skipped|idle/);
    } else {
      // Pass path — happens when the server's scheduler did advance
      // the run to a pausable state (e.g. `waiting_approval`). In that
      // case the pause step should either pass or, if the run moved on
      // again between poll and pause, surface a friendly 409 mapped by
      // the runStateErrors classifier — NEVER the raw state-machine
      // string.
      const pauseStep = scenario.getByTestId("step-pause_run");
      await expect(pauseStep).toHaveAttribute("data-status", /pass|fail|skipped/, { timeout: 15_000 });
      const pauseStatus = await pauseStep.getAttribute("data-status");
      if (pauseStatus === "fail") {
        const errSpan = scenario.getByTestId("step-pause_run-error");
        const errText = (await errSpan.textContent()) ?? "";
        expect(errText.toLowerCase()).not.toContain("invalid run transition");
        expect(errText.toLowerCase()).not.toContain("partial_fence_triple");
      }
    }
  });
});
