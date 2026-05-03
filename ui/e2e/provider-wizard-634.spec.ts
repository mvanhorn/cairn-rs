/**
 * Dogfood #634 — Add-Provider wizard must render the Connection step.
 *
 * Regression:
 *   Previously, clicking a provider-kind tile on step 1 (Type) both
 *   selected the kind AND called `setStep(1)`. Clicking "Next →"
 *   immediately after then advanced to step 2 (Models), so the
 *   Connection form (API key entry) was visually skipped and the
 *   resulting connection had no credential binding.
 *
 * Guard:
 *   1. Selecting a tile must NOT auto-advance. Step 0 (Type) remains
 *      visible after the click, with the tile marked selected.
 *   2. Clicking "Next →" from step 0 moves to step 1 (Connection) —
 *      the API-key input must be visible and marked required for
 *      credential-bearing adapters.
 *   3. Leaving the API-key empty must keep the operator on step 1
 *      (HTML5 `required` validation blocks form submission).
 *
 * The backend contract (422 credential_required) is pinned separately
 * by crates/cairn-app/tests/test_dogfood_634_credential_required.rs.
 */
import { test, expect, type Page } from "@playwright/test";
import { TOKEN } from "./helpers";

async function signIn(page: Page) {
  await page.goto("/");
  await page.waitForLoadState("domcontentloaded");
  const tokenInput = page.getByTestId("login-token-input");
  const sidebar = page.getByTestId("sidebar");
  await expect
    .poll(async () => {
      if (await sidebar.isVisible().catch(() => false)) return "sidebar";
      if (await tokenInput.isVisible().catch(() => false)) return "login";
      return "loading";
    }, { timeout: 10_000 })
    .not.toBe("loading");
  if (await sidebar.isVisible({ timeout: 1000 }).catch(() => false)) return;

  const devShortcut = page.getByRole("button", { name: TOKEN });
  if (await devShortcut.isVisible({ timeout: 1000 }).catch(() => false)) {
    await devShortcut.click();
  } else {
    await tokenInput.click();
    await tokenInput.fill("");
    await tokenInput.pressSequentially(TOKEN, { delay: 10 });
  }
  await expect
    .poll(() => tokenInput.inputValue(), { timeout: 5_000 })
    .toBe(TOKEN);
  const submitBtn = page.getByTestId("login-submit-btn");
  await expect(submitBtn).toBeEnabled();
  await submitBtn.click({ timeout: 5000 });
  await expect(sidebar).toBeVisible({ timeout: 10_000 });
}

test.describe("#634 — Add-Provider wizard Connection step", () => {
  test("selecting a Type tile does not auto-advance past Connection", async ({ page }) => {
    await signIn(page);
    await page.goto("/#providers");
    await page.waitForLoadState("domcontentloaded");

    // Open the wizard.
    const addBtn = page.getByTestId("add-provider-btn");
    await expect(addBtn).toBeVisible({ timeout: 10_000 });
    await addBtn.click();

    // Step 0 (Type) — the grid of provider tiles is rendered. Pick
    // Z.ai (GLM Coding Plan) — this is the exact tile the dogfood
    // operator picked when they hit the skip bug.
    const zaiTile = page.getByTestId("provider-kind-zai-coding");
    await expect(zaiTile).toBeVisible({ timeout: 5000 });
    await zaiTile.click();

    // CRITICAL GUARD: the tile click must NOT have advanced the
    // wizard. The API key input (step 1) should NOT yet be visible.
    // Before the #634 fix the next line would fail — `setStep(1)`
    // fired from the tile click, rendering the Connection form early.
    await expect(page.getByTestId("provider-api-key")).toBeHidden();

    // The Type grid is still visible — the tile stayed on screen.
    await expect(zaiTile).toBeVisible();

    // Now clicking "Next →" moves to step 1 (Connection). The API-key
    // input becomes visible and is marked required.
    await page.getByRole("button", { name: /Next/ }).click();
    const apiKeyInput = page.getByTestId("provider-api-key");
    await expect(apiKeyInput).toBeVisible({ timeout: 5000 });
    // Guard against regressions that drop the `required` attribute —
    // the UI must block submission when no key is entered.
    await expect(apiKeyInput).toHaveAttribute("required", "");
    // And the type must be `password` so the key isn't shoulder-surfed
    // during onboarding demos.
    await expect(apiKeyInput).toHaveAttribute("type", "password");
  });
});
