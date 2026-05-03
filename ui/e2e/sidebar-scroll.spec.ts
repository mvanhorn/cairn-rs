/**
 * Sidebar scrollability on short viewports — dogfood #633 regression guard.
 *
 * The sidebar has ~30 nav items across 5 groups. On short viewports
 * (1280×720 laptop, 1024×600 netbook) the tail-end items fall below the
 * fold. Before the fix, the scroll container was missing `min-h-0`, so
 * the flex-child couldn't shrink below its content height and
 * `overflow-y: auto` silently no-opped. Playwright's auto-scroll walked
 * up to the already-satisfied `<aside>` and `click()` rejected with
 * "element is outside of the viewport".
 *
 * This spec locks in:
 *   1. The nav is the scroll container (not the aside).
 *   2. Its `clientHeight` never exceeds the viewport.
 *   3. Bottom-of-sidebar items (Retention under Admin, Settings under
 *      Infrastructure) are reachable by `scrollIntoView()` + click at
 *      1280×720 AND 1024×600.
 *
 * Retention lives in the admin-only group, which is hidden unless the
 * operator holds `TenantRole::Admin`. The `dev-admin-token` operator
 * is super-admin so the group is always visible here. We still guard
 * the test by waiting for the button to exist before asserting on
 * scroll behaviour, to keep the failure mode readable if RBAC ever
 * changes.
 */
import { test, expect, type Page } from '@playwright/test';
import { signIn } from './helpers';

// Per-test timeout: we do some scroll+click dances and a sign-in.
test.use({ actionTimeout: 5_000 });

async function assertNavScrolls(page: Page) {
  const nav = page.getByTestId('sidebar-nav');
  await expect(nav).toBeVisible();

  const metrics = await nav.evaluate((el) => {
    const style = getComputedStyle(el);
    return {
      overflowY: style.overflowY,
      clientHeight: el.clientHeight,
      scrollHeight: el.scrollHeight,
      viewport: window.innerHeight,
    };
  });

  // The nav must declare an auto/scroll overflow AND actually be capped
  // at (or below) the viewport. If min-h-0 is missing the clientHeight
  // exceeds the viewport and scrollIntoView() is a no-op.
  expect(metrics.overflowY).toMatch(/auto|scroll/);
  expect(metrics.clientHeight).toBeLessThanOrEqual(metrics.viewport);
  // And — by construction — there should be more content than fits:
  // if this fails, the test is no longer exercising the bug.
  expect(metrics.scrollHeight).toBeGreaterThan(metrics.clientHeight);
}

async function clickTailItem(page: Page, testid: string) {
  const btn = page.getByTestId(testid);
  await expect(btn, `${testid} should exist in the DOM`).toHaveCount(1);
  // The bug: scrollIntoView + click would fail with "element is outside
  // of the viewport". Playwright's click() does its own auto-scroll;
  // after the fix it finds the scrollable nav and scrolls within it.
  await btn.click({ timeout: 5_000 });
  await expect(btn).toHaveAttribute('aria-current', 'page');
}

test.describe('sidebar scroll — short viewports (#633)', () => {
  test('1280x720: every nav item is reachable', async ({ page }) => {
    await page.setViewportSize({ width: 1280, height: 720 });
    await signIn(page);

    await assertNavScrolls(page);

    // Middle-of-list below fold on a stock 1280x720 install.
    await clickTailItem(page, 'nav-credentials');
    // Tail of Admin group — the item the issue specifically calls out.
    await clickTailItem(page, 'nav-retention');
  });

  test('1024x600: bottom-end laptop still works', async ({ page }) => {
    // 1024x600 is the worst-case target: tablets, netbooks, small
    // windowed browser sessions. If it passes here it passes everywhere.
    await page.setViewportSize({ width: 1024, height: 600 });
    await signIn(page);

    await assertNavScrolls(page);

    await clickTailItem(page, 'nav-settings');
    await clickTailItem(page, 'nav-retention');
  });

  test('footer stays pinned while nav scrolls', async ({ page }) => {
    // Operator UX: account + sign-out must remain reachable no matter
    // how far the nav is scrolled. The footer's `shrink-0` guarantees it.
    await page.setViewportSize({ width: 1280, height: 720 });
    await signIn(page);

    const nav = page.getByTestId('sidebar-nav');
    await nav.evaluate((el) => {
      el.scrollTop = el.scrollHeight; // jump to the bottom of the nav
    });

    // Sign-out button lives in the footer. It must be clickable without
    // needing to scroll the aside.
    const signOut = page.getByRole('button', { name: /sign out/i });
    await expect(signOut).toBeVisible();
    const inView = await signOut.evaluate((el) => {
      const r = el.getBoundingClientRect();
      return r.top >= 0 && r.bottom <= window.innerHeight;
    });
    expect(inView).toBe(true);
  });
});
