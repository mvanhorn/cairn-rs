/**
 * Shared E2E test helpers — single source of truth for signIn, nav, API helpers.
 *
 * Both operator-journey.spec.ts and real-scenarios.spec.ts import from here.
 */
import { expect, type Page, type APIRequestContext } from "@playwright/test";

export const TOKEN = "dev-admin-token";

/**
 * API base URL for APIRequestContext calls. Mirrors the precedence rule in
 * `playwright.config.ts` so `PLAYWRIGHT_BASE_URL` / `CAIRN_E2E_BASE_URL` /
 * `CAIRN_E2E_PORT` all override the Rust API calls these helpers make (not
 * just the browser baseURL). Without this, specs using
 * `apiPost/apiGet/...` would hit :3000 even when Playwright is pointed
 * elsewhere.
 *
 * Precedence (first non-empty wins):
 *   1. `PLAYWRIGHT_BASE_URL` — canonical override consumed by
 *      `playwright.config.ts`. We just trim + strip trailing slashes;
 *      strict validation lives in the config.
 *   2. `CAIRN_E2E_BASE_URL`  — relaxed variant: port-less localhost URLs
 *      get the port from CAIRN_E2E_PORT / DEFAULT.
 *   3. `CAIRN_E2E_PORT`      — numeric port override only; BASE becomes
 *      `http://localhost:<port>`.
 *   4. Default               — `http://localhost:3000`.
 *
 * Trailing slashes are stripped so `${BASE}${path}` never produces
 * `//v1/...`.
 */
function resolveApiBase(): string {
  const strip = (s: string) => s.replace(/\/+$/, "");

  const pwBase = process.env.PLAYWRIGHT_BASE_URL?.trim();
  if (pwBase) return strip(pwBase);

  const envBase = process.env.CAIRN_E2E_BASE_URL?.trim();
  const DEFAULT_PORT = 3000;

  const rawPort = process.env.CAIRN_E2E_PORT?.trim();
  let port = DEFAULT_PORT;
  if (rawPort) {
    const n = Number(rawPort);
    if (Number.isFinite(n) && Number.isInteger(n) && n > 0 && n <= 65_535) {
      port = n;
    }
  }

  if (envBase) {
    try {
      const u = new URL(envBase);
      if (u.hostname === "localhost" || u.hostname === "127.0.0.1") {
        return strip(u.port ? u.origin : `${u.protocol}//${u.hostname}:${port}`);
      }
      return strip(u.origin);
    } catch {
      // fall through to default
    }
  }
  return `http://localhost:${port}`;
}

export const BASE = resolveApiBase();
export const HDR = { Authorization: `Bearer ${TOKEN}`, "Content-Type": "application/json" };
export const DEFAULT_SCOPE = {
  tenant_id: "default_tenant",
  workspace_id: "default_workspace",
  project_id: "default_project",
};

// ── Auth ─────────────────────────────────────────────────────────────────────

export async function signIn(page: Page) {
  await page.goto("/");
  await page.waitForLoadState("domcontentloaded");
  const sidebar = page.getByTestId("sidebar");

  // Resolve the current page state before deciding what to do: either
  // we landed on the sidebar (already signed in), the login form, or
  // the app is still mounting. Without this poll the helper would race
  // early React mounts on cold starts.
  await expect
    .poll(async () => {
      if (await sidebar.isVisible().catch(() => false)) return "sidebar";
      if (await page.getByTestId("login-token-input").isVisible().catch(() => false)) return "login";
      return "loading";
    }, { timeout: 10_000 })
    .not.toBe("loading");

  // Already signed in (localStorage token persisted from prior test in same context)
  if (await sidebar.isVisible({ timeout: 1000 }).catch(() => false)) return;

  const input = page.getByTestId("login-token-input");

  // Use `pressSequentially` rather than `fill()`. React 19's
  // controlled-input onChange handler occasionally does not observe
  // the single-shot value set by `.fill()` under Playwright, which
  // leaves the Sign In button disabled and the test hung. Typing each
  // character produces the InputEvent sequence React expects and is
  // the pattern used elsewhere in the suite (real-scenarios.spec.ts).
  await input.click();
  await input.fill("");
  await input.pressSequentially(TOKEN, { delay: 10 });
  await expect.poll(() => input.inputValue(), { timeout: 5_000 }).toBe(TOKEN);

  const submitBtn = page.getByTestId("login-submit-btn");
  await expect(submitBtn).toBeEnabled({ timeout: 5_000 });
  await submitBtn.click({ timeout: 5000 });
  await expect(sidebar).toBeVisible({ timeout: 10_000 });
}

// ── Navigation ───────────────────────────────────────────────────────────────

export async function nav(page: Page, hash: string) {
  await page.goto(`/#${hash}`);
  await page.waitForLoadState("domcontentloaded");
  // Wait for the page content to render (sidebar visible = app is mounted)
  await page.getByTestId("sidebar").waitFor({ state: "visible", timeout: 5000 }).catch(() => {});

  // Dismiss the scope picker if it auto-opened. `SCOPE_NEEDS_PICK_EVENT`
  // in `App.tsx` auto-opens the popover when cairn-app has multiple
  // tenants but no `cairn_scope` is cached. Without this, every spec
  // that runs after any spec which creates a tenant gets its clicks
  // intercepted by a scope-popover that opens on navigation.
  // Tests that explicitly exercise the picker (`scope-picker.spec.ts`)
  // don't use this helper to open it, so they're unaffected.
  const popover = page.getByTestId("scope-popover");
  const visible = await popover
    .waitFor({ state: "visible", timeout: 200 })
    .then(() => true)
    .catch(() => false);
  if (visible) {
    await page.keyboard.press("Escape").catch(() => {});
    await popover.waitFor({ state: "hidden", timeout: 1000 }).catch(() => {});
  }
}

// ── API helpers ──────────────────────────────────────────────────────────────

// `data` is typed as `object | unknown[]` — some endpoints (e.g.
// /v1/events/append) accept a JSON array envelope, others a plain
// object. Both Playwright's `request.post` and the cairn backend
// happily serialise either shape; the union keeps array callers honest
// without an `unknown as object` cast at the call site.
export async function apiPost(r: APIRequestContext, path: string, data: object | unknown[]) {
  const resp = await r.post(`${BASE}${path}`, { headers: HDR, data });
  return { status: resp.status(), body: await resp.json().catch(() => ({})) };
}

export async function apiGet(r: APIRequestContext, path: string) {
  const resp = await r.get(`${BASE}${path}`, { headers: { Authorization: `Bearer ${TOKEN}` } });
  return { status: resp.status(), body: await resp.json().catch(() => ({})) };
}

export async function apiPut(r: APIRequestContext, path: string, data: object) {
  const resp = await r.put(`${BASE}${path}`, { headers: HDR, data });
  return { status: resp.status(), body: await resp.json().catch(() => ({})) };
}

export async function apiDel(r: APIRequestContext, path: string) {
  return r.delete(`${BASE}${path}`, { headers: { Authorization: `Bearer ${TOKEN}` } });
}

// ── Data extraction ──────────────────────────────────────────────────────────

/** Extract array from various API response shapes. */
export function listFrom<T = unknown>(resp: unknown): T[] {
  const b = (resp as Record<string, unknown>)?.body ?? resp;
  if (Array.isArray(b)) return b as T[];
  const obj = b as Record<string, unknown>;
  for (const key of ["items", "events", "results", "matches", "data"]) {
    if (Array.isArray(obj?.[key])) return obj[key] as T[];
  }
  return [];
}

// ── ID generation ────────────────────────────────────────────────────────────

export const uid = () => Date.now().toString(36) + Math.random().toString(36).slice(2, 6);

// ── LLM test tracking ───────────────────────────────────────────────────────

let _llmAssertionsExercised = 0;

/** Call inside if(status===200) blocks to count real LLM assertions. */
export function trackLlmAssertion() { _llmAssertionsExercised++; }

/** Get count of real LLM assertions exercised in this worker. */
export function llmAssertionCount() { return _llmAssertionsExercised; }
