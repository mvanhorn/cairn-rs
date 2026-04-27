import { defineConfig } from "@playwright/test";

// Resolve the base URL + port Playwright should use. Three env knobs, in
// precedence order:
//
//   1. `PLAYWRIGHT_BASE_URL` — bare origin (scheme + host + explicit port).
//      Validated strictly because both `use.baseURL` and the managed
//      `webServer.port` / `--port ${PORT}` flag derive from it; any
//      mismatch makes the suite point at the wrong cairn-app.
//      Invalid values hard-error up front.
//   2. `CAIRN_E2E_BASE_URL`  — same semantics as PLAYWRIGHT_BASE_URL, but
//      relaxed: a port-less localhost URL is fine, we synthesize the
//      port from CAIRN_E2E_PORT (or the :3000 default). Remote hosts
//      skip the managed webServer — the caller owns that process.
//   3. `CAIRN_E2E_PORT`      — numeric port override only. BASE becomes
//      `http://localhost:<port>`.
//
// Default is `http://localhost:3000` with the bundled webServer.
//
// Invalid CAIRN_E2E_PORT / CAIRN_E2E_BASE_URL falls back with a
// console.warn so CI failures aren't opaque.

const DEFAULT_PORT = 3000;

function parsePort(raw: string | undefined): number {
  if (raw == null || raw === "") return DEFAULT_PORT;
  const n = Number(raw);
  if (!Number.isFinite(n) || !Number.isInteger(n) || n <= 0 || n > 65_535) {
    // eslint-disable-next-line no-console
    console.warn(
      `[playwright.config] ignoring invalid CAIRN_E2E_PORT=${JSON.stringify(raw)} — falling back to ${DEFAULT_PORT}.`,
    );
    return DEFAULT_PORT;
  }
  return n;
}

/**
 * Strict parser for PLAYWRIGHT_BASE_URL. The env var MUST be a bare
 * origin (scheme + host + explicit port, no path/query/hash), e.g.
 * `http://localhost:3002`. Anything else is rejected up-front so
 * `use.baseURL` and `webServer.port` / `--port` cannot drift apart, and
 * so API helpers that concatenate `BASE + "/v1/..."` never produce
 * doubled paths or stray query strings.
 */
function parseStrictBaseUrl(raw: string): { baseUrl: string; port: number; isLocalhost: boolean } {
  let parsed: URL;
  try {
    parsed = new URL(raw);
  } catch {
    throw new Error(
      `PLAYWRIGHT_BASE_URL=${JSON.stringify(raw)} is not a valid URL — expected e.g. "http://localhost:3000"`,
    );
  }
  if (parsed.protocol !== "http:" && parsed.protocol !== "https:") {
    throw new Error(
      `PLAYWRIGHT_BASE_URL must use http(s); got scheme "${parsed.protocol}" in ${JSON.stringify(raw)}`,
    );
  }
  if (!parsed.port) {
    throw new Error(
      `PLAYWRIGHT_BASE_URL must include an explicit port; got ${JSON.stringify(raw)}`,
    );
  }
  const port = Number(parsed.port);
  if (!Number.isFinite(port) || port <= 0 || port > 65_535) {
    throw new Error(
      `PLAYWRIGHT_BASE_URL has invalid port ${JSON.stringify(parsed.port)} in ${JSON.stringify(raw)}`,
    );
  }
  const isBareRoot = parsed.pathname === "" || parsed.pathname === "/";
  if (!isBareRoot || parsed.search !== "" || parsed.hash !== "") {
    throw new Error(
      `PLAYWRIGHT_BASE_URL must be a bare origin (no path/query/hash); got ${JSON.stringify(raw)}`,
    );
  }
  const isLocalhost = parsed.hostname === "localhost" || parsed.hostname === "127.0.0.1";
  return { baseUrl: parsed.origin, port, isLocalhost };
}

function resolveBase(): { baseUrl: string; port: number; useManagedServer: boolean } {
  const pwBase = process.env.PLAYWRIGHT_BASE_URL?.trim();
  if (pwBase) {
    const { baseUrl, port, isLocalhost } = parseStrictBaseUrl(pwBase);
    return { baseUrl, port, useManagedServer: isLocalhost };
  }

  let port = parsePort(process.env.CAIRN_E2E_PORT);
  let baseUrl = `http://localhost:${port}`;
  let useManagedServer = true;

  const envBase = process.env.CAIRN_E2E_BASE_URL?.trim();
  if (envBase) {
    try {
      const u = new URL(envBase);
      if (u.hostname === "localhost" || u.hostname === "127.0.0.1") {
        // Localhost — keep the managed webServer and make sure PORT
        // stays in sync with BASE. An explicit port in the URL wins;
        // otherwise we synthesize `http://localhost:<port>` so URL.origin's
        // default :80 doesn't race the server started on :3000.
        if (u.port) {
          port = parsePort(u.port);
          baseUrl = u.origin;
        } else {
          baseUrl = `${u.protocol}//${u.hostname}:${port}`;
        }
      } else {
        // Remote host — the caller owns the server. A port-less remote
        // URL is fine (caller probably means :80/:443).
        baseUrl = u.origin;
        useManagedServer = false;
      }
    } catch {
      // eslint-disable-next-line no-console
      console.warn(
        `[playwright.config] ignoring invalid CAIRN_E2E_BASE_URL=${JSON.stringify(envBase)} — using ${baseUrl}.`,
      );
    }
  }

  return { baseUrl, port, useManagedServer };
}

const { baseUrl: BASE_URL, port: BASE_PORT, useManagedServer } = resolveBase();

export default defineConfig({
  testDir: "./e2e",
  timeout: 15_000,
  retries: 0,
  use: {
    baseURL: BASE_URL,
    headless: true,
    screenshot: "only-on-failure",
    trace: "retain-on-failure",
  },
  // Only auto-start the bundled cairn-app when we're targeting localhost;
  // for remote base URLs the test operator is responsible for the server.
  webServer: useManagedServer
    ? {
        command: `cd .. && CAIRN_ADMIN_TOKEN=dev-admin-token BEDROCK_API_KEY=test BEDROCK_MODEL_ID=test AWS_REGION=us-west-2 ./target/debug/cairn-app --port ${BASE_PORT}`,
        port: BASE_PORT,
        reuseExistingServer: true,
        timeout: 15_000,
      }
    : undefined,
});
