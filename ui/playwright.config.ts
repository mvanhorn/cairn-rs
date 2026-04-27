import { defineConfig } from "@playwright/test";

// `PLAYWRIGHT_BASE_URL` lets CI / local runs point at an already-running
// cairn-app on a non-default port (e.g. when port 3000 is held by a prod
// instance on the dev host). Defaults to the conventional :3000 used by
// the bundled webServer below.
//
// The env var MUST be a bare origin (scheme + host + explicit port, no
// path / query / hash), e.g. `http://localhost:3002`. Everything else is
// rejected up-front so `use.baseURL` and `webServer.port`/`--port`
// cannot drift apart, and so API helpers that concatenate
// `BASE + "/v1/..."` never produce doubled paths or stray query
// strings. The webServer block also skips auto-spawning when the
// configured host isn't localhost — for a remote base URL the caller
// owns the server.
function parseBaseUrl(raw: string): { baseUrl: string; port: number; isLocalhost: boolean } {
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
  if (!Number.isFinite(port) || port <= 0 || port > 65535) {
    throw new Error(
      `PLAYWRIGHT_BASE_URL has invalid port ${JSON.stringify(parsed.port)} in ${JSON.stringify(raw)}`,
    );
  }
  // Reject anything beyond a bare origin. A path (`/app`), query
  // (`?x=1`), or hash (`#y`) in the env var would break helpers that
  // build request URLs with `${BASE}${path}`. A lone trailing slash is
  // normalized to the canonical `parsed.origin` below.
  const isBareRoot = parsed.pathname === "" || parsed.pathname === "/";
  if (!isBareRoot || parsed.search !== "" || parsed.hash !== "") {
    throw new Error(
      `PLAYWRIGHT_BASE_URL must be a bare origin (no path/query/hash); got ${JSON.stringify(raw)}`,
    );
  }
  const isLocalhost = parsed.hostname === "localhost" || parsed.hostname === "127.0.0.1";
  // Return the canonical origin so callers never see a trailing slash,
  // port coercion surprises, or differently-cased schemes.
  return { baseUrl: parsed.origin, port, isLocalhost };
}

const { baseUrl: BASE_URL, port: BASE_PORT, isLocalhost: BASE_IS_LOCAL } = parseBaseUrl(
  process.env.PLAYWRIGHT_BASE_URL ?? "http://localhost:3000",
);

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
  webServer: BASE_IS_LOCAL
    ? {
        command: `cd .. && CAIRN_ADMIN_TOKEN=dev-admin-token BEDROCK_API_KEY=test BEDROCK_MODEL_ID=test AWS_REGION=us-west-2 ./target/debug/cairn-app --port ${BASE_PORT}`,
        port: BASE_PORT,
        reuseExistingServer: true,
        timeout: 15_000,
      }
    : undefined,
});
