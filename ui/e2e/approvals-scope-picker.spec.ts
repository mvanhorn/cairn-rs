/**
 * ApprovalsPage — scope / match-policy picker (issue #638).
 *
 * Closes dogfood #638: operators need to approve "all `write` calls inside
 * this repo" from the UI. Before this spec, the drawer hard-coded
 * session-scope to the `Exact` match policy, so a realistic multi-tool run
 * required one click per byte-different call. The drawer now exposes the
 * three `ApprovalMatchPolicy` variants (`Exact`, `ExactPath`,
 * `ProjectScopedPath`) via a sub-dropdown that becomes visible once the
 * operator picks "Session".
 *
 * We verify the two behaviours that matter to the operator:
 *
 *   1. The request body on `POST /v1/approvals/:id/approve` carries the
 *      exact wire shape the domain expects for every variant: `{ "scope":
 *      { "type": "session", "match_policy": { "kind": "project_scoped_path",
 *      "project_root": "/workspaces/proj" } } }`. A mismatch here would be
 *      silently accepted by some older server builds and would widen the
 *      wrong blast radius — we assert byte-for-byte on the three variants
 *      so the UI cannot regress.
 *
 *   2. The live preview under the radio names the blast radius ("anywhere
 *      inside /workspaces/proj") so operators are not surprised. A missing
 *      preview was part of the original complaint in the issue.
 *
 * The list + approve endpoints are stubbed via `page.route()` (pattern
 * borrowed from `silent-mutations.spec.ts`) so the spec is independent of
 * the runtime tool-call projection: we are testing the UI wiring, not the
 * server-side resolver, and the resolver has its own Rust test suite at
 * `crates/cairn-runtime/tests/tool_call_approval_service.rs`.
 */
import { test, expect, type Page, type Route } from "@playwright/test";
import { signIn, nav, DEFAULT_SCOPE } from "./helpers";

test.use({ actionTimeout: 10_000, serviceWorkers: "block" });

test.beforeEach(async ({ page }) => {
  await page.addInitScript((scope) => {
    try {
      localStorage.setItem("cairn_scope", JSON.stringify(scope));
    } catch {
      /* ignore storage quota */
    }
  }, DEFAULT_SCOPE);
});

// ── Fixture: synthetic tool-call approval ──────────────────────────────────

const CALL_ID = "tc_638_fixture";

/**
 * Build a synthetic pending tool-call approval. Matches
 * `ToolCallApprovalRecord` (see `ui/src/lib/types.ts`). `match_policy.kind:
 * "exact"` is the server-side default at proposal time; the drawer lets
 * the operator widen it at approval time.
 */
function toolCallFixture() {
  return {
    kind: "tool_call",
    call_id: CALL_ID,
    session_id: "sess_638",
    run_id: "run_638",
    project: DEFAULT_SCOPE,
    tool_name: "write",
    original_tool_args: { path: "/workspaces/cairn/README.md", content: "# hi" },
    amended_tool_args: null,
    approved_tool_args: null,
    display_summary: "Write /workspaces/cairn/README.md",
    match_policy: { kind: "exact" as const },
    state: "pending" as const,
    operator_id: null,
    scope: null,
    reason: null,
    proposed_at_ms: Date.now(),
    approved_at_ms: null,
    rejected_at_ms: null,
    last_amended_at_ms: null,
    version: 1,
    created_at: Date.now(),
    updated_at: Date.now(),
  };
}

async function stubApprovalsList(page: Page) {
  await page.route(
    (url) => url.pathname === "/v1/approvals" || url.pathname.startsWith("/v1/approvals?"),
    async (route) => {
      if (route.request().method() !== "GET") return route.continue();
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({ items: [toolCallFixture()], hasMore: false }),
      });
    },
  );

  // The unified drawer re-fetches the single record on open via getApproval
  // (GET /v1/approvals/:id). Stub it to the same fixture.
  await page.route(
    (url) => url.pathname === `/v1/approvals/${CALL_ID}`,
    async (route) => {
      if (route.request().method() !== "GET") return route.continue();
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify(toolCallFixture()),
      });
    },
  );
}

/** Captures the request body on the approve POST so the assertion can
 *  run against the byte-exact JSON the UI shipped over the wire. The
 *  handler returns a 200 with a realistic approved record so the
 *  drawer's success toast fires and doesn't hide a flaky assertion
 *  behind a 500. */
async function interceptApprove(
  page: Page,
  captured: { body?: unknown },
) {
  await page.route(
    (url) => url.pathname === `/v1/approvals/${CALL_ID}/approve`,
    async (route: Route) => {
      const req = route.request();
      if (req.method() !== "POST") return route.continue();
      const raw = req.postData() ?? "";
      try {
        captured.body = JSON.parse(raw);
      } catch {
        captured.body = raw;
      }
      const approved = {
        ...toolCallFixture(),
        state: "approved",
        operator_id: "test",
        approved_at_ms: Date.now(),
      };
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify(approved),
      });
    },
  );
}

async function openDrawer(page: Page) {
  await nav(page, "approvals");
  const row = page.getByTestId(`approval-row-${CALL_ID}`);
  await expect(row).toBeVisible({ timeout: 10_000 });
  await row.click();
  // The Approve button only mounts once the drawer has hydrated with the
  // tool-call body — waiting on it is equivalent to waiting for the
  // drawer's React effects to settle.
  await expect(page.getByTestId("approval-approve-btn")).toBeVisible({ timeout: 5_000 });
}

// ── Tests ──────────────────────────────────────────────────────────────────

test.describe("ApprovalsPage — scope / match-policy picker (#638)", () => {
  test("Once → posts { scope: { type: 'once' } }", async ({ page }) => {
    await stubApprovalsList(page);
    const captured: { body?: unknown } = {};
    await interceptApprove(page, captured);

    await signIn(page);
    await openDrawer(page);

    // The "Once" radio is the default; explicitly assert-then-click to
    // guard against a future default swap sneaking in under us.
    const once = page.getByTestId("approval-scope-once");
    await expect(once).toBeChecked();
    await page.getByTestId("approval-approve-btn").click();

    await expect.poll(() => captured.body).toEqual({
      scope: { type: "once" },
    });
  });

  test("Session · Exactly this call → scope.session + match_policy.exact", async ({ page }) => {
    await stubApprovalsList(page);
    const captured: { body?: unknown } = {};
    await interceptApprove(page, captured);

    await signIn(page);
    await openDrawer(page);

    await page.getByTestId("approval-scope-session").check();
    await page
      .getByTestId("approval-match-policy-select")
      .selectOption("exact");

    // Preview copy describes the blast radius — assertion locks in the
    // operator-facing sentence so a future refactor cannot silently
    // drop it.
    const preview = page.getByTestId("approval-scope-preview");
    await expect(preview).toContainText(/byte-identical arguments/i);

    await page.getByTestId("approval-approve-btn").click();
    await expect.poll(() => captured.body).toEqual({
      scope: { type: "session", match_policy: { kind: "exact" } },
    });
  });

  test("Session · Same file path → scope.session + match_policy.exact_path", async ({ page }) => {
    await stubApprovalsList(page);
    const captured: { body?: unknown } = {};
    await interceptApprove(page, captured);

    await signIn(page);
    await openDrawer(page);

    await page.getByTestId("approval-scope-session").check();
    await page
      .getByTestId("approval-match-policy-select")
      .selectOption("exact_path");

    // Path is seeded from `original_tool_args.path` — assert the seed
    // matches the fixture, then override it to prove the input is
    // actually wired to the submit path.
    const pathInput = page.getByTestId("approval-match-policy-path");
    await expect(pathInput).toHaveValue("/workspaces/cairn/README.md");
    await pathInput.fill("/workspaces/cairn/CHANGELOG.md");

    await expect(page.getByTestId("approval-scope-preview")).toContainText(
      "/workspaces/cairn/CHANGELOG.md",
    );

    await page.getByTestId("approval-approve-btn").click();
    await expect.poll(() => captured.body).toEqual({
      scope: {
        type: "session",
        match_policy: {
          kind: "exact_path",
          path: "/workspaces/cairn/CHANGELOG.md",
        },
      },
    });
  });

  test("Session · Project root → scope.session + match_policy.project_scoped_path (issue's primary ask)", async ({
    page,
  }) => {
    await stubApprovalsList(page);
    const captured: { body?: unknown } = {};
    await interceptApprove(page, captured);

    await signIn(page);
    await openDrawer(page);

    await page.getByTestId("approval-scope-session").check();
    await page
      .getByTestId("approval-match-policy-select")
      .selectOption("project_scoped_path");

    const rootInput = page.getByTestId("approval-match-policy-project-root");
    // Seeded from the parent directory of the extracted path.
    await expect(rootInput).toHaveValue("/workspaces/cairn");
    // Operator overrides with an explicit repo root — the primary use
    // case from the dogfood transcript.
    await rootInput.fill("/home/ubuntu/my-project");

    await expect(page.getByTestId("approval-scope-preview")).toContainText(
      "/home/ubuntu/my-project",
    );

    await page.getByTestId("approval-approve-btn").click();
    await expect.poll(() => captured.body).toEqual({
      scope: {
        type: "session",
        match_policy: {
          kind: "project_scoped_path",
          project_root: "/home/ubuntu/my-project",
        },
      },
    });
  });

  test("Session · ProjectScopedPath with empty root → Approve stays disabled", async ({ page }) => {
    await stubApprovalsList(page);
    const captured: { body?: unknown } = {};
    await interceptApprove(page, captured);

    await signIn(page);
    await openDrawer(page);

    await page.getByTestId("approval-scope-session").check();
    await page
      .getByTestId("approval-match-policy-select")
      .selectOption("project_scoped_path");
    await page.getByTestId("approval-match-policy-project-root").fill("");

    const approve = page.getByTestId("approval-approve-btn");
    await expect(approve).toBeDisabled();
    // Preview flips to the "fill this in" hint so operators know why.
    await expect(page.getByTestId("approval-scope-preview")).toContainText(
      /fill in the project root/i,
    );
    // No POST went out.
    expect(captured.body).toBeUndefined();
  });
});
