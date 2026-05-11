/**
 * Compile-time + runtime coverage check for the TS `FailureClass` union.
 *
 * The TS union in `ui/src/lib/types.ts` MUST cover every variant of the
 * Rust enum `cairn_domain::lifecycle::FailureClass`. If Rust gains a new
 * variant and nobody updates the TS side, every UI consumer that
 * switches on `failure_class` silently falls through to the default
 * branch — issue #424 is that bug filed against a prior drift.
 *
 * This test does two things:
 *
 * 1. **Compile-time**: the `FAILURE_CLASS_VALUES` const below is typed as
 *    `readonly FailureClass[]`. The `AssertEqual` helper asserts that the
 *    element type is EXACTLY `FailureClass` — no missing variants, no
 *    extras. Missing: `Type '"..."' is not assignable to type 'FailureClass'`.
 *    Extra: the `AssertEqual` check fails at the bottom of the file.
 *
 * 2. **Runtime-vs-Rust**: parses `crates/cairn-domain/src/lifecycle.rs`,
 *    extracts the Rust variants, converts them to the snake_case JSON
 *    form serde emits, and asserts the TS list matches exactly. If Rust
 *    adds a variant, this test fails loudly before the wire drifts.
 */

import { describe, it, expect } from "vitest";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import type { FailureClass } from "../types";

/**
 * Every `FailureClass` wire string the server can emit, in the order the
 * Rust enum declares them. Add new variants in Rust-declaration order so
 * the diff stays readable.
 */
export const FAILURE_CLASS_VALUES = [
  "timed_out",
  "dependency_failed",
  "approval_rejected",
  "policy_denied",
  "execution_error",
  "lease_expired",
  "canceled_by_operator",
  "terminal_write_deadlock",
  "verification_rejected",
  "orphan_child",
  "all_providers_exhausted",
  "model_reported_failure",
  "contract_not_met",
] as const satisfies readonly FailureClass[];

// ── Compile-time coverage ────────────────────────────────────────────────────
//
// `AssertEqual<A, B>` resolves to `true` iff `A` and `B` are mutually
// assignable. We use it to prove that the array element type covers the
// full `FailureClass` union (i.e. nothing in `FailureClass` is missing
// from `FAILURE_CLASS_VALUES`).
type AssertEqual<A, B> =
  (<T>() => T extends A ? 1 : 2) extends (<T>() => T extends B ? 1 : 2) ? true : false;

// If you add a variant to `FailureClass` without adding it here, this
// line fails with `Type 'false' is not assignable to type 'true'`.
const _coverageProof: AssertEqual<(typeof FAILURE_CLASS_VALUES)[number], FailureClass> = true;
void _coverageProof;

// ── Runtime parity with Rust ─────────────────────────────────────────────────

/** Convert `PascalCase` to `snake_case` the way serde's `rename_all` does. */
function pascalToSnake(name: string): string {
  return name
    .replace(/([A-Z])/g, (_, ch: string, idx: number) =>
      idx === 0 ? ch.toLowerCase() : `_${ch.toLowerCase()}`,
    );
}

/**
 * Extract the variants from the `FailureClass` enum declaration in the
 * canonical Rust source file. We parse the file directly rather than
 * running `cargo` so the test is fast and has no toolchain dependency.
 */
function extractRustFailureClassVariants(): string[] {
  // `vitest` runs with cwd = `ui/`. Rust source is two directories up.
  const rustPath = resolve(
    __dirname,
    "../../../../crates/cairn-domain/src/lifecycle.rs",
  );
  const src = readFileSync(rustPath, "utf-8");

  // Find the `pub enum FailureClass { … }` block. `[\s\S]*?` matches any
  // char including newlines, non-greedily.
  const enumMatch = src.match(/pub enum FailureClass \{([\s\S]*?)\n\}/);
  if (!enumMatch) {
    throw new Error(
      `Could not locate 'pub enum FailureClass' in ${rustPath}. ` +
        `Did the Rust file move? Update the path in this test.`,
    );
  }

  // Strip `///` doc comments and `//` line comments, split on commas, keep
  // identifiers only.
  const body = enumMatch[1]
    .replace(/\/\/\/.*$/gm, "")
    .replace(/\/\/.*$/gm, "");

  return body
    .split(",")
    .map((line) => line.trim())
    .filter((line) => line.length > 0)
    .map((line) => {
      // A variant is a bare identifier (no payload in this enum).
      const match = line.match(/^([A-Z][A-Za-z0-9]*)/);
      if (!match) {
        throw new Error(
          `Unexpected variant syntax in FailureClass: ${JSON.stringify(line)}`,
        );
      }
      return match[1];
    });
}

describe("FailureClass TS union", () => {
  it("covers every Rust variant with identical snake_case strings", () => {
    const rustVariants = extractRustFailureClassVariants();
    const rustAsJson = rustVariants.map(pascalToSnake);

    // Order matters: we keep TS list in Rust-declaration order so a diff
    // is immediately obvious.
    expect([...FAILURE_CLASS_VALUES]).toEqual(rustAsJson);
  });

  it("has no duplicate variants", () => {
    const unique = new Set(FAILURE_CLASS_VALUES);
    expect(unique.size).toBe(FAILURE_CLASS_VALUES.length);
  });

  it("uses only snake_case lowercase + underscores", () => {
    for (const v of FAILURE_CLASS_VALUES) {
      expect(v).toMatch(/^[a-z][a-z0-9_]*$/);
    }
  });
});
