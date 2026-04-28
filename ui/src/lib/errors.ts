/**
 * Shared error-message helpers.
 *
 * Lifted from `ProjectReposPage.tsx` (the first caller that needed it)
 * because audit issue #382 flagged four pages inlining the
 * `e instanceof Error ? e.message : String(e)` pattern — and `String(e)`
 * on a non-Error throw produces "[object Object]", which is worse than
 * the static fallback it replaces. Centralising the helper also
 * sidesteps the eight call sites in #377 from each reinventing their
 * own three-line ternary.
 *
 * Contract:
 *   - `e instanceof Error` with a non-empty `.message` → return `.message`.
 *   - Anything else (plain throws, `null`, `undefined`, Error without a
 *     message) → return `fallback`. We deliberately do **not** fall
 *     back to `String(e)`; if the server returned a non-Error value
 *     that's almost certainly "[object Object]" garbage and the
 *     operator gets nothing useful from it.
 */

/**
 * Extract a human-readable message from an unknown thrown value,
 * with an operator-visible fallback when nothing useful is present.
 */
export function errorMessage(e: unknown, fallback: string): string {
  if (e instanceof Error && e.message) return e.message;
  return fallback;
}
