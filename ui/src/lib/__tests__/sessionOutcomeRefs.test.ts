/**
 * Compile-time + runtime check for cross-type references in `SessionOutcome`.
 *
 * Audit finding #383: before this test existed, `SessionOutcome` referenced
 * `checkpoint_id: string` and `workspace_snapshot_id?: string | null` with
 * no corresponding `Checkpoint` / `WorkspaceSnapshot` interfaces in
 * `ui/src/lib/types.ts` — the shapes were effectively "any string" at the
 * type level, and any Rust-side rename on the backend (e.g.
 * `CheckpointId` → `RunCheckpointId`) would ship to production without
 * surfacing a UI build failure. Same class of drift as #424 for `FailureClass`.
 *
 * This file does two things:
 *
 * 1. **Compile-time**: the `AssertEqual` / `AssertExtends` helpers prove
 *    `SessionOutcome.checkpoint_id` is the same type as
 *    `Checkpoint["checkpoint_id"]`, and likewise for
 *    `workspace_snapshot_id` / `WorkspaceSnapshot["snapshot_id"]`. If
 *    either side drifts, `tsc --noEmit` fails.
 *
 * 2. **Runtime-vs-Rust**: parses `crates/cairn-domain/src/session_orchestration.rs`
 *    and `crates/cairn-store/src/projections/checkpoint.rs` to assert the
 *    TS interfaces name every Rust field. Matches the pattern used by
 *    `failureClass.test.ts` for the `FailureClass` enum.
 */

import { describe, it, expect } from "vitest";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import type { Checkpoint, SessionOutcome, WorkspaceSnapshot } from "../types";

// ── Compile-time cross-type references ──────────────────────────────────────
//
// `AssertEqual<A, B>` holds iff `A` and `B` are mutually assignable.
// `AssertExtends<A, B>` holds iff `A extends B`. We use the former because
// the SessionOutcome field should resolve to exactly `Checkpoint["checkpoint_id"]`,
// not merely a subtype thereof — any narrowing on the outcome side would
// hide drift.

type AssertEqual<A, B> =
  (<T>() => T extends A ? 1 : 2) extends (<T>() => T extends B ? 1 : 2) ? true : false;

// checkpoint_id wire shape must be the same on both sides.
const _ckptRefCheck: AssertEqual<SessionOutcome["checkpoint_id"], Checkpoint["checkpoint_id"]> = true;
void _ckptRefCheck;

// workspace_snapshot_id is optional + nullable on the outcome side but the
// underlying id type must match the snapshot record. We strip `null | undefined`
// via `NonNullable` before comparing to the source-of-truth id type.
const _snapRefCheck: AssertEqual<NonNullable<SessionOutcome["workspace_snapshot_id"]>, WorkspaceSnapshot["snapshot_id"]> = true;
void _snapRefCheck;

// ── Runtime parity with Rust ────────────────────────────────────────────────

function extractStructFields(source: string, structName: string): string[] {
  const structRe = new RegExp(
    `pub struct ${structName}\\s*\\{([\\s\\S]*?)\\n\\}`,
  );
  const match = source.match(structRe);
  if (!match) {
    throw new Error(
      `Could not locate \`pub struct ${structName}\` in the Rust source file`,
    );
  }
  // Strip attribute lines (`#[...]` and `#[serde(...)]`) and doc comments
  // before walking the body. `rustfmt` lays each field out on its own
  // line in this codebase, so a line-based pass is sufficient and avoids
  // a fragile comma-based split (field types may contain commas inside
  // generic arguments like `HashMap<String, Value>`).
  const fields: string[] = [];
  for (const rawLine of match[1].split("\n")) {
    const line = rawLine
      .replace(/\/\/\/.*$/, "")
      .replace(/\/\/.*$/, "")
      .trim();
    if (!line) continue;
    if (line.startsWith("#[")) continue;
    // `pub field_name: Type,`
    const fieldMatch = line.match(/^pub\s+([a-z_][a-z0-9_]*)\s*:/);
    if (fieldMatch) fields.push(fieldMatch[1]);
  }
  return fields;
}

function readRustFile(relPath: string): string {
  // Vitest runs with cwd = `ui/`. Rust source lives two directories up.
  const absPath = resolve(__dirname, "../../../../", relPath);
  return readFileSync(absPath, "utf-8");
}

describe("SessionOutcome cross-type references", () => {
  it("Checkpoint TS interface covers every CheckpointRecord Rust field", () => {
    const src = readRustFile("crates/cairn-store/src/projections/checkpoint.rs");
    const rustFields = extractStructFields(src, "CheckpointRecord");
    // TS keys on the Checkpoint interface — drop optional markers.
    const tsKeys: Array<keyof Checkpoint> = [
      "checkpoint_id",
      "project",
      "run_id",
      "disposition",
      "data",
      "version",
      "created_at",
    ];
    expect([...tsKeys].sort()).toEqual([...rustFields].sort());
  });

  it("WorkspaceSnapshot TS interface covers every Rust field", () => {
    const src = readRustFile("crates/cairn-domain/src/session_orchestration.rs");
    const rustFields = extractStructFields(src, "WorkspaceSnapshot");
    const tsKeys: Array<keyof WorkspaceSnapshot> = [
      "snapshot_id",
      "workspace_id",
      "snapshot_path",
      "created_at",
      "expires_at",
      "parent_snapshot_id",
    ];
    expect([...tsKeys].sort()).toEqual([...rustFields].sort());
  });

  it("SessionOutcome TS interface covers every Rust field", () => {
    const src = readRustFile("crates/cairn-domain/src/session_orchestration.rs");
    const rustFields = extractStructFields(src, "SessionOutcome");
    const tsKeys: Array<keyof SessionOutcome> = [
      "session_id",
      "root_run_id",
      "project",
      "checkpoint_id",
      "workspace_snapshot_id",
      "termination_reason",
      "compacted_summary",
      "next_step_hint",
      "cost_micros",
      "emitted_at",
    ];
    expect([...tsKeys].sort()).toEqual([...rustFields].sort());
  });
});
