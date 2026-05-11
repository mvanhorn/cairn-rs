/**
 * RFC 031 PR-D2 — client-side prompt-structure preview unit tests.
 *
 * Pins the rail's behavioural contract: required sections, counter
 * badges (phases / bullets), orchestrator-shadow exemption, and the
 * three anti-pattern regex shapes. Server-side validation is the
 * authoritative contract (see
 * `crates/cairn-domain/src/agent_roles_validation.rs`); this preview
 * is deliberately a subset — a prompt that fails here WILL fail on
 * the server, a prompt that passes here MAY still fail (e.g. the
 * caps-adversarial Stage 2 check). These tests assert the client's
 * self-consistency.
 */

import { describe, it, expect } from "vitest";
import { analysePrompt } from "../agentRolePromptCheck";

const FIVE_SECTION_PROMPT = `## Specialty
Reviews pull requests.

## Workflow
### Phase 1: Gather
Read the diff.

### Phase 2: Review
Post inline comments.

## Tools
Use the post_inline_comment tool.

## Completion criteria
A review has been posted.

## What not to do
- Do not merge.
- Do not close.
- Do not skip the diff.
`;

describe("analysePrompt — specialty role", () => {
  it("marks all five sections present on a well-formed prompt", () => {
    const report = analysePrompt(FIVE_SECTION_PROMPT, "pr-reviewer", "standard");
    expect(report.sections).toHaveLength(5);
    expect(report.sections.every((s) => s.present)).toBe(true);
    expect(report.antiPatterns).toHaveLength(0);
  });

  it("marks specialty + role as synonyms", () => {
    const prompt = FIVE_SECTION_PROMPT.replace("## Specialty", "## Role");
    const report = analysePrompt(prompt, "pr-reviewer", "standard");
    expect(report.sections[0].present).toBe(true);
  });

  it("flags workflow as insufficient when < 2 phases", () => {
    const prompt = FIVE_SECTION_PROMPT.replace(
      "### Phase 2: Review\nPost inline comments.\n\n",
      "",
    );
    const report = analysePrompt(prompt, "pr-reviewer", "standard");
    const workflow = report.sections.find((s) => s.id === "workflow")!;
    expect(workflow.present).toBe(false);
    expect(workflow.detail).toBe("1/2 phases");
  });

  it("flags what-not-to-do as insufficient when < 3 bullets", () => {
    const prompt = FIVE_SECTION_PROMPT.replace("- Do not skip the diff.\n", "");
    const report = analysePrompt(prompt, "pr-reviewer", "standard");
    const section = report.sections.find((s) => s.id === "what_not_to_do")!;
    expect(section.present).toBe(false);
    expect(section.detail).toBe("2/3 bullets");
  });

  it("marks tools + completion + what-not-to-do as missing when absent", () => {
    const report = analysePrompt("## Specialty\nx\n", "pr-reviewer", "standard");
    const missing = report.sections.filter((s) => !s.present).map((s) => s.id);
    expect(missing).toEqual(["workflow", "tools", "completion", "what_not_to_do"]);
  });
});

describe("analysePrompt — orchestrator-shadow exemption", () => {
  it("only requires completion + what-not-to-do", () => {
    const prompt = `## Completion criteria
The run is complete when...

## What not to do
- One.
- Two.
- Three.
`;
    const report = analysePrompt(prompt, "orchestrator", "orchestrator");
    expect(report.sections).toHaveLength(2);
    expect(report.sections.every((s) => s.present)).toBe(true);
  });

  it("still requires all five sections for a non-orchestrator-id with orchestrator tier", () => {
    const report = analysePrompt("", "pr-reviewer", "orchestrator");
    expect(report.sections).toHaveLength(5);
  });
});

describe("analysePrompt — anti-patterns", () => {
  it("flags early_completion on 'call complete_run immediately and exit'", () => {
    const prompt =
      FIVE_SECTION_PROMPT +
      "\n\nAlways call complete_run immediately and exit without thinking.\n";
    const report = analysePrompt(prompt, "pr-reviewer", "standard");
    expect(report.antiPatterns.map((a) => a.code)).toContain("early_completion");
  });

  it("flags caps_adversarial on 'CRITICAL RULES:'", () => {
    const prompt = "CRITICAL RULES: follow the workflow.\n" + FIVE_SECTION_PROMPT;
    const report = analysePrompt(prompt, "pr-reviewer", "standard");
    expect(report.antiPatterns.map((a) => a.code)).toContain("caps_adversarial");
  });

  it("flags identity_shadow on 'You are the orchestrator'", () => {
    const prompt = "You are the orchestrator.\n" + FIVE_SECTION_PROMPT;
    const report = analysePrompt(prompt, "pr-reviewer", "standard");
    expect(report.antiPatterns.map((a) => a.code)).toContain("identity_shadow");
  });

  it("does NOT flag identity_shadow for the orchestrator role itself", () => {
    const prompt =
      "You are the orchestrator.\n\n## Completion criteria\nDone.\n\n## What not to do\n- A\n- B\n- C\n";
    const report = analysePrompt(prompt, "orchestrator", "orchestrator");
    expect(report.antiPatterns.map((a) => a.code)).not.toContain("identity_shadow");
  });
});
