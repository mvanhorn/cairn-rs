/**
 * RFC 031 PR-D2 — client-side prompt-structure preview.
 *
 * Mirrors the server's `validate_prompt_structure` (in
 * `crates/cairn-domain/src/agent_roles_validation.rs`) closely enough
 * that the editor's section-indicator rail shows the operator where
 * they'll fail BEFORE they submit. Server is still authoritative —
 * this is a UX hint only, not a gate. The per-section detection uses
 * the same ATX-H2 regex shape (`^##[ \t]+...$`) so a passing
 * client-side check is a reliable predictor of passing structural
 * validation on POST / PATCH.
 *
 * Canonical sections (see RFC 031 §Prompt Contract):
 *   ## Specialty  | ## Role
 *   ## Workflow          — requires ≥ 2 `### Phase` headings
 *   ## Tools
 *   ## Completion criteria | ## Completion
 *   ## What not to do | ## Do not — requires ≥ 3 column-0 bullets
 *
 * Orchestrator-shadow exemption: when `roleId === "orchestrator"` and
 * `tier === "orchestrator"`, only Completion + What-not-to-do are
 * required (§Orchestrator-shadow exemption).
 */

import type { AgentRoleTier } from "./types";

export type CanonicalSection =
  | "specialty"
  | "workflow"
  | "tools"
  | "completion"
  | "what_not_to_do";

export interface SectionStatus {
  id: CanonicalSection;
  label: string;
  present: boolean;
  /** Populated for sections with counted sub-structure. */
  detail?: string;
  /** Byte offset of the section header, if present. */
  offset?: number;
}

export interface PromptStructureReport {
  sections: SectionStatus[];
  /** Non-null when any anti-pattern matched client-side. Server is
   *  authoritative — this is a preview only. */
  antiPatterns: AntiPatternHit[];
}

export interface AntiPatternHit {
  code: "early_completion" | "caps_adversarial" | "identity_shadow";
  message: string;
}

const H2_RE = /^##[ \t]+([^\n#][^\n]*?)[ \t:]*$/gm;
const H3_PHASE_RE = /^###[ \t]+Phase\b/gim;
const BULLET_RE = /^[-*+][ \t]+\S/gm;

// Same anti-pattern regex shapes as the server validator (client
// subset — `caps_adversarial` runs Stage 1 only since Stage 2's
// programmatic letter-ratio check is expensive).
const EARLY_COMPLETION_RE =
  /(?:call\s+)?complete_run\s+(?:immediately|right\s+away|at\s+the\s+start)\s+(?:and\s+)?(?:exit|return|stop|without\s+\w+)\b|call\s+complete_run\s+now\b\s+and\s+(?:exit|return|stop)|call\s+complete_run\s+(?:before|without)\s+(?:finishing|completing|verifying)/i;
const CAPS_STAGE1_RE =
  /\b(?:CRITICAL\s+RULES?|MUST\s+NOT\s+FAIL|FAILURE\s+IS\s+UNACCEPTABLE|FATAL\s+ERROR\s+IF)\b/;
const SUBAGENT_IDENTITY_SHADOW_RE = /^\s*You\s+are\s+the\s+(?:orchestrator|operator|user)\b/im;

function titleOf(raw: string): string {
  return raw.trim().replace(/:+$/, "").trim().toLowerCase();
}

function canonicalFromTitle(title: string): CanonicalSection | null {
  switch (title) {
    case "specialty":
    case "role":
      return "specialty";
    case "workflow":
      return "workflow";
    case "tools":
      return "tools";
    case "completion criteria":
    case "completion":
      return "completion";
    case "what not to do":
    case "do not":
      return "what_not_to_do";
    default:
      return null;
  }
}

interface DetectedSection {
  section: CanonicalSection;
  headerOffset: number;
  bodyStart: number;
  bodyEnd: number;
}

function collectSections(prompt: string): DetectedSection[] {
  const hits: { canon: CanonicalSection | null; start: number; end: number }[] = [];
  // `/g`-flagged regexes carry `lastIndex` across `exec` calls. The
  // loop below exhausts `H2_RE` (hits null, which resets lastIndex to
  // 0), so in practice the state doesn't leak — but an early break
  // or exception would. Reset defensively before the loop so the
  // preview stays correct no matter how we exit.
  H2_RE.lastIndex = 0;
  let m: RegExpExecArray | null;
  while ((m = H2_RE.exec(prompt)) !== null) {
    hits.push({
      canon: canonicalFromTitle(titleOf(m[1])),
      start: m.index,
      end: m.index + m[0].length,
    });
  }
  const sections: DetectedSection[] = [];
  for (const [i, h] of hits.entries()) {
    if (!h.canon) continue;
    // First match wins for each canonical section.
    if (sections.some((s) => s.section === h.canon)) continue;
    const nextStart = hits[i + 1]?.start ?? prompt.length;
    const bodyStart = (() => {
      const nl = prompt.indexOf("\n", h.end);
      return nl === -1 ? prompt.length : nl + 1;
    })();
    sections.push({
      section: h.canon,
      headerOffset: h.start,
      bodyStart,
      bodyEnd: nextStart,
    });
  }
  return sections;
}

/** Count column-0 bullets (`-`, `*`, `+`) inside a body range. */
function countBullets(prompt: string, start: number, end: number): number {
  const slice = prompt.slice(start, end);
  return (slice.match(BULLET_RE) ?? []).length;
}

/** Count column-0 `### Phase` headings inside a body range. */
function countPhases(prompt: string, start: number, end: number): number {
  const slice = prompt.slice(start, end);
  return (slice.match(H3_PHASE_RE) ?? []).length;
}

function isOrchestratorShadow(roleId: string, tier: AgentRoleTier): boolean {
  return roleId === "orchestrator" && tier === "orchestrator";
}

const SPECIALTY_SECTIONS: {
  id: CanonicalSection;
  label: string;
}[] = [
  { id: "specialty",       label: "## Specialty" },
  { id: "workflow",        label: "## Workflow" },
  { id: "tools",           label: "## Tools" },
  { id: "completion",      label: "## Completion criteria" },
  { id: "what_not_to_do",  label: "## What not to do" },
];

const ORCHESTRATOR_SHADOW_SECTIONS: typeof SPECIALTY_SECTIONS = [
  { id: "completion",      label: "## Completion criteria" },
  { id: "what_not_to_do",  label: "## What not to do" },
];

export function analysePrompt(
  prompt: string,
  roleId: string,
  tier: AgentRoleTier,
): PromptStructureReport {
  const required = isOrchestratorShadow(roleId, tier)
    ? ORCHESTRATOR_SHADOW_SECTIONS
    : SPECIALTY_SECTIONS;

  const detected = collectSections(prompt);
  const detectedById = new Map(detected.map((d) => [d.section, d]));

  const sections: SectionStatus[] = required.map(({ id, label }) => {
    const hit = detectedById.get(id);
    if (!hit) {
      return { id, label, present: false };
    }
    // Per-section depth checks — mirror server's counters.
    if (id === "workflow") {
      const phases = countPhases(prompt, hit.bodyStart, hit.bodyEnd);
      return {
        id,
        label,
        present: phases >= 2,
        detail: `${phases}/2 phases`,
        offset: hit.headerOffset,
      };
    }
    if (id === "what_not_to_do") {
      const bullets = countBullets(prompt, hit.bodyStart, hit.bodyEnd);
      return {
        id,
        label,
        present: bullets >= 3,
        detail: `${bullets}/3 bullets`,
        offset: hit.headerOffset,
      };
    }
    return { id, label, present: true, offset: hit.headerOffset };
  });

  const antiPatterns: AntiPatternHit[] = [];
  if (EARLY_COMPLETION_RE.test(prompt)) {
    antiPatterns.push({
      code: "early_completion",
      message:
        "Prompt tells the model to call complete_run early / before finishing. Will be rejected at POST/PATCH.",
    });
  }
  if (CAPS_STAGE1_RE.test(prompt)) {
    antiPatterns.push({
      code: "caps_adversarial",
      message:
        "Adversarial-framing phrase (CRITICAL RULES / MUST NOT FAIL / FAILURE IS UNACCEPTABLE / FATAL ERROR IF). May be rejected; server runs a stricter Stage 2 check.",
    });
  }
  if (!isOrchestratorShadow(roleId, tier) && SUBAGENT_IDENTITY_SHADOW_RE.test(prompt)) {
    antiPatterns.push({
      code: "identity_shadow",
      message:
        "Sub-agent prompt asserts 'You are the orchestrator/operator/user' — conflicts with the BASE_SUBAGENT_PROMPT identity the server prepends.",
    });
  }

  return { sections, antiPatterns };
}
