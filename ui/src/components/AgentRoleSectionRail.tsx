/**
 * RFC 031 PR-D2 §Editor form layout — section-indicator rail.
 *
 * Renders one badge per required H2 section for the role's prompt.
 * Each badge shows `present ✓` / `missing ✗` / `insufficient (N/M)`.
 * Clicking a present badge scrolls the system-prompt textarea to the
 * section header. Missing badges reveal a short help hint.
 *
 * This is a client-side preview — the server's structural validator
 * is still authoritative on POST / PATCH. The rail uses the same
 * regex shapes so a clean rail reliably predicts a green 201/200
 * response. See `lib/agentRolePromptCheck.ts`.
 */

import { Check, ChevronRight, X } from "lucide-react";
import { clsx } from "clsx";

import type {
  PromptStructureReport,
  SectionStatus,
} from "../lib/agentRolePromptCheck";

interface Props {
  report: PromptStructureReport;
  onJumpTo?: (headerOffset: number) => void;
}

function classFor(s: SectionStatus): string {
  if (s.present) {
    return "text-emerald-500 dark:text-emerald-400 bg-emerald-50 dark:bg-emerald-950/40 border-emerald-200 dark:border-emerald-900";
  }
  return "text-red-500 dark:text-red-400 bg-red-50 dark:bg-red-950/40 border-red-200 dark:border-red-900";
}

export function AgentRoleSectionRail({ report, onJumpTo }: Props) {
  return (
    <div className="space-y-1.5">
      <p className="text-[10px] text-gray-400 dark:text-zinc-500 uppercase tracking-wide">
        Required sections
      </p>
      <div className="flex flex-col gap-1">
        {report.sections.map((s) => (
          <button
            key={s.id}
            type="button"
            onClick={() => {
              if (s.offset !== undefined && onJumpTo) onJumpTo(s.offset);
            }}
            disabled={s.offset === undefined}
            title={
              s.present
                ? "Present"
                : s.detail
                  ? `Insufficient — ${s.detail}`
                  : "Missing"
            }
            className={clsx(
              "flex items-center gap-1.5 px-2 py-1 rounded border text-[11px] font-mono transition-colors focus:outline-none focus-visible:ring-2 focus-visible:ring-indigo-400",
              classFor(s),
              s.offset !== undefined
                ? "hover:brightness-105 cursor-pointer"
                : "cursor-default",
            )}
          >
            {s.present ? <Check size={11} /> : <X size={11} />}
            <span className="flex-1 text-left">{s.label}</span>
            {s.detail && <span className="text-[10px] opacity-80">{s.detail}</span>}
            {s.offset !== undefined && (
              <ChevronRight size={11} className="opacity-60" />
            )}
          </button>
        ))}
      </div>

      {report.antiPatterns.length > 0 && (
        <div className="mt-3 rounded-lg border border-amber-400 dark:border-amber-700 bg-amber-50 dark:bg-amber-950/40 px-2.5 py-2">
          <p className="text-[10px] font-semibold uppercase tracking-wide text-amber-700 dark:text-amber-300">
            Anti-patterns
          </p>
          <ul className="mt-1 space-y-1">
            {report.antiPatterns.map((a) => (
              <li
                key={a.code}
                className="text-[10px] text-amber-800 dark:text-amber-200 leading-relaxed"
              >
                <span className="font-mono text-amber-600 dark:text-amber-400">
                  {a.code}
                </span>
                {" — "}
                {a.message}
              </li>
            ))}
          </ul>
        </div>
      )}
    </div>
  );
}

export default AgentRoleSectionRail;
