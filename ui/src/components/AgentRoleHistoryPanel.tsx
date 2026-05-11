/**
 * RFC 031 PR-D3 §History panel — timeline of AgentRoleDefined /
 * AgentRoleRetracted events for `(project, role_id)`.
 *
 * Entries render oldest → newest so the operator sees the role
 * evolve. Consecutive `defined` entries expose a simple "changed"
 * summary (name / tier / response_shape / tools / forbid flag / prompt
 * length delta) — the goal is a quick "what changed last" read, not a
 * line-by-line diff renderer. A full prompt diff view is a v1.1
 * enhancement if operator feedback warrants it.
 */

import { useQuery } from "@tanstack/react-query";
import { ArrowUpRight, Loader2, Trash2, UserCheck } from "lucide-react";

import { defaultApi } from "../lib/api";
import { useScope } from "../hooks/useScope";
import type { AgentRole, AgentRoleHistoryEntry } from "../lib/types";

function listsEqual(a: string[], b: string[]): boolean {
  if (a.length !== b.length) return false;
  for (let i = 0; i < a.length; i++) if (a[i] !== b[i]) return false;
  return true;
}

/** Summarise what changed between two consecutive `defined` entries. */
function describeChange(prev: AgentRole, next: AgentRole): string[] {
  const deltas: string[] = [];
  if (prev.display_name !== next.display_name) {
    deltas.push(`name: "${prev.display_name}" → "${next.display_name}"`);
  }
  if (prev.description !== next.description) {
    deltas.push("description changed");
  }
  if (prev.tier !== next.tier) {
    // Tier is immutable on PATCH per §D6, so this really only fires
    // on a retract-then-repost with a different tier — worth calling
    // out.
    deltas.push(`tier: ${prev.tier} → ${next.tier}`);
  }
  if (prev.response_shape !== next.response_shape) {
    deltas.push(`response_shape: ${prev.response_shape} → ${next.response_shape}`);
  }
  if (prev.forbid_all_tools !== next.forbid_all_tools) {
    deltas.push(
      `forbid_all_tools: ${prev.forbid_all_tools} → ${next.forbid_all_tools}`,
    );
  }
  if (!listsEqual(prev.tools, next.tools)) {
    deltas.push(
      `tools: ${prev.tools.length} → ${next.tools.length} entries`,
    );
  }
  const pPrompt = prev.system_prompt ?? "";
  const nPrompt = next.system_prompt ?? "";
  if (pPrompt !== nPrompt) {
    const delta = nPrompt.length - pPrompt.length;
    const sign = delta > 0 ? "+" : "";
    deltas.push(`prompt: ${sign}${delta} bytes (${nPrompt.length} total)`);
  }
  if (prev.max_context_tokens !== next.max_context_tokens) {
    deltas.push(
      `max_context_tokens: ${prev.max_context_tokens ?? "default"} → ${
        next.max_context_tokens ?? "default"
      }`,
    );
  }
  if (deltas.length === 0) {
    deltas.push("identical payload (re-POST restoring retracted row)");
  }
  return deltas;
}

interface EntryRowProps {
  entry: AgentRoleHistoryEntry;
  prevDefinedRole: AgentRole | null;
  isLast: boolean;
}

function EntryRow({ entry, prevDefinedRole, isLast }: EntryRowProps) {
  const iso = new Date(entry.at_ms).toISOString();
  return (
    <li className="relative pl-8 pb-4 last:pb-0">
      {/* Timeline spine */}
      {!isLast && (
        <span className="absolute left-[11px] top-5 bottom-0 w-px bg-gray-200 dark:bg-zinc-800" />
      )}
      <span
        className={
          "absolute left-1 top-1 w-5 h-5 rounded-full border-2 flex items-center justify-center " +
          (entry.kind === "defined"
            ? "bg-indigo-50 dark:bg-indigo-950/60 border-indigo-400 dark:border-indigo-600 text-indigo-500 dark:text-indigo-400"
            : "bg-red-50 dark:bg-red-950/60 border-red-400 dark:border-red-600 text-red-500 dark:text-red-400")
        }
      >
        {entry.kind === "defined" ? (
          <UserCheck size={10} />
        ) : (
          <Trash2 size={10} />
        )}
      </span>

      <div className="rounded-lg border border-gray-200 dark:border-zinc-800 bg-white dark:bg-zinc-950 px-4 py-3">
        <div className="flex items-center justify-between gap-2">
          <span className="text-[12px] font-semibold text-gray-800 dark:text-zinc-200">
            {entry.kind === "defined" ? "Defined" : "Retracted"}
          </span>
          <span
            className="text-[10px] font-mono text-gray-400 dark:text-zinc-500"
            title={iso}
          >
            {iso.replace("T", " ").replace(/\.\d+Z$/, "Z")} · {entry.actor}
          </span>
        </div>

        {entry.kind === "defined" && entry.role && (
          <div className="mt-2">
            <div className="flex items-center gap-1.5 flex-wrap text-[10px] font-mono">
              <span className="text-gray-400 dark:text-zinc-500">{entry.role.tier}</span>
              <span className="text-gray-300 dark:text-zinc-700">·</span>
              <span className="text-gray-400 dark:text-zinc-500">
                {entry.role.response_shape}
              </span>
              {entry.shadows_builtin && (
                <>
                  <span className="text-gray-300 dark:text-zinc-700">·</span>
                  <span className="text-amber-600 dark:text-amber-400">
                    shadows {entry.shadows_builtin}
                  </span>
                </>
              )}
            </div>
            {prevDefinedRole && (
              <ul className="mt-2 space-y-0.5 text-[11px] text-gray-600 dark:text-zinc-400">
                {describeChange(prevDefinedRole, entry.role).map((d, i) => (
                  <li key={i} className="flex items-start gap-1.5">
                    <ArrowUpRight
                      size={10}
                      className="text-indigo-400 mt-0.5 shrink-0"
                    />
                    <span>{d}</span>
                  </li>
                ))}
              </ul>
            )}
            {!prevDefinedRole && (
              <p className="mt-1 text-[11px] text-gray-500 dark:text-zinc-500 italic">
                First definition of this role in the project.
              </p>
            )}
          </div>
        )}

        {entry.kind === "retracted" && (
          <p className="mt-1 text-[11px] text-gray-500 dark:text-zinc-500">
            §D7: running orchestrations continue with the retired prompt; new runs fall
            back (built-in if shadowed, generic otherwise).
          </p>
        )}
      </div>
    </li>
  );
}

interface Props {
  roleId: string;
}

export function AgentRoleHistoryPanel({ roleId }: Props) {
  const [scope] = useScope();
  const { data, isLoading, isError } = useQuery({
    queryKey: [
      "agent-role-history",
      scope.tenant_id,
      scope.workspace_id,
      scope.project_id,
      roleId,
    ],
    queryFn: () => defaultApi.getAgentRoleHistory(roleId),
    staleTime: 30_000,
  });

  if (isLoading) {
    return (
      <div className="flex items-center gap-2 text-[11px] text-gray-400 dark:text-zinc-600">
        <Loader2 size={12} className="animate-spin" />
        Loading history…
      </div>
    );
  }
  if (isError || !data) {
    return <p className="text-[11px] text-red-400">Failed to load history.</p>;
  }

  const items = data.items;
  if (items.length === 0) {
    return (
      <p className="text-[11px] text-gray-500 dark:text-zinc-500 italic">
        No history — this role is either a built-in or has never been defined in this
        project.
      </p>
    );
  }

  // Thread the last-seen `defined` role through the list so each
  // `defined` entry can summarise what changed vs the previous one.
  const rendered: { entry: AgentRoleHistoryEntry; prev: AgentRole | null }[] = [];
  let lastDefined: AgentRole | null = null;
  for (const e of items) {
    rendered.push({ entry: e, prev: e.kind === "defined" ? lastDefined : null });
    if (e.kind === "defined" && e.role) {
      lastDefined = e.role;
    }
  }

  return (
    <ul className="relative">
      {rendered.map((r, i) => (
        <EntryRow
          key={`${r.entry.at_ms}-${r.entry.kind}-${i}`}
          entry={r.entry}
          prevDefinedRole={r.prev}
          isLast={i === rendered.length - 1}
        />
      ))}
    </ul>
  );
}

export default AgentRoleHistoryPanel;
