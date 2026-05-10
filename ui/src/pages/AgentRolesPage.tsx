/**
 * RFC 031 PR-D1 — Agent Roles list page.
 *
 * Merged list of built-in + operator-defined roles for the active
 * project scope. Source badges distinguish `builtin` / `custom` /
 * `custom_shadow`; custom rows link to the detail page, built-ins
 * link to a read-only view that shows the assembled prompt and tools.
 *
 * Deliberately minimal for PR-D1 — no source filter dropdown, no
 * bulk actions, no search. Those enhancements land in follow-up
 * PR-D2 alongside the history panel and draft persistence.
 */

import { useQuery } from "@tanstack/react-query";
import { Loader2, Plus, Wrench } from "lucide-react";

import { defaultApi } from "../lib/api";
import type { AgentRoleListItem } from "../lib/types";
import { useScope } from "../hooks/useScope";
import { AgentRoleBadge } from "../components/AgentRoleBadge";

interface RowProps {
  item: AgentRoleListItem;
}

function Row({ item }: RowProps) {
  const isEditable = item.source === "custom" || item.source === "custom_shadow";
  const onClick = () => {
    window.location.hash = `agent/${encodeURIComponent(item.role.role_id)}`;
  };
  return (
    <button
      onClick={onClick}
      className="w-full text-left rounded-xl border border-gray-200 dark:border-zinc-800 bg-white dark:bg-zinc-950 hover:border-indigo-500 dark:hover:border-indigo-400 transition-colors px-5 py-4 flex items-start gap-4 focus:outline-none focus-visible:ring-2 focus-visible:ring-indigo-400"
    >
      <div className="flex-1 min-w-0">
        <div className="flex items-center gap-2 flex-wrap">
          <span className="text-[14px] font-semibold text-gray-900 dark:text-zinc-100 font-mono">
            {item.role.role_id}
          </span>
          <AgentRoleBadge source={item.source} />
          <span className="text-[10px] font-mono text-gray-400 dark:text-zinc-600 bg-gray-100 dark:bg-zinc-800 border border-gray-200 dark:border-zinc-700 rounded px-1.5 py-0.5">
            {item.role.tier}
          </span>
          <span className="text-[10px] font-mono text-gray-400 dark:text-zinc-600 bg-gray-100 dark:bg-zinc-800 border border-gray-200 dark:border-zinc-700 rounded px-1.5 py-0.5">
            {item.role.response_shape}
          </span>
        </div>
        <p className="text-[12px] text-gray-600 dark:text-zinc-400 mt-1 leading-relaxed line-clamp-2">
          {item.role.display_name}
          {item.role.description ? ` — ${item.role.description}` : ""}
        </p>
        <div className="flex items-center gap-1.5 mt-2 flex-wrap">
          {item.role.forbid_all_tools ? (
            <span className="flex items-center gap-1 text-[10px] font-mono text-red-500 dark:text-red-400 bg-red-50 dark:bg-red-950/40 border border-red-200 dark:border-red-900 rounded px-1.5 py-0.5">
              forbid_all_tools
            </span>
          ) : item.role.tools.length === 0 ? (
            <span className="text-[10px] text-gray-400 dark:text-zinc-600 italic">
              no allowlist (unrestricted)
            </span>
          ) : (
            item.role.tools.slice(0, 6).map((t) => (
              <span
                key={t}
                className="flex items-center gap-1 text-[10px] font-mono text-gray-500 dark:text-zinc-400 bg-gray-100 dark:bg-zinc-800 border border-gray-200 dark:border-zinc-700 rounded px-1.5 py-0.5"
              >
                <Wrench size={9} className="text-gray-400 dark:text-zinc-600" />
                {t}
              </span>
            ))
          )}
          {item.role.tools.length > 6 && (
            <span className="text-[10px] text-gray-400 dark:text-zinc-600">
              +{item.role.tools.length - 6}
            </span>
          )}
        </div>
      </div>
      {isEditable && item.defined_by && (
        <div className="text-right shrink-0">
          <p className="text-[10px] text-gray-400 dark:text-zinc-600">Defined by</p>
          <p className="text-[11px] font-mono text-gray-500 dark:text-zinc-400">
            {item.defined_by}
          </p>
        </div>
      )}
    </button>
  );
}

export function AgentRolesPage() {
  const [scope] = useScope();
  const { data, isLoading, isError, error } = useQuery({
    queryKey: [
      "agent-roles",
      scope.tenant_id,
      scope.workspace_id,
      scope.project_id,
    ],
    queryFn: () => defaultApi.listAgentRoles("all"),
    staleTime: 30_000,
  });

  const goToNew = () => {
    window.location.hash = "agent-new";
  };

  return (
    <div className="flex flex-col h-full bg-gray-50 dark:bg-zinc-900">
      <div className="flex items-center gap-3 px-5 h-11 border-b border-gray-200 dark:border-zinc-800 shrink-0">
        <span className="text-[13px] font-medium text-gray-800 dark:text-zinc-200">
          Agent roles
        </span>
        <span className="text-[11px] text-gray-400 dark:text-zinc-600">
          {scope.tenant_id} / {scope.workspace_id} / {scope.project_id}
        </span>
        <div className="ml-auto">
          <button
            onClick={goToNew}
            className="flex items-center gap-1.5 px-3 py-1.5 rounded-md bg-indigo-600 hover:bg-indigo-500 text-white text-[12px] font-medium transition-colors shadow-sm focus:outline-none focus-visible:ring-2 focus-visible:ring-indigo-400"
          >
            <Plus size={12} />
            New role
          </button>
        </div>
      </div>

      <div className="flex-1 overflow-y-auto p-5">
        {isLoading ? (
          <div className="flex items-center justify-center min-h-48 gap-2 text-gray-400 dark:text-zinc-600">
            <Loader2 size={16} className="animate-spin" />
            <span className="text-[13px]">Loading roles…</span>
          </div>
        ) : isError ? (
          <div className="text-center py-12">
            <p className="text-[13px] text-red-400">
              Failed to load roles: {error instanceof Error ? error.message : "unknown"}
            </p>
          </div>
        ) : (
          <div className="max-w-5xl space-y-3">
            <div className="rounded-xl border border-indigo-800/40 bg-indigo-950/20 px-5 py-4">
              <p className="text-[13px] font-medium text-indigo-300 mb-1">
                Per-project agent roles (RFC 031)
              </p>
              <p className="text-[12px] text-gray-400 dark:text-zinc-500 leading-relaxed">
                Built-in roles ship as the baseline; any operator-defined role
                with the same id shadows the built-in for this project's runs.
                Retracting a custom role falls back to the built-in (or to the
                generic role for novel ids) per §D7.
              </p>
            </div>
            {(data?.items ?? []).map((item) => (
              <Row key={item.role.role_id} item={item} />
            ))}
            {data && data.items.length === 0 && (
              <p className="text-center text-[13px] text-gray-400 dark:text-zinc-600 py-12">
                No roles found. Click "New role" to define one.
              </p>
            )}
          </div>
        )}
      </div>
    </div>
  );
}

export default AgentRolesPage;
