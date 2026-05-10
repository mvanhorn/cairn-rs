/**
 * RFC 031 PR-D — Agent Role detail page.
 *
 * Read-only view of a single role, with Edit and Retract actions when
 * the row is operator-defined (source: custom | custom_shadow).
 * Built-in rows show the assembled prompt without edit affordances.
 *
 * PR-D3 lands:
 *   - History panel with the full AgentRoleDefined / AgentRoleRetracted
 *     timeline and inter-snapshot change summaries.
 *   - Retract-confirmation modal (replaces the PR-D1 `window.confirm`)
 *     with the full §D7 explanation + explicit two-button choice.
 *   - Copy-to-project modal that targets another project scope.
 */

import { useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { ArrowLeft, Copy, Edit2, Loader2, Trash2, Wrench } from "lucide-react";

import { defaultApi, ApiError } from "../lib/api";
import { useToast } from "../components/Toast";
import { useScope } from "../hooks/useScope";
import { AgentRoleBadge } from "../components/AgentRoleBadge";
import { AgentRoleHistoryPanel } from "../components/AgentRoleHistoryPanel";
import { AgentRoleRetractModal } from "../components/AgentRoleRetractModal";
import { AgentRoleCopyToProjectModal } from "../components/AgentRoleCopyToProjectModal";

interface Props {
  roleId: string;
}

export function AgentRoleDetailPage({ roleId }: Props) {
  const toast = useToast();
  const qc = useQueryClient();
  const [scope] = useScope();
  const [retractModalOpen, setRetractModalOpen] = useState(false);
  const [copyModalOpen, setCopyModalOpen] = useState(false);

  const { data, isLoading, isError, error } = useQuery({
    queryKey: [
      "agent-role",
      scope.tenant_id,
      scope.workspace_id,
      scope.project_id,
      roleId,
    ],
    queryFn: () => defaultApi.getAgentRole(roleId),
    retry: (n, err) => (err instanceof ApiError && err.status === 404 ? false : n < 2),
  });

  const retractMut = useMutation({
    mutationFn: () => defaultApi.retractAgentRole(roleId),
    onSuccess: () => {
      toast.success(`Retracted ${roleId}. New runs fall back to the built-in / generic.`);
      qc.invalidateQueries({ queryKey: ["agent-role"] });
      qc.invalidateQueries({ queryKey: ["agent-roles"] });
      window.location.hash = "agents";
    },
    onError: (e) => {
      toast.error(e instanceof Error ? e.message : "Retract failed");
    },
  });

  if (isLoading) {
    return (
      <div className="flex items-center justify-center h-full gap-2 text-gray-400 dark:text-zinc-600">
        <Loader2 size={16} className="animate-spin" />
        <span className="text-[13px]">Loading role…</span>
      </div>
    );
  }

  if (isError || !data) {
    const is404 = error instanceof ApiError && error.status === 404;
    return (
      <div className="flex flex-col items-center justify-center h-full gap-3 text-gray-400 dark:text-zinc-600">
        <p className="text-[13px] text-red-400">
          {is404
            ? `Role \`${roleId}\` does not exist in this project.`
            : `Failed to load: ${error instanceof Error ? error.message : "unknown"}`}
        </p>
        <button
          onClick={() => (window.location.hash = "agents")}
          className="text-[12px] text-indigo-400 hover:text-indigo-300"
        >
          ← Back to roles
        </button>
      </div>
    );
  }

  const { item, etag } = data;
  const role = item.role;
  const isEditable = item.source === "custom" || item.source === "custom_shadow";

  const onEdit = () => {
    window.location.hash = `agent-edit/${encodeURIComponent(roleId)}`;
  };

  const onRetract = () => setRetractModalOpen(true);

  return (
    <div className="flex flex-col h-full bg-gray-50 dark:bg-zinc-900">
      <div className="flex items-center gap-3 px-5 h-11 border-b border-gray-200 dark:border-zinc-800 shrink-0">
        <button
          onClick={() => (window.location.hash = "agents")}
          className="flex items-center gap-1 text-[12px] text-gray-400 dark:text-zinc-500 hover:text-gray-700 dark:hover:text-zinc-300"
        >
          <ArrowLeft size={12} />
          Roles
        </button>
        <span className="text-[13px] font-medium text-gray-800 dark:text-zinc-200 font-mono">
          {role.role_id}
        </span>
        <AgentRoleBadge source={item.source} />
        {etag && (
          <span
            className="text-[10px] font-mono text-gray-400 dark:text-zinc-600"
            title="ETag used for If-Match on PATCH"
          >
            ETag {etag}
          </span>
        )}
        <div className="ml-auto flex items-center gap-2">
          <button
            onClick={() => setCopyModalOpen(true)}
            className="flex items-center gap-1 px-2 py-1 rounded-md border border-gray-300 dark:border-zinc-700 text-gray-600 dark:text-zinc-400 hover:bg-gray-100 dark:hover:bg-zinc-800 text-[11px] font-medium transition-colors"
            title="Copy this role into another project"
          >
            <Copy size={11} />
            Copy to project…
          </button>
          {isEditable && (
            <>
              <button
                onClick={onRetract}
                disabled={retractMut.isPending}
                className="flex items-center gap-1 px-2 py-1 rounded-md border border-red-400 dark:border-red-600 text-red-500 dark:text-red-400 hover:bg-red-50 dark:hover:bg-red-950/40 text-[11px] font-medium transition-colors disabled:opacity-50"
              >
                {retractMut.isPending ? (
                  <Loader2 size={11} className="animate-spin" />
                ) : (
                  <Trash2 size={11} />
                )}
                {item.source === "custom_shadow" ? "Restore built-in" : "Retract"}
              </button>
              <button
                onClick={onEdit}
                className="flex items-center gap-1 px-2 py-1 rounded-md bg-indigo-600 hover:bg-indigo-500 text-white text-[11px] font-medium transition-colors"
              >
                <Edit2 size={11} />
                Edit
              </button>
            </>
          )}
        </div>
      </div>

      <div className="flex-1 overflow-y-auto p-5">
        <div className="max-w-5xl space-y-5">
          {/* Metadata card */}
          <div className="rounded-xl border border-gray-200 dark:border-zinc-800 bg-white dark:bg-zinc-950 p-5">
            <h2 className="text-[12px] font-semibold text-gray-800 dark:text-zinc-200 mb-3 uppercase tracking-wide">
              Metadata
            </h2>
            <dl className="grid grid-cols-1 md:grid-cols-2 gap-3 text-[12px]">
              <div>
                <dt className="text-gray-400 dark:text-zinc-600">Display name</dt>
                <dd className="text-gray-800 dark:text-zinc-200">{role.display_name}</dd>
              </div>
              <div>
                <dt className="text-gray-400 dark:text-zinc-600">Tier</dt>
                <dd className="text-gray-800 dark:text-zinc-200 font-mono">{role.tier}</dd>
              </div>
              <div>
                <dt className="text-gray-400 dark:text-zinc-600">Response shape</dt>
                <dd className="text-gray-800 dark:text-zinc-200 font-mono">
                  {role.response_shape}
                </dd>
              </div>
              <div>
                <dt className="text-gray-400 dark:text-zinc-600">Max context tokens</dt>
                <dd className="text-gray-800 dark:text-zinc-200 font-mono">
                  {role.max_context_tokens ?? <span className="italic">role-tier default</span>}
                </dd>
              </div>
              {item.shadows_builtin && (
                <div>
                  <dt className="text-gray-400 dark:text-zinc-600">Shadows built-in</dt>
                  <dd className="text-gray-800 dark:text-zinc-200 font-mono">
                    {item.shadows_builtin}
                  </dd>
                </div>
              )}
              {item.defined_at && (
                <div>
                  <dt className="text-gray-400 dark:text-zinc-600">Defined</dt>
                  <dd className="text-gray-800 dark:text-zinc-200">
                    {new Date(item.defined_at).toISOString()}
                    {item.defined_by ? ` · ${item.defined_by}` : ""}
                  </dd>
                </div>
              )}
              <div className="md:col-span-2">
                <dt className="text-gray-400 dark:text-zinc-600">Description</dt>
                <dd className="text-gray-800 dark:text-zinc-200 leading-relaxed">
                  {role.description || (
                    <span className="italic text-gray-400 dark:text-zinc-600">
                      No description.
                    </span>
                  )}
                </dd>
              </div>
            </dl>
          </div>

          {/* Tools card */}
          <div className="rounded-xl border border-gray-200 dark:border-zinc-800 bg-white dark:bg-zinc-950 p-5">
            <h2 className="text-[12px] font-semibold text-gray-800 dark:text-zinc-200 mb-3 uppercase tracking-wide">
              Tool allowlist
            </h2>
            {role.forbid_all_tools ? (
              <p className="text-[12px] text-red-500 dark:text-red-400">
                <span className="font-mono">forbid_all_tools = true</span> — role runs with an
                empty tool set regardless of <span className="font-mono">tools[]</span>.
              </p>
            ) : role.tools.length === 0 ? (
              <p className="text-[12px] text-gray-400 dark:text-zinc-600">
                No allowlist (unrestricted — every registered tool is available, §D3 default).
              </p>
            ) : (
              <div className="flex flex-wrap gap-1.5">
                {role.tools.map((t) => (
                  <span
                    key={t}
                    className="flex items-center gap-1 text-[11px] font-mono text-gray-500 dark:text-zinc-400 bg-gray-100 dark:bg-zinc-800 border border-gray-200 dark:border-zinc-700 rounded px-2 py-1"
                  >
                    <Wrench size={10} className="text-gray-400 dark:text-zinc-600" />
                    {t}
                  </span>
                ))}
              </div>
            )}
          </div>

          {/* System prompt card */}
          <div className="rounded-xl border border-gray-200 dark:border-zinc-800 bg-white dark:bg-zinc-950 p-5">
            <h2 className="text-[12px] font-semibold text-gray-800 dark:text-zinc-200 mb-3 uppercase tracking-wide">
              System prompt
            </h2>
            <pre className="whitespace-pre-wrap text-[11px] font-mono text-gray-700 dark:text-zinc-300 bg-gray-50 dark:bg-zinc-900 border border-gray-200 dark:border-zinc-800 rounded-lg p-3 max-h-[600px] overflow-y-auto leading-relaxed">
              {role.system_prompt || (
                <span className="italic text-gray-400 dark:text-zinc-600">
                  No specialty overlay — assembled prompt falls back to the base sub-agent
                  prompt only.
                </span>
              )}
            </pre>
          </div>

          {/* RFC 031 PR-D3 — History panel. */}
          <div className="rounded-xl border border-gray-200 dark:border-zinc-800 bg-white dark:bg-zinc-950 p-5">
            <h2 className="text-[12px] font-semibold text-gray-800 dark:text-zinc-200 mb-3 uppercase tracking-wide">
              History
            </h2>
            <AgentRoleHistoryPanel roleId={roleId} />
          </div>
        </div>
      </div>

      {/* RFC 031 PR-D3 — retract-confirmation modal. Replaces the
           PR-D1 `window.confirm` with an explicit two-button choice
           and the full §D7 explanation, so the operator sees the
           consequence ("running orchestrations never interrupted;
           new runs fall back") before committing. */}
      {retractModalOpen && (
        <AgentRoleRetractModal
          roleId={roleId}
          source={item.source}
          onCancel={() => setRetractModalOpen(false)}
          onConfirm={() => {
            setRetractModalOpen(false);
            retractMut.mutate();
          }}
          isPending={retractMut.isPending}
        />
      )}

      {/* RFC 031 PR-D3 — copy-to-project modal. */}
      {copyModalOpen && (
        <AgentRoleCopyToProjectModal
          role={role}
          onClose={() => setCopyModalOpen(false)}
        />
      )}
    </div>
  );
}

export default AgentRoleDetailPage;
