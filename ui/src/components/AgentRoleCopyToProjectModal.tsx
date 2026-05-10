/**
 * RFC 031 PR-D3 §Copy to project.
 *
 * Lets an operator clone the current role into a different project
 * scope. The modal collects target tenant/workspace/project +
 * (optionally) a new role id, then fires POST against the target.
 *
 * Conflict resolution (RFC §Copy to project):
 *   - 201 → toast success + navigate to the new role's detail page.
 *   - 409 (target already has an active role with the same id) →
 *     offer [Overwrite] (PATCH with the target's current ETag) or
 *     [Rename in target] (prefill `{id}-copy` and re-submit).
 *   - 422 → render the server's validation failures inline.
 *   - 403 / 404 → plain-text error.
 *
 * Tenant / workspace / project selectors are deliberately simple text
 * inputs — the full typeahead against `GET /v1/projects` described in
 * the RFC is a v1.1 enhancement. Copying to a project the operator
 * doesn't have access to surfaces as a 403 from the server.
 */

import { useState } from "react";
import { useMutation } from "@tanstack/react-query";
import { Copy, Loader2, X } from "lucide-react";

import { defaultApi, ApiError } from "../lib/api";
import { useToast } from "./Toast";
import { useScope } from "../hooks/useScope";
import type { AgentRole } from "../lib/types";

interface Props {
  role: AgentRole;
  onClose: () => void;
}

interface Target {
  tenant_id: string;
  workspace_id: string;
  project_id: string;
  role_id: string;
}

type Step = "form" | "conflict";

export function AgentRoleCopyToProjectModal({ role, onClose }: Props) {
  const toast = useToast();
  const [scope] = useScope();
  const [step, setStep] = useState<Step>("form");
  const [target, setTarget] = useState<Target>({
    tenant_id: scope.tenant_id,
    workspace_id: scope.workspace_id,
    project_id: scope.project_id,
    role_id: role.role_id,
  });
  const [conflictEtag, setConflictEtag] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  const sameProject =
    target.tenant_id === scope.tenant_id &&
    target.workspace_id === scope.workspace_id &&
    target.project_id === scope.project_id;
  const sameId = target.role_id === role.role_id;
  const canSubmit =
    target.tenant_id.trim() !== "" &&
    target.workspace_id.trim() !== "" &&
    target.project_id.trim() !== "" &&
    target.role_id.trim() !== "" &&
    !(sameProject && sameId);

  const createMut = useMutation({
    mutationFn: () =>
      defaultApi.createAgentRole(
        {
          id: target.role_id,
          name: role.display_name,
          tier: role.tier,
          description: role.description || undefined,
          system_prompt: role.system_prompt ?? "",
          tools: role.tools,
          forbid_all_tools: role.forbid_all_tools,
          max_context_tokens: role.max_context_tokens ?? undefined,
          response_shape: role.response_shape,
        },
        {
          tenant_id: target.tenant_id,
          workspace_id: target.workspace_id,
          project_id: target.project_id,
        },
      ),
    onSuccess: (r) => {
      toast.success(
        `Copied ${role.role_id} → ${target.tenant_id}/${target.workspace_id}/${target.project_id}/${r.result.role.role_id}.`,
      );
      onClose();
    },
    onError: async (err) => {
      if (err instanceof ApiError && err.status === 409) {
        // Fetch the target's current ETag so an Overwrite can set If-Match.
        try {
          const existing = await defaultApi.getAgentRole(target.role_id, {
            tenant_id: target.tenant_id,
            workspace_id: target.workspace_id,
            project_id: target.project_id,
          });
          setConflictEtag(existing.etag);
        } catch {
          setConflictEtag(null);
        }
        setStep("conflict");
        return;
      }
      if (err instanceof ApiError) {
        setError(`${err.code}: ${err.message}`);
        return;
      }
      setError(err instanceof Error ? err.message : "Copy failed");
    },
  });

  const overwriteMut = useMutation({
    mutationFn: () =>
      defaultApi.patchAgentRole(
        target.role_id,
        {
          name: role.display_name,
          description: role.description,
          system_prompt: role.system_prompt ?? "",
          tools: role.tools,
          forbid_all_tools: role.forbid_all_tools,
          max_context_tokens: role.max_context_tokens,
          response_shape: role.response_shape,
        },
        conflictEtag,
        {
          tenant_id: target.tenant_id,
          workspace_id: target.workspace_id,
          project_id: target.project_id,
        },
      ),
    onSuccess: (r) => {
      toast.success(
        `Overwrote ${target.tenant_id}/${target.workspace_id}/${target.project_id}/${r.result.role.role_id}.`,
      );
      onClose();
    },
    onError: async (err) => {
      if (err instanceof ApiError && err.status === 412) {
        // Target ETag drifted between our GET and PATCH. Re-fetch and
        // let the operator reconfirm.
        try {
          const latest = await defaultApi.getAgentRole(target.role_id, {
            tenant_id: target.tenant_id,
            workspace_id: target.workspace_id,
            project_id: target.project_id,
          });
          setConflictEtag(latest.etag);
          setError(
            "Target role changed in another tab. Reload to see the latest and retry.",
          );
        } catch {
          setError("Target role changed and could not be re-fetched. Cancel and retry.");
        }
        return;
      }
      if (err instanceof ApiError) {
        setError(`${err.code}: ${err.message}`);
        return;
      }
      setError(err instanceof Error ? err.message : "Overwrite failed");
    },
  });

  const busy = createMut.isPending || overwriteMut.isPending;

  return (
    <>
      <div className="fixed inset-0 z-40 bg-black/70" onClick={onClose} />
      <div className="fixed inset-0 z-50 flex items-center justify-center p-4">
        <div className="w-full max-w-xl bg-white dark:bg-zinc-950 border border-gray-200 dark:border-zinc-800 rounded-xl shadow-2xl flex flex-col">
          <div className="flex items-center justify-between px-5 py-4 border-b border-gray-200 dark:border-zinc-800">
            <div className="flex items-center gap-2">
              <Copy size={16} className="text-indigo-400" />
              <p className="text-[13px] font-semibold text-gray-900 dark:text-zinc-100">
                {step === "form" ? "Copy role to project" : "Target project has this role"}
              </p>
            </div>
            <button
              onClick={onClose}
              className="text-gray-400 dark:text-zinc-600 hover:text-gray-500 dark:hover:text-zinc-400 transition-colors"
              aria-label="Close"
            >
              <X size={16} />
            </button>
          </div>

          {step === "form" && (
            <div className="px-5 py-4 space-y-3">
              <p className="text-[12px] text-gray-600 dark:text-zinc-400 leading-relaxed">
                Copies <span className="font-mono">{role.role_id}</span> into the target
                project. Tenant and workspace default to your current scope; change any
                field to target a different location.
              </p>
              <div className="grid grid-cols-1 sm:grid-cols-3 gap-2">
                <Field
                  label="Tenant"
                  value={target.tenant_id}
                  onChange={(v) => setTarget((t) => ({ ...t, tenant_id: v }))}
                />
                <Field
                  label="Workspace"
                  value={target.workspace_id}
                  onChange={(v) => setTarget((t) => ({ ...t, workspace_id: v }))}
                />
                <Field
                  label="Project"
                  value={target.project_id}
                  onChange={(v) => setTarget((t) => ({ ...t, project_id: v }))}
                />
              </div>
              <Field
                label="Role id in target"
                value={target.role_id}
                onChange={(v) => setTarget((t) => ({ ...t, role_id: v }))}
                hint="Rename here to dodge an existing id in the target (e.g. `pr-reviewer-valkey` → `pr-reviewer-glide`)."
              />
              {sameProject && sameId && (
                <p className="text-[11px] text-amber-600 dark:text-amber-400">
                  Target is the current project and role id. Change at least one field to copy.
                </p>
              )}
              {error && (
                <p className="text-[11px] text-red-500 dark:text-red-400">{error}</p>
              )}
            </div>
          )}

          {step === "conflict" && (
            <div className="px-5 py-4 space-y-3">
              <p className="text-[12px] text-gray-600 dark:text-zinc-400 leading-relaxed">
                The target project already has an active role with id{" "}
                <span className="font-mono">{target.role_id}</span>. Pick how to resolve:
              </p>
              <div className="rounded-lg bg-gray-50 dark:bg-zinc-900 border border-gray-200 dark:border-zinc-800 px-3 py-2 space-y-1 text-[11px] font-mono text-gray-600 dark:text-zinc-400">
                <p>Source: {scope.tenant_id}/{scope.workspace_id}/{scope.project_id}/{role.role_id}</p>
                <p>Target: {target.tenant_id}/{target.workspace_id}/{target.project_id}/{target.role_id}</p>
                {conflictEtag && <p>Target ETag: {conflictEtag}</p>}
              </div>
              <p className="text-[11px] text-gray-500 dark:text-zinc-500">
                Overwriting sends a PATCH to the target with the body of this role; the
                target's existing definition is replaced.
              </p>
              {error && (
                <p className="text-[11px] text-red-500 dark:text-red-400">{error}</p>
              )}
            </div>
          )}

          <div className="flex items-center justify-end gap-2 px-5 py-3 border-t border-gray-200 dark:border-zinc-800 bg-gray-50/50 dark:bg-zinc-900/50 rounded-b-xl">
            {step === "form" ? (
              <>
                <button
                  onClick={onClose}
                  disabled={busy}
                  className="px-3 py-1.5 rounded-md border border-gray-300 dark:border-zinc-700 text-gray-600 dark:text-zinc-400 hover:bg-gray-100 dark:hover:bg-zinc-800 text-[12px] font-medium transition-colors disabled:opacity-50"
                >
                  Cancel
                </button>
                <button
                  onClick={() => {
                    setError(null);
                    createMut.mutate();
                  }}
                  disabled={!canSubmit || busy}
                  className="flex items-center gap-1 px-3 py-1.5 rounded-md bg-indigo-600 hover:bg-indigo-500 text-white text-[12px] font-medium transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
                >
                  {createMut.isPending ? (
                    <Loader2 size={12} className="animate-spin" />
                  ) : (
                    <Copy size={12} />
                  )}
                  Copy
                </button>
              </>
            ) : (
              <>
                <button
                  onClick={onClose}
                  disabled={busy}
                  className="px-3 py-1.5 rounded-md border border-gray-300 dark:border-zinc-700 text-gray-600 dark:text-zinc-400 hover:bg-gray-100 dark:hover:bg-zinc-800 text-[12px] font-medium transition-colors disabled:opacity-50"
                >
                  Cancel
                </button>
                <button
                  onClick={() => {
                    setError(null);
                    setStep("form");
                    setTarget((t) => ({ ...t, role_id: `${t.role_id}-copy` }));
                  }}
                  disabled={busy}
                  className="px-3 py-1.5 rounded-md border border-indigo-400 dark:border-indigo-700 text-indigo-600 dark:text-indigo-400 hover:bg-indigo-50 dark:hover:bg-indigo-950/40 text-[12px] font-medium transition-colors disabled:opacity-50"
                >
                  Rename & retry
                </button>
                <button
                  onClick={() => {
                    setError(null);
                    overwriteMut.mutate();
                  }}
                  disabled={busy}
                  className="flex items-center gap-1 px-3 py-1.5 rounded-md bg-red-600 hover:bg-red-500 text-white text-[12px] font-medium transition-colors disabled:opacity-50"
                >
                  {overwriteMut.isPending ? (
                    <Loader2 size={12} className="animate-spin" />
                  ) : (
                    <Copy size={12} />
                  )}
                  Overwrite target
                </button>
              </>
            )}
          </div>
        </div>
      </div>
    </>
  );
}

interface FieldProps {
  label: string;
  value: string;
  onChange: (v: string) => void;
  hint?: string;
}

function Field({ label, value, onChange, hint }: FieldProps) {
  return (
    <div>
      <label className="block text-[10px] text-gray-400 dark:text-zinc-500 uppercase tracking-wide mb-1">
        {label}
      </label>
      <input
        type="text"
        value={value}
        onChange={(e) => onChange(e.target.value)}
        className="w-full rounded-md bg-gray-50 dark:bg-zinc-900 border border-gray-200 dark:border-zinc-700 px-2.5 py-1.5 text-[12px] font-mono text-gray-800 dark:text-zinc-200 focus:outline-none focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500/30 transition-colors"
      />
      {hint && (
        <p className="text-[10px] text-gray-400 dark:text-zinc-600 mt-0.5">{hint}</p>
      )}
    </div>
  );
}

export default AgentRoleCopyToProjectModal;
