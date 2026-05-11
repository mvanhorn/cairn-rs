/**
 * RFC 031 PR-D3 §Retract-during-active-run confirmation.
 *
 * Surfaces the §D7 guarantee and probes
 * `GET /v1/runs?agent_role_id=<id>&status=running` so the operator
 * sees "N runs are currently using this role" before committing.
 * The probe is best-effort: if the request fails or the backend
 * returns an empty list, the modal falls back to the generic §D7
 * copy rather than blocking the retract. The retract itself is
 * safe either way — running orchestrations are never interrupted.
 */

import { useQuery } from "@tanstack/react-query";
import { Loader2, Trash2, X } from "lucide-react";

import { defaultApi } from "../lib/api";
import type { AgentRoleSource } from "../lib/types";

interface Props {
  roleId: string;
  source: AgentRoleSource;
  isPending: boolean;
  onCancel: () => void;
  onConfirm: () => void;
}

export function AgentRoleRetractModal({
  roleId,
  source,
  isPending,
  onCancel,
  onConfirm,
}: Props) {
  const isShadow = source === "custom_shadow";
  const title = isShadow ? "Restore built-in?" : "Retract role?";
  const verb = isShadow ? "Restore built-in" : "Retract";
  const action = isShadow
    ? `This project will stop shadowing the built-in \`${roleId}\`. New runs fall back to the built-in role.`
    : `\`${roleId}\` will be retracted from this project. New runs fall back to the built-in if the id shadows one, or to the generic role otherwise (RFC 031 §D7).`;

  // RFC 031 PR-D3 — probe in-flight runs. `refetchOnWindowFocus`
  // is off so opening the modal shows a stable number; the probe
  // re-runs only if the modal is re-mounted. Errors are swallowed —
  // the §D7 guarantee holds either way.
  const runsProbe = useQuery({
    queryKey: ["runs", "by-agent-role", roleId, "running"],
    queryFn: () =>
      defaultApi.getRuns({ agent_role_id: roleId, status: "running" }),
    refetchOnWindowFocus: false,
    retry: false,
  });
  const activeCount = runsProbe.data?.length ?? 0;
  const probeReady = !runsProbe.isLoading && !runsProbe.isError;
  return (
    <>
      <div className="fixed inset-0 z-40 bg-black/70" onClick={onCancel} />
      <div className="fixed inset-0 z-50 flex items-center justify-center p-4">
        <div className="w-full max-w-lg bg-white dark:bg-zinc-950 border border-gray-200 dark:border-zinc-800 rounded-xl shadow-2xl flex flex-col">
          <div className="flex items-center justify-between px-5 py-4 border-b border-gray-200 dark:border-zinc-800">
            <div className="flex items-center gap-2">
              <Trash2 size={16} className="text-red-500 dark:text-red-400" />
              <p className="text-[13px] font-semibold text-gray-900 dark:text-zinc-100">
                {title}
              </p>
            </div>
            <button
              onClick={onCancel}
              className="text-gray-400 dark:text-zinc-600 hover:text-gray-500 dark:hover:text-zinc-400 transition-colors"
              aria-label="Cancel"
            >
              <X size={16} />
            </button>
          </div>

          <div className="px-5 py-4 space-y-3 text-[12px] text-gray-600 dark:text-zinc-300 leading-relaxed">
            <p>{action}</p>
            <div className="rounded-lg bg-amber-50 dark:bg-amber-950/40 border border-amber-200 dark:border-amber-900 px-3 py-2">
              <p
                className="text-[11px] text-amber-800 dark:text-amber-200"
                data-testid="agent-role-retract-guarantee"
              >
                <span className="font-semibold">§D7 guarantee:</span>{" "}
                {probeReady && activeCount > 0 ? (
                  <>
                    <span
                      className="font-semibold"
                      data-testid="agent-role-retract-active-count"
                    >
                      {activeCount} run{activeCount === 1 ? "" : "s"}
                    </span>{" "}
                    currently using this role will continue to completion with the
                    retired prompt. Only new runs see the fallback — running
                    orchestrations are never interrupted.
                  </>
                ) : (
                  <>
                    running orchestrations are never interrupted. Any run currently
                    using this role continues to completion with the retired prompt;
                    only new runs see the fallback.
                  </>
                )}
              </p>
            </div>
            <p className="text-[11px] text-gray-500 dark:text-zinc-500">
              The retract is reversible: a re-POST of the same id clears{" "}
              <span className="font-mono">retracted_at</span> atomically (§D6).
            </p>
          </div>

          <div className="flex items-center justify-end gap-2 px-5 py-3 border-t border-gray-200 dark:border-zinc-800 bg-gray-50/50 dark:bg-zinc-900/50 rounded-b-xl">
            <button
              onClick={onCancel}
              disabled={isPending}
              data-testid="agent-role-retract-cancel-btn"
              className="px-3 py-1.5 rounded-md border border-gray-300 dark:border-zinc-700 text-gray-600 dark:text-zinc-400 hover:bg-gray-100 dark:hover:bg-zinc-800 text-[12px] font-medium transition-colors disabled:opacity-50"
            >
              Keep role
            </button>
            <button
              onClick={onConfirm}
              disabled={isPending}
              data-testid="agent-role-retract-confirm-btn"
              className="flex items-center gap-1 px-3 py-1.5 rounded-md bg-red-600 hover:bg-red-500 text-white text-[12px] font-medium transition-colors shadow-sm disabled:opacity-50"
            >
              {isPending ? (
                <Loader2 size={12} className="animate-spin" />
              ) : (
                <Trash2 size={12} />
              )}
              {verb}
            </button>
          </div>
        </div>
      </div>
    </>
  );
}

export default AgentRoleRetractModal;
