/**
 * RFC 031 PR-D3 §Retract-during-active-run confirmation.
 *
 * The RFC spec prescribed a server probe
 * (`/v1/projects/:project/runs?agent_role_id=X&state=active`) that
 * would surface "N runs are currently using this role" before the
 * retract. That endpoint does not yet exist on the server and adding
 * it is larger than PR-D3's scope. The §D7 contract is stable
 * regardless of how many runs are in flight — retracts never
 * interrupt running orchestrations — so the modal surfaces that
 * guarantee directly as the confirmation copy. When the runs-by-
 * role-id probe lands, add the N-runs count to the banner.
 */

import { Loader2, Trash2, X } from "lucide-react";

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
              <p className="text-[11px] text-amber-800 dark:text-amber-200">
                <span className="font-semibold">§D7 guarantee:</span>{" "}
                running orchestrations are never interrupted. Any run currently using this
                role continues to completion with the retired prompt; only new runs see the
                fallback.
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
              className="px-3 py-1.5 rounded-md border border-gray-300 dark:border-zinc-700 text-gray-600 dark:text-zinc-400 hover:bg-gray-100 dark:hover:bg-zinc-800 text-[12px] font-medium transition-colors disabled:opacity-50"
            >
              Keep role
            </button>
            <button
              onClick={onConfirm}
              disabled={isPending}
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
