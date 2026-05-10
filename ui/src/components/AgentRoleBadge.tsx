/**
 * RFC 031 PR-D2 — shared source-badge for agent-role rows.
 *
 * Renders the "builtin / custom / custom_shadow" provenance badge with
 * consistent colour + icon across the list, detail, and (future)
 * history-panel surfaces. Extracted from the duplicated `sourceBadge`
 * helpers PR-D1 inlined in each page.
 */

import { Shield, User, Users } from "lucide-react";
import { clsx } from "clsx";
import type { ReactElement } from "react";

import type { AgentRoleSource } from "../lib/types";

interface BadgeSpec {
  label: string;
  color: string;
  icon: ReactElement;
}

const SPECS: Record<AgentRoleSource, BadgeSpec> = {
  builtin: {
    label: "Built-in",
    color:
      "text-gray-500 dark:text-zinc-400 bg-gray-100/60 dark:bg-zinc-800/60 border-gray-200 dark:border-zinc-700",
    icon: <Shield size={10} />,
  },
  custom: {
    label: "Custom",
    color: "text-indigo-400 bg-indigo-950/40 border-indigo-800/40",
    icon: <User size={10} />,
  },
  custom_shadow: {
    label: "Custom · shadows built-in",
    color: "text-amber-400 bg-amber-950/40 border-amber-800/40",
    icon: <Users size={10} />,
  },
};

interface Props {
  source: AgentRoleSource;
}

export function AgentRoleBadge({ source }: Props) {
  const spec = SPECS[source];
  return (
    <span
      className={clsx(
        "flex items-center gap-1 text-[10px] font-medium px-1.5 py-0.5 rounded border",
        spec.color,
      )}
    >
      {spec.icon}
      {spec.label}
    </span>
  );
}

export default AgentRoleBadge;
