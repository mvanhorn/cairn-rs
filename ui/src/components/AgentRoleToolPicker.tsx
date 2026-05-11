/**
 * RFC 031 role editor — tool-allowlist multi-select backed by #799.
 *
 * Loads the per-project tool inventory from
 * `GET /v1/projects/:project/tools`. When the endpoint returns tools
 * the picker renders checkbox rows grouped by `source` (builtin /
 * plugin:<id>) with a `tier` chip. When the endpoint returns an empty
 * list (fresh project, no plugins) it falls back to the legacy
 * freehand input so the operator isn't blocked.
 *
 * The RFC's inline guidance lives on the freehand fallback — the
 * picker's presence makes the guidance unnecessary for the typical
 * case where the tool registry is wired at boot.
 */

import { useMemo, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { Loader2, Wrench, X } from "lucide-react";
import { clsx } from "clsx";

import { defaultApi } from "../lib/api";
import { useScope } from "../hooks/useScope";
import type { ProjectToolItem } from "../lib/types";

interface Props {
  /** Comma/whitespace-separated tool ids (the form-state shape). */
  value: string;
  onChange: (next: string) => void;
  disabled?: boolean;
}

function parseIds(raw: string): string[] {
  return raw
    .split(/[\s,]+/)
    .map((t) => t.trim())
    .filter((t) => t.length > 0);
}

function serialiseIds(ids: Iterable<string>): string {
  return Array.from(ids).join(", ");
}

function tierTint(tier: ProjectToolItem["tier"]): string {
  switch (tier) {
    case "core":
      return "text-indigo-600 dark:text-indigo-400 bg-indigo-50 dark:bg-indigo-950/40 border-indigo-200 dark:border-indigo-900";
    case "registered":
      return "text-emerald-600 dark:text-emerald-400 bg-emerald-50 dark:bg-emerald-950/40 border-emerald-200 dark:border-emerald-900";
    case "deferred":
      return "text-gray-600 dark:text-zinc-400 bg-gray-100 dark:bg-zinc-800 border-gray-200 dark:border-zinc-700";
  }
}

export function AgentRoleToolPicker({ value, onChange, disabled }: Props) {
  const [scope] = useScope();
  const [search, setSearch] = useState("");
  const query = useQuery({
    queryKey: [
      "project-tools",
      scope.tenant_id,
      scope.workspace_id,
      scope.project_id,
    ],
    queryFn: () => defaultApi.listProjectTools(),
    staleTime: 60_000,
  });

  const selected = useMemo(() => new Set(parseIds(value)), [value]);

  const toggle = (id: string) => {
    const next = new Set(selected);
    if (next.has(id)) next.delete(id);
    else next.add(id);
    onChange(serialiseIds(next));
  };

  const clearAll = () => onChange("");

  // Loading + error / empty → freehand fallback.
  const items = query.data?.items ?? [];
  const haveInventory = !query.isLoading && !query.isError && items.length > 0;

  if (!haveInventory) {
    const hint = query.isLoading
      ? "Loading project tool inventory…"
      : query.isError
        ? "Tool inventory unavailable; enter ids manually."
        : "Fresh project with no registered tools yet; enter ids manually.";
    return (
      <>
        <textarea
          value={value}
          onChange={(e) => onChange(e.target.value)}
          disabled={disabled}
          rows={3}
          placeholder="grep, read, bash, post_inline_comment"
          className={clsx(
            "w-full rounded-lg border px-3 py-2 text-[12px] font-mono resize-none focus:outline-none focus:ring-1 transition-colors",
            disabled
              ? "bg-gray-100 dark:bg-zinc-800 border-gray-200 dark:border-zinc-700 text-gray-400 dark:text-zinc-600 cursor-not-allowed"
              : "bg-gray-50 dark:bg-zinc-900 border-gray-200 dark:border-zinc-700 text-gray-800 dark:text-zinc-200 focus:border-indigo-500 focus:ring-indigo-500/30",
          )}
        />
        <p className="text-[11px] text-gray-400 dark:text-zinc-600 mt-1">
          {query.isLoading ? (
            <span className="inline-flex items-center gap-1">
              <Loader2 size={10} className="animate-spin" />
              {hint}
            </span>
          ) : (
            hint
          )}{" "}
          Comma- or whitespace-separated tool ids; empty = no restriction (§D3).
          Unknown ids emit <span className="font-mono">ToolDeclaredButMissing</span>{" "}
          at DECIDE time.
        </p>
      </>
    );
  }

  // Inventory present — picker UI.
  const lowered = search.trim().toLowerCase();
  const filtered = lowered
    ? items.filter(
        (t) =>
          t.id.toLowerCase().includes(lowered) ||
          t.description.toLowerCase().includes(lowered),
      )
    : items;

  // Group by source for stable rendering.
  const groups = new Map<string, ProjectToolItem[]>();
  for (const t of filtered) {
    const bucket = groups.get(t.source) ?? [];
    bucket.push(t);
    groups.set(t.source, bucket);
  }
  const groupOrder = Array.from(groups.keys()).sort((a, b) => {
    // Built-ins first, then plugin:<id> alphabetically.
    if (a === "builtin") return -1;
    if (b === "builtin") return 1;
    return a.localeCompare(b);
  });

  // Unknown ids the operator has in the current value but the
  // registry doesn't know about (stale allowlist). Show as a separate
  // chip row so they can be removed.
  const knownIds = new Set(items.map((t) => t.id));
  const strangers = Array.from(selected).filter((id) => !knownIds.has(id));

  return (
    <div
      className={clsx(
        "rounded-lg border bg-gray-50 dark:bg-zinc-900 border-gray-200 dark:border-zinc-700",
        disabled && "opacity-60 pointer-events-none",
      )}
    >
      <div className="flex items-center gap-2 px-2 py-1.5 border-b border-gray-200 dark:border-zinc-800">
        <input
          type="search"
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          placeholder="Filter by id or description"
          className="flex-1 bg-transparent text-[12px] text-gray-800 dark:text-zinc-200 placeholder-gray-400 dark:placeholder-zinc-600 focus:outline-none"
        />
        <span className="text-[10px] text-gray-400 dark:text-zinc-600 font-mono">
          {selected.size}/{items.length}
        </span>
        {selected.size > 0 && (
          <button
            type="button"
            onClick={clearAll}
            className="text-[11px] text-gray-500 dark:text-zinc-400 hover:text-red-500 dark:hover:text-red-400 flex items-center gap-1"
          >
            <X size={11} />
            Clear
          </button>
        )}
      </div>

      <div className="max-h-64 overflow-y-auto divide-y divide-gray-200 dark:divide-zinc-800">
        {groupOrder.map((src) => (
          <div key={src}>
            <p className="text-[10px] uppercase tracking-wide text-gray-400 dark:text-zinc-600 px-3 py-1 bg-gray-100/60 dark:bg-zinc-900/60">
              {src === "builtin" ? "Built-in" : src}
            </p>
            {groups.get(src)!.map((t) => (
              <label
                key={`${src}/${t.id}`}
                className="flex items-start gap-2 px-3 py-2 hover:bg-indigo-50/40 dark:hover:bg-indigo-950/20 cursor-pointer"
              >
                <input
                  type="checkbox"
                  checked={selected.has(t.id)}
                  onChange={() => toggle(t.id)}
                  disabled={disabled}
                  className="mt-0.5"
                />
                <div className="flex-1 min-w-0">
                  <div className="flex items-center gap-2 flex-wrap">
                    <span className="text-[12px] font-mono text-gray-800 dark:text-zinc-200">
                      {t.id}
                    </span>
                    <span
                      className={clsx(
                        "text-[10px] font-mono px-1.5 py-0.5 rounded border",
                        tierTint(t.tier),
                      )}
                    >
                      {t.tier}
                    </span>
                  </div>
                  <p className="text-[11px] text-gray-500 dark:text-zinc-500 leading-relaxed">
                    {t.description || (
                      <span className="italic text-gray-400 dark:text-zinc-600">
                        No description.
                      </span>
                    )}
                  </p>
                </div>
              </label>
            ))}
          </div>
        ))}
        {filtered.length === 0 && (
          <p className="text-[11px] text-gray-400 dark:text-zinc-600 italic px-3 py-3">
            No tools match the filter.
          </p>
        )}
      </div>

      {strangers.length > 0 && (
        <div className="px-2 py-1.5 border-t border-amber-300 dark:border-amber-700 bg-amber-50/40 dark:bg-amber-950/30">
          <p className="text-[10px] uppercase tracking-wide text-amber-700 dark:text-amber-300 mb-1">
            Declared but not in this project's registry
          </p>
          <div className="flex flex-wrap gap-1.5">
            {strangers.map((id) => (
              <button
                key={id}
                type="button"
                onClick={() => toggle(id)}
                className="flex items-center gap-1 text-[10px] font-mono text-amber-700 dark:text-amber-300 bg-amber-100 dark:bg-amber-900/50 border border-amber-300 dark:border-amber-700 rounded px-1.5 py-0.5 hover:bg-amber-200 dark:hover:bg-amber-800"
                title="Tool id not present in the current project inventory. Click to remove."
              >
                <Wrench size={9} />
                {id}
                <X size={9} />
              </button>
            ))}
          </div>
          <p className="text-[10px] text-amber-700/80 dark:text-amber-300/80 mt-1">
            Kept verbatim in the allowlist — they emit{" "}
            <span className="font-mono">ToolDeclaredButMissing</span> at DECIDE time
            until a plugin registers them.
          </p>
        </div>
      )}
    </div>
  );
}

export default AgentRoleToolPicker;
