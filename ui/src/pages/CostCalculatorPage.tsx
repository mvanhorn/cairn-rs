/**
 * CostCalculatorPage — estimate LLM API costs before committing to a model.
 *
 * Data source is the bundled LiteLLM catalog at GET /v1/models/catalog.
 * That's the single source of truth for model pricing in cairn — the same
 * registry the router uses for cost accounting. We layer the provider
 * registry (`GET /v1/providers/connections`) on top purely to mark which
 * catalog entries are actually configured in this deployment (the "configured"
 * pill); the *prices* always come from the catalog.
 *
 * Filters match the API surface: provider dropdown, free-only chip, max-cost
 * slider, tools / reasoning capability chips.
 */

import { useState, useMemo, useId } from "react";
import { useQuery } from "@tanstack/react-query";
import {
  Calculator,
  Check,
  ChevronDown,
  ChevronUp,
  Coins,
  Copy,
  Info,
  RotateCcw,
} from "lucide-react";
import { clsx } from "clsx";
import { defaultApi } from "../lib/api";
import { ErrorFallback } from "../components/ErrorFallback";
import { useClipboard } from "../hooks/useClipboard";
import type { ModelCatalogEntry } from "../lib/types";

// Initial-form defaults — used at first load and on Reset.
const DEFAULT_TOKENS_IN  = 10_000;
const DEFAULT_TOKENS_OUT = 2_000;
/** Preferred default model ID when available in the catalog. Matches the
 *  namespaced form used by the router/registry so cost estimates are non-zero
 *  on first load. If the catalog doesn't contain it, we fall back to the
 *  first model with non-zero pricing (see `pickDefaultModelId`). */
const PREFERRED_DEFAULT_MODEL_ID = "openai/gpt-4o-mini";

// ── Types + helpers ──────────────────────────────────────────────────────────

interface ModelRow {
  id:           string;
  name:         string;
  provider:     string;
  inputPer1M:   number;
  outputPer1M:  number;
  contextK:     number;
  tier:         string;
  supportsTools: boolean;
  reasoning:    boolean;
  free:         boolean;
  /** true when the deployment has a configured provider-connection that
   *  advertises this model ID. Purely informational — the price is the
   *  catalog price either way. */
  configured:   boolean;
}

/** Pick a sensible default model from the catalog rows:
 *    1. The preferred ID (`openai/gpt-4o-mini`) if present.
 *    2. Otherwise the first row with non-zero input or output pricing
 *       (so the initial estimate isn't misleadingly $0.00).
 *    3. Otherwise `rows[0]` — the catalog might be free-only on this deploy.
 *    4. Empty string if the catalog is empty. */
function pickDefaultModelId(rows: ModelRow[]): string {
  if (rows.length === 0) return "";
  const preferred = rows.find(r => r.id === PREFERRED_DEFAULT_MODEL_ID);
  if (preferred) return preferred.id;
  const paid = rows.find(r => r.inputPer1M > 0 || r.outputPer1M > 0);
  if (paid) return paid.id;
  return rows[0]!.id;
}

function catalogToRows(
  entries: ModelCatalogEntry[],
  configuredIds: Set<string>,
): ModelRow[] {
  return entries.map(e => ({
    id:           e.id,
    name:         e.display_name,
    provider:     e.provider,
    inputPer1M:   e.cost_per_1m_input,
    outputPer1M:  e.cost_per_1m_output,
    contextK:     Math.max(1, Math.round(e.context_len / 1000)),
    tier:         e.tier,
    supportsTools: e.supports_tools,
    reasoning:    e.reasoning,
    free:         e.cost_per_1m_input === 0 && e.cost_per_1m_output === 0,
    configured:   configuredIds.has(e.id),
  }));
}

function calcCost(m: ModelRow, tokensIn: number, tokensOut: number): number {
  return (tokensIn  / 1_000_000) * m.inputPer1M
       + (tokensOut / 1_000_000) * m.outputPer1M;
}

function fmtUSD(n: number): string {
  if (n === 0)      return "Free";
  if (n < 0.0001)   return `$${n.toExponential(2)}`;
  if (n < 0.01)     return `$${n.toFixed(6)}`;
  if (n < 1)        return `$${n.toFixed(4)}`;
  if (n < 100)      return `$${n.toFixed(2)}`;
  return `$${n.toLocaleString(undefined, { maximumFractionDigits: 2 })}`;
}

function fmtTokens(n: number): string {
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
  if (n >= 1_000)     return `${(n / 1_000).toFixed(0)}K`;
  return String(n);
}

const TOKEN_PRESETS = [
  { label: "1K",   value: 1_000      },
  { label: "10K",  value: 10_000     },
  { label: "100K", value: 100_000    },
  { label: "1M",   value: 1_000_000  },
  { label: "10M",  value: 10_000_000 },
];

// ── Token input ───────────────────────────────────────────────────────────────

function TokenInput({ label, value, onChange, testId }: {
  label: string;
  value: number;
  onChange: (n: number) => void;
  testId?: string;
}) {
  // Issue #386: associate the <label> with the <input> via htmlFor/id so
  // screen readers announce the label when the input receives focus, and
  // so clicking the label focuses the input. Without this association, AT
  // users navigating by label hit a dead element. useId() yields a stable
  // ID per mount which is safe even when multiple TokenInput instances
  // coexist on the same page.
  const inputId = useId();
  return (
    <div>
      <label htmlFor={inputId} className="text-[11px] text-gray-400 dark:text-zinc-500 uppercase tracking-wider block mb-2">{label}</label>
      <div className="space-y-2">
        <input
          id={inputId}
          type="number"
          min={0}
          value={value}
          onChange={e => onChange(Math.max(0, parseInt(e.target.value, 10) || 0))}
          data-testid={testId}
          className="w-full rounded border border-gray-200 dark:border-zinc-800 bg-gray-50 dark:bg-zinc-900 text-[14px] text-gray-900 dark:text-zinc-100
                     font-mono px-3 py-2 focus:outline-none focus:border-indigo-500 transition-colors
                     [appearance:textfield] [&::-webkit-outer-spin-button]:appearance-none
                     [&::-webkit-inner-spin-button]:appearance-none"
        />
        <div className="flex gap-1.5 flex-wrap">
          {TOKEN_PRESETS.map(p => (
            <button key={p.label} onClick={() => onChange(p.value)}
              className={clsx(
                "rounded px-2 py-0.5 text-[10px] font-mono font-medium transition-colors border",
                value === p.value
                  ? "bg-indigo-600/20 text-indigo-300 border-indigo-700/50"
                  : "text-gray-400 dark:text-zinc-600 border-gray-200 dark:border-zinc-800 hover:text-gray-700 dark:hover:text-zinc-300 hover:border-zinc-600",
              )}>
              {p.label}
            </button>
          ))}
        </div>
      </div>
    </div>
  );
}

// ── Cost bar ──────────────────────────────────────────────────────────────────

function CostBar({ cost, maxCost }: { cost: number; maxCost: number }) {
  const pct = maxCost > 0 ? Math.min(100, (cost / maxCost) * 100) : 0;
  const color =
    pct > 66 ? "bg-red-500/60"   :
    pct > 33 ? "bg-amber-500/60" :
               "bg-emerald-500/60";
  return (
    <div className="h-1.5 rounded-full bg-gray-100 dark:bg-zinc-800 overflow-hidden w-24">
      <div className={clsx("h-full rounded-full transition-all", color)} style={{ width: `${pct}%` }} />
    </div>
  );
}

// ── Filter chip ──────────────────────────────────────────────────────────────

function FilterChip({ label, active, onToggle }: { label: string; active: boolean; onToggle: () => void }) {
  return (
    <button
      type="button"
      onClick={onToggle}
      className={clsx(
        "px-2 h-6 rounded-full text-[11px] font-medium transition-colors border",
        active
          ? "bg-indigo-600/20 text-indigo-300 border-indigo-600/60"
          : "text-gray-500 dark:text-zinc-500 border-gray-200 dark:border-zinc-700 hover:border-gray-400 dark:hover:border-zinc-500",
      )}
    >
      {label}
    </button>
  );
}

// ── Page ──────────────────────────────────────────────────────────────────────

export function CostCalculatorPage() {
  const [tokensIn,       setTokensIn]       = useState(DEFAULT_TOKENS_IN);
  const [tokensOut,      setTokensOut]      = useState(DEFAULT_TOKENS_OUT);
  // Empty string means "use the computed default once the catalog loads."
  // The real default is chosen in `pickDefaultModelId` so it always lands on
  // a model with non-zero pricing when one exists.
  const [selectedId,     setSelectedId]     = useState("");
  const [sortBy,         setSortBy]         = useState<"cost" | "provider" | "name">("cost");
  const [sortAsc,        setSortAsc]        = useState(true);
  const [filterProvider, setFilterProvider] = useState<string>("all");
  const [freeOnly,       setFreeOnly]       = useState(false);
  const [toolsOnly,      setToolsOnly]      = useState(false);
  const [reasoningOnly,  setReasoningOnly]  = useState(false);
  const [configuredOnly, setConfiguredOnly] = useState(false);
  // Issue #386: stable id so the Model <label> can `htmlFor` the <select>.
  const modelSelectId = useId();
  // `copied` flips true only on a SUCCESSFUL copy and auto-resets after
  // 1.5 s — we mirror that in the button label below so a failed copy
  // (denied permission, HTTP context without clipboard API, etc.) doesn't
  // silently show "Copied". The failure toast is surfaced by the hook.
  const { copy: copyToClipboard, copied: resultCopied } = useClipboard({
    successMessage: "Copied to clipboard",
  });

  // ── Fetch provider registry (for the "configured" badge only) ───────────
  const { data: registry } = useQuery({
    queryKey: ["provider-registry"],
    queryFn:  () => defaultApi.getProviderRegistry(),
    staleTime: 120_000,
    retry: 1,
  });
  const configuredIds = useMemo(() => {
    const ids = new Set<string>();
    for (const entry of registry ?? []) {
      if (!entry.available) continue;
      for (const m of entry.models) ids.add(m.id);
    }
    return ids;
  }, [registry]);

  // ── Fetch bundled catalog ───────────────────────────────────────────────
  // limit=1000 is the server cap, sufficient for the full catalog (~500
  // chat-mode entries). If a deployment ever ships a catalog larger than
  // that, bump the cap on both ends together.
  const { data: catalog, isLoading: catalogLoading, isError: catalogError,
          error: catalogErr, refetch: catalogRefetch } = useQuery({
    queryKey: ["model-catalog-all"],
    queryFn:  () => defaultApi.listModelCatalog({ limit: 1000 }),
    staleTime: 300_000,
    retry: 1,
  });

  const MODELS: ModelRow[] = useMemo(() => {
    if (!catalog?.items) return [];
    return catalogToRows(catalog.items, configuredIds);
  }, [catalog, configuredIds]);

  const PROVIDERS = useMemo(
    () => [...new Set(MODELS.map(m => m.provider))].sort(),
    [MODELS],
  );

  // Computed default that prefers a paid, namespaced model so the initial
  // estimate isn't $0.00 on first load. Recomputes when the catalog changes.
  const defaultModelId = useMemo(() => pickDefaultModelId(MODELS), [MODELS]);

  // Effective selection: explicit user choice wins; otherwise fall back to
  // the computed default (which itself falls back to `MODELS[0]` only when
  // no paid model exists on this deployment).
  const effectiveId = selectedId || defaultModelId;
  const selected = MODELS.find(m => m.id === effectiveId)
                ?? MODELS.find(m => m.id === defaultModelId)
                ?? MODELS[0];
  const selectedCost = selected ? calcCost(selected, tokensIn, tokensOut) : 0;
  const inputCost  = selected ? (tokensIn  / 1_000_000) * selected.inputPer1M  : 0;
  const outputCost = selected ? (tokensOut / 1_000_000) * selected.outputPer1M : 0;

  const tableRows = useMemo(() => {
    let rows = MODELS.map(m => ({ ...m, cost: calcCost(m, tokensIn, tokensOut) }));
    if (filterProvider !== "all") rows = rows.filter(r => r.provider === filterProvider);
    if (freeOnly)       rows = rows.filter(r => r.free);
    if (toolsOnly)      rows = rows.filter(r => r.supportsTools);
    if (reasoningOnly)  rows = rows.filter(r => r.reasoning);
    if (configuredOnly) rows = rows.filter(r => r.configured);
    rows.sort((a, b) => {
      let v = 0;
      if (sortBy === "cost")     v = a.cost - b.cost;
      if (sortBy === "provider") v = a.provider.localeCompare(b.provider);
      if (sortBy === "name")     v = a.name.localeCompare(b.name);
      return sortAsc ? v : -v;
    });
    return rows;
  }, [MODELS, tokensIn, tokensOut, sortBy, sortAsc, filterProvider,
      freeOnly, toolsOnly, reasoningOnly, configuredOnly]);

  const maxCost = Math.max(...tableRows.map(r => r.cost), 0.0001);

  function toggleSort(col: typeof sortBy) {
    if (sortBy === col) setSortAsc(v => !v);
    else { setSortBy(col); setSortAsc(true); }
  }

  /** Clear token inputs and model selection back to the computed defaults.
   *  The model reverts to `pickDefaultModelId(MODELS)` by clearing the
   *  explicit selection — we don't touch filters/sort because those aren't
   *  part of the form the task brief calls out. */
  function handleReset() {
    setTokensIn(DEFAULT_TOKENS_IN);
    setTokensOut(DEFAULT_TOKENS_OUT);
    setSelectedId("");
  }

  /** Copy a human-readable summary of the estimate to the clipboard. Matches
   *  the in-page result layout: provider/name, token counts, per-line and
   *  total costs. `useClipboard` surfaces the success/failure toast and flips
   *  `resultCopied` only on success. */
  async function handleCopyResult() {
    if (!selected) return;
    const summary =
      `${selected.provider} · ${selected.name} (${selected.id})\n` +
      `Input:  ${fmtTokens(tokensIn)} tokens → ${fmtUSD(inputCost)}\n` +
      `Output: ${fmtTokens(tokensOut)} tokens → ${fmtUSD(outputCost)}\n` +
      `Total:  ${fmtUSD(selectedCost)}`;
    await copyToClipboard(summary);
  }

  const SortIcon = ({ col }: { col: typeof sortBy }) =>
    sortBy !== col ? null :
    sortAsc ? <ChevronUp size={10} className="inline ml-0.5" /> :
              <ChevronDown size={10} className="inline ml-0.5" />;

  if (catalogError) {
    return <ErrorFallback error={catalogErr} resource="model catalog" onRetry={() => void catalogRefetch()} />;
  }

  return (
    <div className="flex flex-col h-full bg-white dark:bg-zinc-950 overflow-y-auto">
      <div className="max-w-5xl mx-auto px-5 py-5 space-y-6 w-full">

        {/* Header */}
        <div className="flex items-center gap-3">
          <div className="w-8 h-8 rounded-lg bg-indigo-500/10 flex items-center justify-center shrink-0">
            <Calculator size={16} className="text-indigo-400" />
          </div>
          <div>
            <h1 className="text-[15px] font-semibold text-gray-900 dark:text-zinc-100">Cost Calculator</h1>
            <p className="text-[11px] text-gray-400 dark:text-zinc-600 mt-0.5">
              Estimate LLM API spend from the bundled LiteLLM catalog. Pricing per 1M tokens.
              {catalogLoading && <span className="ml-1 text-indigo-400">Loading catalog…</span>}
              {catalog && !catalogLoading && (
                <span className="ml-1">
                  {MODELS.length} model{MODELS.length === 1 ? "" : "s"} · {configuredIds.size} configured
                </span>
              )}
            </p>
          </div>
        </div>

        {/* Calculator card */}
        <div className="rounded-xl border border-gray-200 dark:border-zinc-800 bg-gray-50 dark:bg-zinc-900 p-5 space-y-5">
          <div className="grid grid-cols-1 sm:grid-cols-3 gap-5">
            {/* Token inputs */}
            <TokenInput label="Input tokens"  value={tokensIn}  onChange={setTokensIn}  testId="costcalc-tokens-in"  />
            <TokenInput label="Output tokens" value={tokensOut} onChange={setTokensOut} testId="costcalc-tokens-out" />

            {/* Model selector */}
            <div>
              <label htmlFor={modelSelectId} className="text-[11px] text-gray-400 dark:text-zinc-500 uppercase tracking-wider block mb-2">Model</label>
              <select id={modelSelectId} value={effectiveId} onChange={e => setSelectedId(e.target.value)}
                data-testid="costcalc-model-select"
                className="w-full rounded border border-gray-200 dark:border-zinc-800 bg-gray-50 dark:bg-zinc-900 text-[13px] text-gray-800 dark:text-zinc-200
                           px-3 py-2 focus:outline-none focus:border-indigo-500 transition-colors">
                {PROVIDERS.map(prov => (
                  <optgroup key={prov} label={prov}>
                    {MODELS.filter(m => m.provider === prov).map(m => (
                      <option key={m.id} value={m.id}>
                        {m.name}{m.configured ? " ✓" : ""}
                      </option>
                    ))}
                  </optgroup>
                ))}
              </select>
            </div>
          </div>

          {/* Result */}
          {selected && (
            <div className="rounded-lg border border-gray-200 dark:border-zinc-800 bg-white dark:bg-zinc-950 divide-y divide-gray-200 dark:divide-zinc-800">
              <div className="flex items-center justify-between px-4 py-3 gap-3">
                <div className="flex items-center gap-3 min-w-0">
                  <span className="text-[11px] font-medium text-gray-500 dark:text-zinc-400">
                    {selected.provider}
                  </span>
                  <span className="text-[14px] font-medium text-gray-800 dark:text-zinc-200 truncate">{selected.name}</span>
                  {selected.reasoning && (
                    <span className="text-[10px] bg-violet-600/20 text-violet-300 rounded px-1.5 py-0.5">reasoning</span>
                  )}
                  {selected.free && (
                    <span className="text-[10px] bg-emerald-600/20 text-emerald-400 rounded px-1.5 py-0.5">free</span>
                  )}
                  {!selected.configured && (
                    <span className="text-[10px] text-gray-400 dark:text-zinc-600 bg-gray-100 dark:bg-zinc-800 rounded px-1.5 py-0.5">not configured</span>
                  )}
                </div>
                <div className="flex items-center gap-2 shrink-0">
                  <div className="text-right">
                    <p
                      data-testid="costcalc-total-cost"
                      className="text-[22px] font-semibold text-gray-900 dark:text-zinc-100 tabular-nums leading-none"
                    >
                      {fmtUSD(selectedCost)}
                    </p>
                    <p className="text-[10px] text-gray-400 dark:text-zinc-600 mt-0.5">estimated total</p>
                  </div>
                  <div className="flex items-center gap-1.5 pl-2 border-l border-gray-200 dark:border-zinc-800">
                    <button
                      type="button"
                      onClick={() => void handleCopyResult()}
                      data-testid="costcalc-copy-btn"
                      title="Copy estimate to clipboard"
                      className="flex items-center gap-1.5 rounded-md bg-gray-50 dark:bg-zinc-900 border border-gray-200 dark:border-zinc-800 px-2.5 py-1.5 text-[11px] text-gray-500 dark:text-zinc-400 hover:text-gray-700 dark:hover:text-zinc-200 hover:border-gray-400 dark:hover:border-zinc-600 transition-colors"
                    >
                      {resultCopied
                        ? <><Check size={11} className="text-emerald-400" /> Copied</>
                        : <><Copy  size={11} /> Copy</>}
                    </button>
                    <button
                      type="button"
                      onClick={handleReset}
                      data-testid="costcalc-reset-btn"
                      title="Reset to defaults"
                      className="flex items-center gap-1.5 rounded-md bg-gray-50 dark:bg-zinc-900 border border-gray-200 dark:border-zinc-800 px-2.5 py-1.5 text-[11px] text-gray-500 dark:text-zinc-400 hover:text-gray-700 dark:hover:text-zinc-200 hover:border-gray-400 dark:hover:border-zinc-600 transition-colors"
                    >
                      <RotateCcw size={11} /> Reset
                    </button>
                  </div>
                </div>
              </div>

              <div className="grid grid-cols-2 divide-x divide-gray-200 dark:divide-zinc-800">
                <div className="px-4 py-2.5">
                  <p className="text-[10px] text-gray-400 dark:text-zinc-600 mb-1">Input: {fmtTokens(tokensIn)} tokens</p>
                  <p className="text-[13px] font-mono text-gray-700 dark:text-zinc-300 tabular-nums">{fmtUSD(inputCost)}</p>
                  <p className="text-[10px] text-gray-300 dark:text-zinc-600 mt-0.5">
                    {selected.free ? "free" : `$${selected.inputPer1M.toFixed(4)} / 1M`}
                  </p>
                </div>
                <div className="px-4 py-2.5">
                  <p className="text-[10px] text-gray-400 dark:text-zinc-600 mb-1">Output: {fmtTokens(tokensOut)} tokens</p>
                  <p className="text-[13px] font-mono text-gray-700 dark:text-zinc-300 tabular-nums">{fmtUSD(outputCost)}</p>
                  <p className="text-[10px] text-gray-300 dark:text-zinc-600 mt-0.5">
                    {selected.free ? "free" : `$${selected.outputPer1M.toFixed(4)} / 1M`}
                  </p>
                </div>
              </div>

              {selected.contextK > 0 && (
                <div className="px-4 py-2 flex items-center gap-2">
                  <Info size={11} className="text-gray-400 dark:text-zinc-600 shrink-0" />
                  <p className="text-[11px] text-gray-400 dark:text-zinc-600">
                    Context window: <span className="text-gray-400 dark:text-zinc-500 font-mono">{selected.contextK}K tokens</span>
                    {selected.contextK * 1000 < tokensIn + tokensOut && (
                      <span className="ml-2 text-amber-500">— exceeds context window</span>
                    )}
                  </p>
                </div>
              )}
            </div>
          )}
        </div>

        {/* Pricing comparison table */}
        <div>
          <div className="flex items-center justify-between mb-3 flex-wrap gap-2">
            <div className="flex items-center gap-2">
              <Coins size={13} className="text-gray-400 dark:text-zinc-500" />
              <span className="text-[12px] font-medium text-gray-700 dark:text-zinc-300">
                All Models — Cost for {fmtTokens(tokensIn)} in / {fmtTokens(tokensOut)} out
              </span>
            </div>
            <div className="flex items-center gap-2 flex-wrap">
              <FilterChip label="Free"       active={freeOnly}       onToggle={() => setFreeOnly(v => !v)} />
              <FilterChip label="Tools"      active={toolsOnly}      onToggle={() => setToolsOnly(v => !v)} />
              <FilterChip label="Reasoning"  active={reasoningOnly}  onToggle={() => setReasoningOnly(v => !v)} />
              <FilterChip label="Configured" active={configuredOnly} onToggle={() => setConfiguredOnly(v => !v)} />
              <select value={filterProvider} onChange={e => setFilterProvider(e.target.value)}
                className="rounded border border-gray-200 dark:border-zinc-800 bg-gray-50 dark:bg-zinc-900 text-[11px] text-gray-500 dark:text-zinc-400 px-2 py-1
                           focus:outline-none focus:border-indigo-500 transition-colors">
                <option value="all">All providers</option>
                {PROVIDERS.map(p => <option key={p} value={p}>{p}</option>)}
              </select>
            </div>
          </div>

          <div className="rounded-xl border border-gray-200 dark:border-zinc-800 overflow-hidden">
            <table className="min-w-full text-[12px]">
              <thead>
                <tr className="bg-gray-50 dark:bg-zinc-900 border-b border-gray-200 dark:border-zinc-800">
                  <th onClick={() => toggleSort("name")}
                    className="px-4 py-2.5 text-left text-[10px] font-medium text-gray-400 dark:text-zinc-500 uppercase tracking-wider cursor-pointer hover:text-gray-700 dark:hover:text-zinc-300 transition-colors">
                    Model <SortIcon col="name" />
                  </th>
                  <th onClick={() => toggleSort("provider")}
                    className="px-3 py-2.5 text-left text-[10px] font-medium text-gray-400 dark:text-zinc-500 uppercase tracking-wider cursor-pointer hover:text-gray-700 dark:hover:text-zinc-300 transition-colors hidden sm:table-cell">
                    Provider <SortIcon col="provider" />
                  </th>
                  <th className="px-3 py-2.5 text-right text-[10px] font-medium text-gray-400 dark:text-zinc-500 uppercase tracking-wider hidden md:table-cell">
                    Input / 1M
                  </th>
                  <th className="px-3 py-2.5 text-right text-[10px] font-medium text-gray-400 dark:text-zinc-500 uppercase tracking-wider hidden md:table-cell">
                    Output / 1M
                  </th>
                  <th onClick={() => toggleSort("cost")}
                    className="px-4 py-2.5 text-right text-[10px] font-medium text-gray-400 dark:text-zinc-500 uppercase tracking-wider cursor-pointer hover:text-gray-700 dark:hover:text-zinc-300 transition-colors">
                    Est. Cost <SortIcon col="cost" />
                  </th>
                  <th className="px-4 py-2.5 w-32 hidden sm:table-cell" />
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-200 dark:divide-zinc-800/50">
                {tableRows.map((m, i) => {
                  const isSelected = m.id === effectiveId;
                  return (
                    <tr key={m.id}
                      onClick={() => setSelectedId(m.id)}
                      className={clsx(
                        "cursor-pointer transition-colors",
                        isSelected
                          ? "bg-indigo-950/30 ring-1 ring-inset ring-indigo-800/40"
                          : i % 2 === 0 ? "bg-gray-50 dark:bg-zinc-900 hover:bg-gray-100/60 dark:hover:bg-zinc-800/60" : "bg-gray-50/50 dark:bg-zinc-900/50 hover:bg-gray-100/60 dark:hover:bg-zinc-800/60",
                      )}>
                      <td className="px-4 py-2.5">
                        <div className="flex items-center gap-2">
                          <span className={clsx("font-medium", isSelected ? "text-indigo-300" : "text-gray-800 dark:text-zinc-200")}>
                            {m.name}
                          </span>
                          {m.reasoning && (
                            <span className="text-[9px] text-violet-300 bg-violet-600/20 rounded px-1 py-0.5 hidden sm:inline">reasoning</span>
                          )}
                          {m.configured && (
                            <span className="text-[9px] text-emerald-400 bg-emerald-600/20 rounded px-1 py-0.5 hidden sm:inline">✓</span>
                          )}
                        </div>
                        <div className="text-[10px] font-mono text-gray-400 dark:text-zinc-500 mt-0.5">{m.id}</div>
                      </td>
                      <td className="px-3 py-2.5 hidden sm:table-cell">
                        <span className="text-[11px] font-medium text-gray-500 dark:text-zinc-400">
                          {m.provider}
                        </span>
                      </td>
                      <td className="px-3 py-2.5 text-right font-mono text-gray-400 dark:text-zinc-500 hidden md:table-cell">
                        {m.inputPer1M === 0 ? <span className="text-emerald-600">free</span> : `$${m.inputPer1M.toFixed(4)}`}
                      </td>
                      <td className="px-3 py-2.5 text-right font-mono text-gray-400 dark:text-zinc-500 hidden md:table-cell">
                        {m.outputPer1M === 0 ? <span className="text-emerald-600">free</span> : `$${m.outputPer1M.toFixed(4)}`}
                      </td>
                      <td className="px-4 py-2.5 text-right">
                        <span className={clsx(
                          "font-mono font-semibold tabular-nums",
                          m.cost === 0   ? "text-emerald-400" :
                          m.cost < 0.01  ? "text-emerald-300" :
                          m.cost < 1.00  ? "text-gray-800 dark:text-zinc-200"    :
                          m.cost < 10.00 ? "text-amber-300"   : "text-red-300",
                        )}>
                          {fmtUSD(m.cost)}
                        </span>
                      </td>
                      <td className="px-4 py-2.5 hidden sm:table-cell">
                        <CostBar cost={m.cost} maxCost={maxCost} />
                      </td>
                    </tr>
                  );
                })}
                {tableRows.length === 0 && (
                  <tr>
                    <td colSpan={6} className="px-4 py-6 text-center text-[12px] text-gray-400 dark:text-zinc-500">
                      No models match these filters.
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>

          <p className="mt-2 text-[10px] text-gray-300 dark:text-zinc-600 text-center">
            Prices from the bundled LiteLLM catalog; verify with the provider's official pricing page before committing. ✓ = configured in this deployment.
          </p>
        </div>

      </div>
    </div>
  );
}

export default CostCalculatorPage;
