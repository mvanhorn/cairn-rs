/**
 * ApprovalsPage — unified operator inbox (F45).
 *
 * Sources a single `/v1/approvals` query that merges plan approvals
 * (`ApprovalRecord`) and tool-call approvals (`ToolCallApprovalRecord`)
 * via a `kind` discriminator. Clicking a row opens a kind-specific drawer:
 *
 *   - ApprovalDrawer       — plan approve/reject with confirmation.
 *   - ToolCallDrawer       — view args, amend inline, pick scope (Once |
 *                            Session + optional match policy), reject
 *                            with reason, approve.
 */

import { useMemo, useState } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { clsx } from "clsx";
import {
  Check,
  Inbox,
  Loader2,
  Pencil,
  RefreshCw,
  Search,
  Wrench,
  X,
} from "lucide-react";
import { ErrorFallback } from "../components/ErrorFallback";
import { StatCard } from "../components/StatCard";
import { CopyButton } from "../components/CopyButton";
import { Drawer } from "../components/Drawer";
import { useToast } from "../components/Toast";
import { defaultApi } from "../lib/api";
import type {
  ApprovalDecision,
  ApprovalMatchPolicy,
  ApprovalRecord,
  ToolCallApprovalRecord,
  UnifiedApproval,
} from "../lib/types";
import { useAutoRefresh, REFRESH_OPTIONS } from "../hooks/useAutoRefresh";
import { EmptyScopeHint } from "../components/EmptyScopeHint";
import { EntityExplainer } from "../components/EntityExplainer";
import { ENTITY_EXPLAINERS } from "../lib/entityExplainers";

// ── Helpers ────────────────────────────────────────────────────────────────────

const shortId = (id: string) =>
  id.length > 22 ? `${id.slice(0, 10)}…${id.slice(-6)}` : id;

const fmtTime = (ms: number) =>
  new Date(ms).toLocaleString(undefined, {
    month: "short", day: "numeric",
    hour: "2-digit", minute: "2-digit",
  });

const fmtRelative = (ms: number): string => {
  const d = Date.now() - ms;
  if (d < 60_000)      return "just now";
  if (d < 3_600_000)   return `${Math.floor(d / 60_000)}m ago`;
  if (d < 86_400_000)  return `${Math.floor(d / 3_600_000)}h ago`;
  if (d < 604_800_000) return `${Math.floor(d / 86_400_000)}d ago`;
  return new Date(ms).toLocaleDateString(undefined, { month: "short", day: "numeric" });
};

// ── Unified row model ──────────────────────────────────────────────────────────
//
// The two projections have different shapes on the wire. Rather than
// sprinkle narrowing throughout the render tree we lift them into a
// single `Row` and keep a tagged back-reference to the original record
// so the drawer can render a kind-specific panel.

type Row =
  | {
      kind: "legacy";
      id: string;
      tool: string;            // label ("plan" | "pause")
      runId: string | null;
      createdAt: number;
      resolved: boolean;
      decision: ApprovalDecision | null;
      source: ApprovalRecord;
    }
  | {
      kind: "tool";
      id: string;
      tool: string;            // actual tool name
      runId: string | null;
      createdAt: number;
      resolved: boolean;
      decision: "approved" | "rejected" | null;
      source: ToolCallApprovalRecord;
    };

const legacyLabel = (a: ApprovalRecord): string =>
  a.task_id ? "plan" : "pause";

const toolCallDecision = (r: ToolCallApprovalRecord): "approved" | "rejected" | null => {
  if (r.state === "approved") return "approved";
  if (r.state === "rejected" || r.state === "timeout") return "rejected";
  return null;
};

const toRow = (r: UnifiedApproval): Row => {
  if (r.kind === "tool_call") {
    const tc = r as ToolCallApprovalRecord & { kind: "tool_call" };
    return {
      kind: "tool",
      id: tc.call_id,
      tool: tc.tool_name,
      runId: tc.run_id,
      createdAt: tc.proposed_at_ms,
      resolved: tc.state !== "pending",
      decision: toolCallDecision(tc),
      source: tc,
    };
  }
  const pl = r as ApprovalRecord & { kind: "plan" };
  return {
    kind: "legacy",
    id: pl.approval_id,
    tool: legacyLabel(pl),
    runId: pl.run_id,
    createdAt: pl.created_at,
    resolved: pl.decision !== null,
    decision: pl.decision,
    source: pl,
  };
};

// ── Badges ─────────────────────────────────────────────────────────────────────

function KindBadge({ kind, label }: { kind: Row["kind"]; label: string }) {
  // Colour by kind; text is the specific tool/label.
  if (kind === "tool") {
    return (
      <span
        title={`Tool call: ${label}`}
        className="inline-flex items-center gap-1 text-[10px] font-mono font-semibold text-sky-300 bg-sky-950/60 border border-sky-800/50 rounded px-1.5 py-0.5"
      >
        <Wrench size={9} strokeWidth={2.5} />
        {label}
      </span>
    );
  }
  return (
    <span
      title={`Approval kind: ${label}`}
      className="inline-flex items-center gap-1 text-[10px] font-mono font-semibold text-amber-300 bg-amber-950/60 border border-amber-800/50 rounded px-1.5 py-0.5"
    >
      {label}
    </span>
  );
}

function StatusDot({ resolved, decision }: { resolved: boolean; decision: Row["decision"] }) {
  const cls = !resolved
    ? "bg-amber-400 shadow-[0_0_6px_rgba(251,191,36,0.6)]"
    : decision === "approved"
      ? "bg-emerald-500"
      : "bg-red-500";
  const label = !resolved
    ? "pending"
    : decision === "approved"
      ? "approved"
      : "rejected";
  return (
    <span
      title={label}
      className={clsx(
        "inline-block rounded-full",
        resolved ? "size-2" : "size-2.5",
        cls,
      )}
      aria-label={label}
    />
  );
}

// ── Row ────────────────────────────────────────────────────────────────────────

function RowItem({
  row,
  selected,
  onClick,
}: {
  row: Row;
  selected: boolean;
  onClick: () => void;
}) {
  return (
    <button
      onClick={onClick}
      data-testid={`approval-row-${row.id}`}
      className={clsx(
        "w-full flex items-center gap-3 px-3 h-9 text-left transition-colors border-l-2",
        selected
          ? "bg-indigo-500/10 border-indigo-500"
          : "border-transparent hover:bg-gray-100/60 dark:hover:bg-zinc-800/60",
      )}
    >
      <KindBadge kind={row.kind} label={row.tool} />
      <span className="font-mono text-[11px] text-gray-500 dark:text-zinc-400 truncate" title={row.id}>
        {shortId(row.id)}
      </span>
      <span
        className="ml-auto tabular-nums text-[11px] text-gray-400 dark:text-zinc-500 whitespace-nowrap"
        title={fmtTime(row.createdAt)}
      >
        {fmtRelative(row.createdAt)}
      </span>
      <StatusDot resolved={row.resolved} decision={row.decision} />
    </button>
  );
}

// ── Legacy approval drawer ────────────────────────────────────────────────────

function LegacyDrawerBody({
  approval,
  onClose,
}: {
  approval: ApprovalRecord;
  onClose: () => void;
}) {
  const qc = useQueryClient();
  const toast = useToast();

  const resolve = useMutation({
    mutationFn: (decision: ApprovalDecision) =>
      decision === "approved"
        ? defaultApi.approveApproval(approval.approval_id)
        : defaultApi.rejectApproval(approval.approval_id),
    onSuccess: (_, decision) => {
      toast.success(decision === "approved" ? "Approval granted." : "Approval denied.");
      void qc.invalidateQueries({ queryKey: ["approvals"] });
      void qc.invalidateQueries({ queryKey: ["runs"] });
      if (approval.run_id) {
        void qc.invalidateQueries({ queryKey: ["run-detail", approval.run_id] });
        void qc.invalidateQueries({ queryKey: ["run-events", approval.run_id] });
      }
      onClose();
    },
    onError: (err: unknown) =>
      toast.error(`Failed to resolve — ${err instanceof Error ? err.message : "try again."}`),
  });

  return (
    <div className="p-4 flex flex-col gap-3 text-[12px]">
      <KV label="Approval ID"><Mono>{approval.approval_id}</Mono><CopyButton text={approval.approval_id} size={10} /></KV>
      {approval.run_id && <KV label="Run"><Mono>{approval.run_id}</Mono><CopyButton text={approval.run_id} size={10} /></KV>}
      {approval.task_id && <KV label="Task"><Mono>{approval.task_id}</Mono></KV>}
      <KV label="Requirement">{approval.requirement}</KV>
      <KV label="Requested">{fmtTime(approval.created_at)}</KV>
      {approval.decision && (
        <KV label="Decision">
          <span className={clsx(
            "text-[11px] font-medium rounded px-1.5 py-0.5",
            approval.decision === "approved"
              ? "text-emerald-400 bg-emerald-950/50 border border-emerald-800/40"
              : "text-red-400 bg-red-950/50 border border-red-800/40",
          )}>
            {approval.decision}
          </span>
        </KV>
      )}
      {approval.decision === null && (
        <div className="pt-2 flex gap-2">
          <button
            onClick={() => resolve.mutate("rejected")}
            disabled={resolve.isPending}
            className="flex-1 px-3 h-8 rounded text-[12px] font-medium bg-red-900/40 text-red-300 hover:bg-red-900/70 border border-red-800/50 transition-colors disabled:opacity-40 inline-flex items-center justify-center gap-1.5"
          >
            <X size={13} /> Reject
          </button>
          <button
            onClick={() => resolve.mutate("approved")}
            disabled={resolve.isPending}
            className="flex-1 px-3 h-8 rounded text-[12px] font-medium bg-emerald-900/50 text-emerald-300 hover:bg-emerald-900 border border-emerald-800/50 transition-colors disabled:opacity-40 inline-flex items-center justify-center gap-1.5"
          >
            {resolve.isPending ? <Loader2 size={13} className="animate-spin" /> : <Check size={13} />}
            Approve
          </button>
        </div>
      )}
    </div>
  );
}

// ── Tool-call drawer ──────────────────────────────────────────────────────────

type ScopeType = "once" | "session";

/** Best-effort extraction of a path argument from the tool's args payload.
 *  Mirrors `crates/cairn-runtime/src/tool_call_approvals.rs::extract_path_arg`
 *  (top-level `"path"`), with a conservative widening to `"file_path"` and
 *  `"cwd"` so the UI can pre-seed a sensible default for the operator when
 *  they flip to `ExactPath`/`ProjectScopedPath`. Non-path tools return
 *  `null` — the input stays empty and the operator types the root. */
function extractPathLike(args: unknown): string | null {
  if (!args || typeof args !== "object") return null;
  const obj = args as Record<string, unknown>;
  for (const key of ["path", "file_path", "cwd", "working_dir"]) {
    const v = obj[key];
    if (typeof v === "string" && v.trim()) return v;
  }
  return null;
}

/** Derive a plausible project root from a path-like argument. Strips the
 *  trailing segment so a path like `/workspaces/proj/src/lib.rs` seeds
 *  `/workspaces/proj/src`. Operators typically widen this by hand when
 *  they want to cover the full repo; pre-filling a directory is still
 *  cheaper than staring at an empty box. Absolute root is preserved. */
function deriveProjectRoot(pathLike: string | null): string {
  if (!pathLike) return "";
  const trimmed = pathLike.replace(/\/+$/, "");
  const lastSep = trimmed.lastIndexOf("/");
  if (lastSep <= 0) return trimmed || "/";
  return trimmed.slice(0, lastSep);
}

/** Build the wire-shape match policy from the drawer state. Returns
 *  `undefined` when the operator would submit an empty path/root — the
 *  caller keeps the Approve button disabled in that case so we never
 *  POST a malformed payload. */
function buildMatchPolicy(
  kind: ApprovalMatchPolicy["kind"],
  path: string,
  projectRoot: string,
): ApprovalMatchPolicy | undefined {
  switch (kind) {
    case "exact":
      return { kind: "exact" };
    case "exact_path": {
      const trimmed = path.trim();
      return trimmed ? { kind: "exact_path", path: trimmed } : undefined;
    }
    case "project_scoped_path": {
      const trimmed = projectRoot.trim();
      return trimmed ? { kind: "project_scoped_path", project_root: trimmed } : undefined;
    }
  }
}

function ToolCallDrawerBody({
  record,
  onClose,
}: {
  record: ToolCallApprovalRecord;
  onClose: () => void;
}) {
  const qc = useQueryClient();
  const toast = useToast();

  // The args the operator is approving — seeded from the live record
  // (amended > original). Edits here are staged locally; PATCH is
  // only fired when the user hits "Save amendment".
  const effectiveArgs = useMemo(
    () => JSON.stringify(record.amended_tool_args ?? record.original_tool_args, null, 2),
    [record],
  );
  // Seed path / project_root from the tool args (or the proposal's
  // captured match policy, if the server pre-populated one). The
  // operator can override either input before approving.
  const seededPath = useMemo(() => {
    const liveArgs = record.amended_tool_args ?? record.original_tool_args;
    const fromArgs = extractPathLike(liveArgs);
    if (fromArgs) return fromArgs;
    if (record.match_policy.kind === "exact_path") return record.match_policy.path;
    if (record.match_policy.kind === "project_scoped_path") return record.match_policy.project_root;
    return "";
  }, [record]);
  const seededRoot = useMemo(() => {
    if (record.match_policy.kind === "project_scoped_path") return record.match_policy.project_root;
    const liveArgs = record.amended_tool_args ?? record.original_tool_args;
    return deriveProjectRoot(extractPathLike(liveArgs));
  }, [record]);

  const [editing, setEditing] = useState(false);
  const [draft, setDraft] = useState(effectiveArgs);
  const [scopeType, setScopeType] = useState<ScopeType>("once");
  const [matchKind, setMatchKind] = useState<ApprovalMatchPolicy["kind"]>(
    record.match_policy.kind,
  );
  const [pathInput, setPathInput] = useState(seededPath);
  const [rootInput, setRootInput] = useState(seededRoot);
  const [rejectReason, setRejectReason] = useState("");
  const [rejectOpen, setRejectOpen] = useState(false);

  const onResolved = () => {
    void qc.invalidateQueries({ queryKey: ["approvals"] });
    if (record.run_id) {
      void qc.invalidateQueries({ queryKey: ["run-detail", record.run_id] });
      void qc.invalidateQueries({ queryKey: ["run-events", record.run_id] });
    }
    onClose();
  };

  const amend = useMutation({
    mutationFn: (new_tool_args: unknown) =>
      defaultApi.amendApproval(record.call_id, { new_tool_args }),
    onSuccess: () => {
      toast.success("Arguments amended.");
      setEditing(false);
      void qc.invalidateQueries({ queryKey: ["approvals"] });
    },
    onError: (err: unknown) =>
      toast.error(`Amend failed — ${err instanceof Error ? err.message : "try again."}`),
  });

  // Build the match policy from the current drawer state. `undefined`
  // means "the operator's pick requires an input they haven't filled
  // in yet" — we surface that as a disabled Approve button so we never
  // POST an empty path / project_root.
  const sessionMatchPolicy =
    scopeType === "session"
      ? buildMatchPolicy(matchKind, pathInput, rootInput)
      : undefined;
  const sessionReady = scopeType === "once" || sessionMatchPolicy !== undefined;

  const approve = useMutation({
    mutationFn: () =>
      defaultApi.approveApproval(record.call_id, {
        scope:
          scopeType === "once"
            ? { type: "once" }
            : { type: "session", match_policy: sessionMatchPolicy },
      }),
    onSuccess: () => {
      toast.success("Tool call approved.");
      onResolved();
    },
    onError: (err: unknown) =>
      toast.error(`Approve failed — ${err instanceof Error ? err.message : "try again."}`),
  });

  const reject = useMutation({
    mutationFn: () =>
      defaultApi.rejectApproval(record.call_id, {
        reason: rejectReason.trim() ? rejectReason.trim() : undefined,
      }),
    onSuccess: () => {
      toast.success("Tool call rejected.");
      onResolved();
    },
    onError: (err: unknown) =>
      toast.error(`Reject failed — ${err instanceof Error ? err.message : "try again."}`),
  });

  const handleSaveAmend = () => {
    let parsed: unknown;
    try {
      parsed = JSON.parse(draft);
    } catch (e) {
      toast.error(`Invalid JSON: ${e instanceof Error ? e.message : "parse error"}`);
      return;
    }
    amend.mutate(parsed);
  };

  const pending = record.state === "pending";
  const busy = amend.isPending || approve.isPending || reject.isPending;

  return (
    <div className="p-4 flex flex-col gap-3 text-[12px]">
      <KV label="Tool">
        <span className="font-mono text-sky-300">{record.tool_name}</span>
      </KV>
      <KV label="Call ID"><Mono>{record.call_id}</Mono><CopyButton text={record.call_id} size={10} /></KV>
      <KV label="Run"><Mono>{record.run_id}</Mono><CopyButton text={record.run_id} size={10} /></KV>
      <KV label="Session"><Mono>{record.session_id}</Mono></KV>
      <KV label="Proposed">{fmtTime(record.proposed_at_ms)}</KV>
      {record.display_summary && (
        <div className="text-gray-500 dark:text-zinc-400 italic">
          “{record.display_summary}”
        </div>
      )}

      <div>
        <div className="flex items-center justify-between mb-1">
          <span className="text-[11px] font-medium uppercase tracking-wide text-gray-400 dark:text-zinc-500">
            {record.amended_tool_args ? "Amended arguments" : "Arguments"}
          </span>
          {pending && !editing && (
            <button
              onClick={() => { setDraft(effectiveArgs); setEditing(true); }}
              className="inline-flex items-center gap-1 text-[11px] text-indigo-400 hover:text-indigo-300 transition-colors"
            >
              <Pencil size={10} /> Edit args
            </button>
          )}
        </div>
        {editing ? (
          <div className="flex flex-col gap-2">
            <textarea
              value={draft}
              onChange={e => setDraft(e.target.value)}
              rows={10}
              spellCheck={false}
              className="w-full font-mono text-[11px] text-gray-800 dark:text-zinc-200 bg-gray-50 dark:bg-zinc-950 border border-gray-200 dark:border-zinc-800 rounded p-2 focus:outline-none focus:border-indigo-500"
            />
            <div className="flex gap-2 justify-end">
              <button
                onClick={() => setEditing(false)}
                disabled={busy}
                className="px-2 h-7 rounded text-[11px] text-gray-400 dark:text-zinc-500 hover:text-gray-700 dark:hover:text-zinc-200 transition-colors disabled:opacity-40"
              >
                Cancel
              </button>
              <button
                onClick={handleSaveAmend}
                disabled={busy}
                className="px-3 h-7 rounded text-[11px] font-medium bg-indigo-900/60 text-indigo-200 hover:bg-indigo-900 border border-indigo-800/50 transition-colors disabled:opacity-40 inline-flex items-center gap-1.5"
              >
                {amend.isPending ? <Loader2 size={11} className="animate-spin" /> : <Pencil size={11} />}
                Save amendment
              </button>
            </div>
          </div>
        ) : (
          <pre className="font-mono text-[11px] text-gray-800 dark:text-zinc-200 bg-gray-50 dark:bg-zinc-950 border border-gray-200 dark:border-zinc-800 rounded p-2 overflow-x-auto whitespace-pre-wrap">
{effectiveArgs}
          </pre>
        )}
      </div>

      {pending && !editing && (
        <>
          <fieldset
            data-testid="approval-scope-fieldset"
            className="flex flex-col gap-2 pt-1 border-t border-gray-200 dark:border-zinc-800"
          >
            <legend className="text-[11px] font-medium uppercase tracking-wide text-gray-400 dark:text-zinc-500 pt-2">
              Scope
            </legend>
            <label className="flex items-center gap-2 text-[12px]">
              <input
                type="radio"
                data-testid="approval-scope-once"
                checked={scopeType === "once"}
                onChange={() => setScopeType("once")}
                className="accent-indigo-500"
              />
              <span>Once — this call only</span>
            </label>
            <label className="flex items-center gap-2 text-[12px]">
              <input
                type="radio"
                data-testid="approval-scope-session"
                checked={scopeType === "session"}
                onChange={() => setScopeType("session")}
                className="accent-indigo-500"
              />
              <span>For this session, applied to…</span>
            </label>
            {scopeType === "session" && (
              <MatchPolicyPicker
                kind={matchKind}
                onKindChange={setMatchKind}
                path={pathInput}
                onPathChange={setPathInput}
                projectRoot={rootInput}
                onProjectRootChange={setRootInput}
              />
            )}
            <ScopePreview
              toolName={record.tool_name}
              scopeType={scopeType}
              matchKind={matchKind}
              path={pathInput}
              projectRoot={rootInput}
              ready={sessionReady}
            />
          </fieldset>

          {rejectOpen ? (
            <div className="flex flex-col gap-2 pt-1 border-t border-gray-200 dark:border-zinc-800">
              <label className="text-[11px] font-medium uppercase tracking-wide text-gray-400 dark:text-zinc-500 pt-2">
                Reject reason (optional — surfaced to the agent)
              </label>
              <textarea
                value={rejectReason}
                onChange={e => setRejectReason(e.target.value)}
                rows={2}
                placeholder="e.g. path is outside approved scope"
                className="w-full text-[12px] text-gray-800 dark:text-zinc-200 bg-gray-50 dark:bg-zinc-950 border border-gray-200 dark:border-zinc-800 rounded p-2 focus:outline-none focus:border-red-500"
              />
              <div className="flex gap-2">
                <button
                  onClick={() => { setRejectOpen(false); setRejectReason(""); }}
                  disabled={busy}
                  className="px-3 h-8 rounded text-[12px] text-gray-400 dark:text-zinc-500 hover:text-gray-700 dark:hover:text-zinc-200 transition-colors disabled:opacity-40"
                >
                  Cancel
                </button>
                <button
                  onClick={() => reject.mutate()}
                  disabled={busy}
                  className="flex-1 px-3 h-8 rounded text-[12px] font-medium bg-red-900/50 text-red-300 hover:bg-red-900 border border-red-800/50 transition-colors disabled:opacity-40 inline-flex items-center justify-center gap-1.5"
                >
                  {reject.isPending ? <Loader2 size={13} className="animate-spin" /> : <X size={13} />}
                  Confirm reject
                </button>
              </div>
            </div>
          ) : (
            <div className="flex gap-2 pt-2">
              <button
                onClick={() => setRejectOpen(true)}
                disabled={busy}
                data-testid="approval-reject-btn"
                className="flex-1 px-3 h-8 rounded text-[12px] font-medium bg-red-900/40 text-red-300 hover:bg-red-900/70 border border-red-800/50 transition-colors disabled:opacity-40 inline-flex items-center justify-center gap-1.5"
              >
                <X size={13} /> Reject
              </button>
              <button
                onClick={() => approve.mutate()}
                disabled={busy || !sessionReady}
                data-testid="approval-approve-btn"
                title={
                  !sessionReady
                    ? matchKind === "exact_path"
                      ? "Enter a file path before approving."
                      : "Enter a project root before approving."
                    : undefined
                }
                className="flex-1 px-3 h-8 rounded text-[12px] font-medium bg-emerald-900/50 text-emerald-200 hover:bg-emerald-900 border border-emerald-800/50 transition-colors disabled:opacity-40 inline-flex items-center justify-center gap-1.5"
              >
                {approve.isPending ? <Loader2 size={13} className="animate-spin" /> : <Check size={13} />}
                Approve
              </button>
            </div>
          )}
        </>
      )}

      {!pending && (
        <div className="pt-2 border-t border-gray-200 dark:border-zinc-800 text-[11px] text-gray-500 dark:text-zinc-400">
          Resolved: <span className="font-medium text-gray-700 dark:text-zinc-300">{record.state}</span>
          {record.reason && <> — {record.reason}</>}
          {record.operator_id && <> — by {record.operator_id}</>}
        </div>
      )}
    </div>
  );
}

/**
 * Sub-choice for "For this session, applied to…" — renders the shape-
 * specific input (file path for `exact_path`, project root for
 * `project_scoped_path`) and a dropdown that picks between the three
 * `ApprovalMatchPolicy` variants. Kept as a controlled component so the
 * parent drawer owns all of the submit-time state.
 *
 * Decomposing match-policy state into three flat scalars (kind + path +
 * projectRoot) rather than the tagged-union type keeps the operator's
 * edits sticky across dropdown flips: if they type a path, flip to
 * `project_scoped_path`, tweak the root, and flip back, the path they
 * originally typed is still there. The narrower `current: ApprovalMatchPolicy`
 * union the drawer used before dropped typed-in fields on every flip.
 */
function MatchPolicyPicker({
  kind,
  onKindChange,
  path,
  onPathChange,
  projectRoot,
  onProjectRootChange,
}: {
  kind: ApprovalMatchPolicy["kind"];
  onKindChange: (k: ApprovalMatchPolicy["kind"]) => void;
  path: string;
  onPathChange: (v: string) => void;
  projectRoot: string;
  onProjectRootChange: (v: string) => void;
}) {
  return (
    <div className="ml-5 flex flex-col gap-1.5 text-[11px] text-gray-500 dark:text-zinc-400">
      <label className="flex items-center gap-2">
        <span className="w-20">Apply to</span>
        <select
          data-testid="approval-match-policy-select"
          value={kind}
          onChange={e => onKindChange(e.target.value as ApprovalMatchPolicy["kind"])}
          className="flex-1 bg-gray-50 dark:bg-zinc-900 border border-gray-200 dark:border-zinc-800 rounded px-2 h-7 text-[11px] text-gray-700 dark:text-zinc-300 focus:outline-none focus:border-indigo-500"
        >
          <option value="exact">Exactly this call</option>
          <option value="exact_path">Same file path</option>
          <option value="project_scoped_path">Anywhere inside project root</option>
        </select>
      </label>
      {kind === "exact_path" && (
        <label className="flex items-center gap-2">
          <span className="w-20">File path</span>
          <input
            data-testid="approval-match-policy-path"
            value={path}
            onChange={e => onPathChange(e.target.value)}
            placeholder="/abs/path/to/file"
            spellCheck={false}
            className="flex-1 font-mono bg-gray-50 dark:bg-zinc-900 border border-gray-200 dark:border-zinc-800 rounded px-2 h-7 text-[11px] text-gray-700 dark:text-zinc-300 focus:outline-none focus:border-indigo-500"
          />
        </label>
      )}
      {kind === "project_scoped_path" && (
        <label className="flex items-center gap-2">
          <span className="w-20">Project root</span>
          <input
            data-testid="approval-match-policy-project-root"
            value={projectRoot}
            onChange={e => onProjectRootChange(e.target.value)}
            placeholder="/workspaces/proj"
            spellCheck={false}
            className="flex-1 font-mono bg-gray-50 dark:bg-zinc-900 border border-gray-200 dark:border-zinc-800 rounded px-2 h-7 text-[11px] text-gray-700 dark:text-zinc-300 focus:outline-none focus:border-indigo-500"
          />
        </label>
      )}
    </div>
  );
}

/**
 * Render a single sentence under the radios describing the blast radius
 * the operator is about to authorise. This is the operator-facing
 * shorthand for "what does Approve-with-Session actually do?" — the
 * domain semantics live in `crates/cairn-domain/src/approvals.rs`.
 *
 * Shown only when `session` is selected. For `once` the blast radius is
 * trivially "this call only" and the radio label already says that.
 */
function ScopePreview({
  toolName,
  scopeType,
  matchKind,
  path,
  projectRoot,
  ready,
}: {
  toolName: string;
  scopeType: ScopeType;
  matchKind: ApprovalMatchPolicy["kind"];
  path: string;
  projectRoot: string;
  ready: boolean;
}) {
  if (scopeType !== "session") return null;

  let body: React.ReactNode;
  if (!ready) {
    body = (
      <span className="text-amber-500 dark:text-amber-400">
        Fill in the {matchKind === "exact_path" ? "file path" : "project root"} to
        preview the blast radius.
      </span>
    );
  } else if (matchKind === "exact") {
    body = (
      <>
        Approves this call and any future <code className="font-mono">{toolName}</code> call with
        byte-identical arguments in the same session.
      </>
    );
  } else if (matchKind === "exact_path") {
    body = (
      <>
        Approves any future <code className="font-mono">{toolName}</code> call whose path
        equals <code className="font-mono">{path.trim()}</code> in the same session.
      </>
    );
  } else {
    const root = projectRoot.trim();
    body = (
      <>
        Approves any future <code className="font-mono">{toolName}</code> call whose path is
        inside <code className="font-mono">{root}</code> (i.e. <code className="font-mono">{root.replace(/\/+$/, "")}/**</code>) in the same session.
      </>
    );
  }

  return (
    <div
      data-testid="approval-scope-preview"
      className="ml-5 mt-1 text-[11px] leading-relaxed text-gray-500 dark:text-zinc-400"
    >
      {body}
    </div>
  );
}

// ── KV helpers ────────────────────────────────────────────────────────────────

function KV({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div className="flex items-center gap-2 min-h-[20px]">
      <span className="w-24 shrink-0 text-[11px] uppercase tracking-wide text-gray-400 dark:text-zinc-500">
        {label}
      </span>
      <div className="flex items-center gap-1 min-w-0 flex-1">{children}</div>
    </div>
  );
}

const Mono = ({ children }: { children: React.ReactNode }) => (
  <span className="font-mono text-[11px] text-gray-700 dark:text-zinc-300 truncate">{children}</span>
);

// ── Filter tabs ────────────────────────────────────────────────────────────────

type KindFilter = "all" | "tool" | "legacy";
type StateFilter = "all" | "pending" | "resolved";

// ── Page ──────────────────────────────────────────────────────────────────────

export function ApprovalsPage() {
  const { ms: refreshMs, setOption, interval } = useAutoRefresh("approvals", "15s");

  const [kindFilter, setKindFilter] = useState<KindFilter>("all");
  const [stateFilter, setStateFilter] = useState<StateFilter>("all");
  const [search, setSearch] = useState("");
  // Compound selection key: `{kind}:{id}`. Plain `id` collides across
  // kinds — a plan approval and a tool-call approval can legitimately
  // share an id (the amend flow historically uses the tool-call id as
  // the approval id), and a bare-string lookup would open the wrong
  // drawer and fire mutations at the wrong record.
  const [selectedKey, setSelectedKey] = useState<string | null>(null);
  const rowKey = (r: Row) => `${r.kind}:${r.id}`;

  // F45 — single unified source of truth. The server merges plan +
  // tool-call approvals and returns them newest-first with a `kind`
  // discriminator on every row.
  const approvalsQ = useQuery({
    queryKey: ["approvals"],
    queryFn: () => defaultApi.listApprovals(),
    refetchInterval: refreshMs,
  });

  const rows: Row[] = useMemo(() => {
    const merged: Row[] = (approvalsQ.data ?? []).map(toRow);
    merged.sort((a, b) => b.createdAt - a.createdAt);
    return merged;
  }, [approvalsQ.data]);

  const filtered = useMemo(() => {
    const needle = search.trim().toLowerCase();
    return rows.filter(r => {
      if (kindFilter === "tool" && r.kind !== "tool") return false;
      if (kindFilter === "legacy" && r.kind !== "legacy") return false;
      if (stateFilter === "pending" && r.resolved) return false;
      if (stateFilter === "resolved" && !r.resolved) return false;
      if (needle) {
        return (
          r.id.toLowerCase().includes(needle) ||
          r.tool.toLowerCase().includes(needle) ||
          (r.runId ?? "").toLowerCase().includes(needle)
        );
      }
      return true;
    });
  }, [rows, kindFilter, stateFilter, search]);

  const selected = useMemo(
    () =>
      filtered.find(r => rowKey(r) === selectedKey) ??
      rows.find(r => rowKey(r) === selectedKey),
    [filtered, rows, selectedKey],
  );

  const pending24 = rows.filter(r => !r.resolved).length;
  const approved24 = useMemo(() => {
    const since = Date.now() - 86_400_000;
    return rows.filter(r => r.resolved && r.decision === "approved" && r.createdAt >= since).length;
  }, [rows]);
  const rejected24 = useMemo(() => {
    const since = Date.now() - 86_400_000;
    return rows.filter(r => r.resolved && r.decision === "rejected" && r.createdAt >= since).length;
  }, [rows]);

  const isLoading = approvalsQ.isLoading;
  const isFetching = approvalsQ.isFetching;
  const error = approvalsQ.error;

  if (approvalsQ.isError) {
    return (
      <ErrorFallback
        error={error}
        resource="approvals"
        onRetry={() => {
          void approvalsQ.refetch();
        }}
      />
    );
  }

  return (
    <div className="flex flex-col h-full bg-gray-50 dark:bg-zinc-900">
      {/* Stat strip */}
      {!isLoading && (
        <div className="grid grid-cols-3 gap-x-6 gap-y-3 px-5 py-3 border-b border-gray-200 dark:border-zinc-800 bg-gray-50 dark:bg-zinc-900 shrink-0">
          <StatCard compact
            label="Pending"
            value={pending24}
            description={pending24 > 0 ? "requires action" : "inbox clear"}
            variant={pending24 > 0 ? "warning" : "success"}
          />
          <StatCard compact label="Approved (24h)" value={approved24} variant="success" />
          <StatCard compact label="Rejected (24h)" value={rejected24} variant="danger" />
        </div>
      )}

      {/* F32 — inline entity explainer. */}
      <div className="px-4 py-1.5 border-b border-gray-200 dark:border-zinc-800 shrink-0 bg-gray-50 dark:bg-zinc-900">
        <EntityExplainer>{ENTITY_EXPLAINERS.approval}</EntityExplainer>
      </div>

      {/* Toolbar */}
      <div className="flex items-center gap-3 px-4 h-10 border-b border-gray-200 dark:border-zinc-800 shrink-0 bg-gray-50 dark:bg-zinc-900">
        <div className="flex items-center gap-0">
          {(["all", "tool", "legacy"] as KindFilter[]).map(k => (
            <button
              key={k}
              onClick={() => setKindFilter(k)}
              className={clsx(
                "px-2 h-10 text-[11px] font-medium transition-colors border-b-2",
                kindFilter === k
                  ? "text-gray-900 dark:text-zinc-100 border-indigo-500"
                  : "text-gray-400 dark:text-zinc-500 border-transparent hover:text-gray-700 dark:hover:text-zinc-300",
              )}
            >
              {k === "all" ? "All" : k === "tool" ? "Tool" : "Plan / Pause"}
            </button>
          ))}
        </div>

        <div className="flex items-center gap-0 ml-2">
          {(["all", "pending", "resolved"] as StateFilter[]).map(s => (
            <button
              key={s}
              onClick={() => setStateFilter(s)}
              className={clsx(
                "px-2 h-10 text-[11px] font-medium transition-colors border-b-2",
                stateFilter === s
                  ? "text-gray-900 dark:text-zinc-100 border-indigo-500"
                  : "text-gray-400 dark:text-zinc-500 border-transparent hover:text-gray-700 dark:hover:text-zinc-300",
              )}
            >
              {s[0].toUpperCase() + s.slice(1)}
            </button>
          ))}
        </div>

        <div className="relative ml-auto">
          <Search size={11} className="absolute left-2 top-1/2 -translate-y-1/2 text-gray-400 dark:text-zinc-600 pointer-events-none" />
          <input
            value={search}
            onChange={e => setSearch(e.target.value)}
            placeholder="Search tool, id, run…"
            className="h-7 pl-6 pr-2 rounded border border-gray-200 dark:border-zinc-700 bg-gray-50 dark:bg-zinc-900 text-[11px] text-gray-700 dark:text-zinc-300 focus:outline-none focus:border-indigo-500 transition-colors w-56"
          />
        </div>

        <div className="flex items-center gap-1">
          <div className="relative">
            <select
              value={interval.option}
              onChange={e => setOption(e.target.value as import("../hooks/useAutoRefresh").RefreshOption)}
              className="appearance-none rounded border border-gray-200 dark:border-zinc-700 bg-gray-50 dark:bg-zinc-900 text-[11px] font-mono pl-5 pr-2 h-7 text-gray-500 dark:text-zinc-400 focus:outline-none focus:border-indigo-500 transition-colors"
              title="Auto-refresh interval"
            >
              {REFRESH_OPTIONS.map(o => <option key={o.option} value={o.option}>{o.label}</option>)}
            </select>
            <span className="absolute left-1.5 top-1/2 -translate-y-1/2 pointer-events-none">
              <RefreshCw size={9} className={isFetching ? "animate-spin text-indigo-400" : "text-gray-400 dark:text-zinc-600"} />
            </span>
          </div>
          <button
            onClick={() => { void approvalsQ.refetch(); }}
            disabled={isFetching}
            className="flex items-center gap-1 h-7 px-2 rounded border border-gray-200 dark:border-zinc-700 bg-gray-50 dark:bg-zinc-900 text-[11px] text-gray-400 dark:text-zinc-500 hover:text-gray-800 dark:hover:text-zinc-200 hover:border-zinc-600 disabled:opacity-40 transition-colors"
            title="Refresh now"
          >
            <RefreshCw size={11} className={isFetching ? "animate-spin" : ""} />
            <span className="hidden sm:inline">Refresh</span>
          </button>
        </div>
      </div>

      {/* List */}
      <div className="flex-1 overflow-y-auto">
        {isLoading ? (
          <div className="divide-y divide-gray-200 dark:divide-zinc-800/40">
            {Array.from({ length: 6 }).map((_, i) => (
              <div key={i} className="flex items-center gap-4 px-4 h-9 animate-pulse">
                <div className="h-3 w-10 rounded bg-gray-100 dark:bg-zinc-800" />
                <div className="h-2.5 w-28 rounded bg-gray-100 dark:bg-zinc-800" />
                <div className="ml-auto h-2.5 w-12 rounded bg-gray-100 dark:bg-zinc-800" />
                <div className="h-2 w-2 rounded-full bg-gray-100 dark:bg-zinc-800" />
              </div>
            ))}
          </div>
        ) : filtered.length === 0 ? (
          <div className="flex flex-col items-center justify-center py-16 gap-2 text-center px-6">
            <Inbox size={26} className="text-gray-300 dark:text-zinc-600" />
            <p className="text-[13px] text-gray-400 dark:text-zinc-600 font-medium">Inbox clear</p>
            <p className="text-[11px] text-gray-300 dark:text-zinc-600 max-w-xs">
              No approvals match this filter. Approvals appear here when a run hits a
              human-in-the-loop gate or a tool call needs operator sign-off.
            </p>
            <EmptyScopeHint empty className="max-w-lg" />
          </div>
        ) : (
          <div className="divide-y divide-gray-100 dark:divide-zinc-800/40">
            {filtered.map(r => (
              <RowItem
                key={rowKey(r)}
                row={r}
                selected={selectedKey === rowKey(r)}
                onClick={() => setSelectedKey(rowKey(r))}
              />
            ))}
          </div>
        )}
      </div>

      {/* Drawer */}
      <Drawer
        open={selected !== undefined}
        onClose={() => setSelectedKey(null)}
        title={
          selected?.kind === "tool"
            ? `Tool call · ${selected.tool}`
            : selected
              ? `Approval · ${selected.tool}`
              : undefined
        }
        width="w-[420px]"
      >
        {selected?.kind === "legacy" && (
          <LegacyDrawerBody approval={selected.source} onClose={() => setSelectedKey(null)} />
        )}
        {selected?.kind === "tool" && (
          <ToolCallDrawerBody record={selected.source} onClose={() => setSelectedKey(null)} />
        )}
      </Drawer>
    </div>
  );
}

export default ApprovalsPage;
