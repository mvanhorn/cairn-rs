/**
 * RFC 031 PR-D1 — Agent Role editor page (create + edit in one form).
 *
 * Left pane: metadata fields (id, name, tier, description,
 * response_shape, max_context_tokens, tools, forbid_all_tools).
 * Right pane: system_prompt textarea with size counter.
 *
 * §D6: on edit the `id` field is disabled; the server rejects PATCH
 * bodies that carry `id` with 422 `ImmutableField`. §PATCH semantics:
 * the editor threads the `ETag` from the initial GET into `If-Match`
 * on save so cross-tab collisions surface as 412 instead of silently
 * overwriting. §Structural-validation failure surface: the 422 body's
 * `details.failures[]` renders as inline field errors; unknown codes
 * fall through to a form-level banner (forward-compat).
 *
 * Draft-persistence (localStorage), section-indicator rail, copy-to-
 * project, and retract-during-active-run confirmation all land in
 * PR-D2 / PR-D3.
 */

import { useMemo, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { ArrowLeft, Loader2, Save, Wrench, X } from "lucide-react";
import { clsx } from "clsx";

import { defaultApi, ApiError } from "../lib/api";
import { useToast } from "../components/Toast";
import { useScope } from "../hooks/useScope";
import type {
  AgentRole,
  AgentRoleAdvisory,
  AgentRoleTier,
  ResponseShape,
} from "../lib/types";

// ── Size caps (mirror RFC 031 §D4) ─────────────────────────────────────────

const SYSTEM_PROMPT_MAX_BYTES = 65_536;
const NAME_MAX_CHARS = 128;
const DESCRIPTION_MAX_BYTES = 4_096;
const ROLE_ID_MAX_LEN = 64;
const ROLE_ID_RE = /^[a-z0-9][a-z0-9_-]*$/;

// ── 422 failure shape ──────────────────────────────────────────────────────

interface ValidationFailure {
  code: string;
  field: string;
  message: string;
  span?: { start: number; end: number } | null;
  suggested_insert_offset?: number | null;
}

function tryExtractFailures(err: unknown): ValidationFailure[] | null {
  // The ApiError captures `code` + `message` but not the `details.failures`.
  // For PR-D1 we parse the message when possible; PR-D2 will teach the
  // fetch wrapper to surface `details`. For now display a generic 422
  // banner and the error message.
  if (err instanceof ApiError && err.status === 422) {
    return [
      {
        code: err.code || "validation_failed",
        field: "",
        message: err.message,
      },
    ];
  }
  return null;
}

// ── UTF-8 byte-count helper ────────────────────────────────────────────────

const BYTE_ENCODER = new TextEncoder();
function byteLen(s: string): number {
  return BYTE_ENCODER.encode(s).length;
}

// ── Form state ─────────────────────────────────────────────────────────────

interface FormState {
  id: string;
  name: string;
  tier: AgentRoleTier;
  description: string;
  system_prompt: string;
  tools: string; // comma/whitespace-separated for the textarea
  forbid_all_tools: boolean;
  max_context_tokens: string; // empty = use role-tier default
  response_shape: ResponseShape;
}

const EMPTY_FORM: FormState = {
  id: "",
  name: "",
  tier: "standard",
  description: "",
  system_prompt: "",
  tools: "",
  forbid_all_tools: false,
  max_context_tokens: "",
  response_shape: "procedural_artifact",
};

function parseToolsField(raw: string): string[] {
  return raw
    .split(/[\s,]+/)
    .map((t) => t.trim())
    .filter((t) => t.length > 0);
}

function roleToForm(role: AgentRole): FormState {
  return {
    id: role.role_id,
    name: role.display_name,
    tier: role.tier,
    description: role.description,
    system_prompt: role.system_prompt ?? "",
    tools: role.tools.join(", "),
    forbid_all_tools: role.forbid_all_tools,
    max_context_tokens: role.max_context_tokens?.toString() ?? "",
    response_shape: role.response_shape,
  };
}

// ── Page ───────────────────────────────────────────────────────────────────

interface Props {
  mode: "new" | "edit";
  roleId?: string;
}

export function AgentRoleEditorPage({ mode, roleId }: Props) {
  const toast = useToast();
  const qc = useQueryClient();
  const [scope] = useScope();

  // On edit, fetch the existing role + ETag.
  const initialQuery = useQuery({
    queryKey: [
      "agent-role",
      scope.tenant_id,
      scope.workspace_id,
      scope.project_id,
      roleId ?? "",
    ],
    queryFn: () => defaultApi.getAgentRole(roleId!),
    enabled: mode === "edit" && !!roleId,
  });

  // RFC 031 PR-D1: seed the form once from the server fetch.
  // React 19 derived-state-from-props idiom: compare the query data
  // against the previously-seeded identity and reset both the form
  // and the ETag when they differ. `useState`-with-updater in render
  // is safe here per React docs ("Storing information from previous
  // renders") and avoids the `react-hooks/set-state-in-effect` rule
  // the bare `useEffect` path triggers.
  const [form, setForm] = useState<FormState>(EMPTY_FORM);
  const [etag, setEtag] = useState<string | null>(null);
  const [seededFrom, setSeededFrom] = useState<unknown>(null);
  const [failures, setFailures] = useState<ValidationFailure[]>([]);
  const [warnings, setWarnings] = useState<AgentRoleAdvisory[]>([]);

  if (
    mode === "edit" &&
    initialQuery.data &&
    seededFrom !== initialQuery.data
  ) {
    setSeededFrom(initialQuery.data);
    setForm(roleToForm(initialQuery.data.item.role));
    setEtag(initialQuery.data.etag);
  }

  // ── Client-side size checks (server is authoritative) ──
  const systemPromptBytes = byteLen(form.system_prompt);
  const descriptionBytes = byteLen(form.description);
  const totalBodyBytes = useMemo(() => {
    // Loose upper-bound estimate of the JSON body. Under-counts quotes /
    // escapes but good enough for the UI counter — server enforces the
    // real 128 KiB limit via DefaultBodyLimit.
    return (
      byteLen(form.id) +
      byteLen(form.name) +
      byteLen(form.description) +
      byteLen(form.system_prompt) +
      byteLen(form.tools) +
      256
    );
  }, [form]);

  const idValid = mode === "edit" || (ROLE_ID_RE.test(form.id) && form.id.length <= ROLE_ID_MAX_LEN);
  const sizesOk =
    systemPromptBytes <= SYSTEM_PROMPT_MAX_BYTES &&
    descriptionBytes <= DESCRIPTION_MAX_BYTES &&
    form.name.length <= NAME_MAX_CHARS;

  const canSubmit = idValid && sizesOk && form.name.trim().length > 0 && form.system_prompt.trim().length > 0;

  // ── Submit ──
  const saveMut = useMutation({
    mutationFn: async () => {
      setFailures([]);
      setWarnings([]);
      const tools = parseToolsField(form.tools);
      const max_context_tokens =
        form.max_context_tokens.trim() === ""
          ? null
          : Number.parseInt(form.max_context_tokens, 10);
      if (mode === "new") {
        return defaultApi.createAgentRole({
          id: form.id,
          name: form.name,
          tier: form.tier,
          description: form.description || undefined,
          system_prompt: form.system_prompt,
          tools,
          forbid_all_tools: form.forbid_all_tools,
          max_context_tokens: max_context_tokens ?? undefined,
          response_shape: form.response_shape,
        });
      }
      return defaultApi.patchAgentRole(
        roleId!,
        {
          name: form.name,
          description: form.description,
          system_prompt: form.system_prompt,
          tools,
          forbid_all_tools: form.forbid_all_tools,
          max_context_tokens,
          response_shape: form.response_shape,
        },
        etag,
      );
    },
    onSuccess: (r) => {
      setWarnings(r.result.warnings ?? []);
      toast.success(
        mode === "new"
          ? `Created ${r.result.role.role_id}.`
          : `Updated ${r.result.role.role_id}.`,
      );
      qc.invalidateQueries({ queryKey: ["agent-roles"] });
      qc.invalidateQueries({ queryKey: ["agent-role"] });
      window.location.hash = `agent/${encodeURIComponent(r.result.role.role_id)}`;
    },
    onError: (err) => {
      const parsed = tryExtractFailures(err);
      if (parsed) {
        setFailures(parsed);
        toast.error("Validation failed. Check the highlighted fields.");
        return;
      }
      if (err instanceof ApiError) {
        if (err.status === 412) {
          toast.error(
            "Role changed in another tab since you opened this editor. Reload to see the latest.",
          );
          return;
        }
        if (err.status === 409) {
          toast.error(`\`${form.id}\` already has an active definition. Use PATCH to update.`);
          return;
        }
        if (err.status === 413) {
          toast.error("Body or field exceeds §D4 size cap.");
          return;
        }
        toast.error(`${err.code}: ${err.message}`);
        return;
      }
      toast.error(err instanceof Error ? err.message : "Save failed");
    },
  });

  if (mode === "edit" && initialQuery.isLoading) {
    return (
      <div className="flex items-center justify-center h-full gap-2 text-gray-400 dark:text-zinc-600">
        <Loader2 size={16} className="animate-spin" />
        <span className="text-[13px]">Loading role…</span>
      </div>
    );
  }

  if (mode === "edit" && initialQuery.isError) {
    return (
      <div className="flex flex-col items-center justify-center h-full gap-3 text-gray-400 dark:text-zinc-600">
        <p className="text-[13px] text-red-400">
          Failed to load role:{" "}
          {initialQuery.error instanceof Error ? initialQuery.error.message : "unknown"}
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

  const failuresByField = new Map<string, ValidationFailure[]>();
  for (const f of failures) {
    const bucket = failuresByField.get(f.field) ?? [];
    bucket.push(f);
    failuresByField.set(f.field, bucket);
  }
  const formLevelFailures = failuresByField.get("") ?? [];

  function fieldError(field: string) {
    const errs = failuresByField.get(field);
    if (!errs || errs.length === 0) return null;
    return (
      <p className="text-[11px] text-red-500 dark:text-red-400 mt-1">
        {errs[0].code}: {errs[0].message}
      </p>
    );
  }

  return (
    <div className="flex flex-col h-full bg-gray-50 dark:bg-zinc-900">
      <div className="flex items-center gap-3 px-5 h-11 border-b border-gray-200 dark:border-zinc-800 shrink-0">
        <button
          onClick={() => (window.location.hash = mode === "edit" ? `agent/${encodeURIComponent(roleId ?? "")}` : "agents")}
          className="flex items-center gap-1 text-[12px] text-gray-400 dark:text-zinc-500 hover:text-gray-700 dark:hover:text-zinc-300"
        >
          <ArrowLeft size={12} />
          {mode === "edit" ? "Role detail" : "Roles"}
        </button>
        <span className="text-[13px] font-medium text-gray-800 dark:text-zinc-200">
          {mode === "new" ? "New agent role" : `Edit ${roleId}`}
        </span>
      </div>

      <div className="flex-1 overflow-y-auto p-5">
        <div className="max-w-6xl grid grid-cols-1 lg:grid-cols-2 gap-5">
          {/* Left pane — metadata */}
          <div className="space-y-4">
            {formLevelFailures.length > 0 && (
              <div className="rounded-lg border border-red-400 dark:border-red-600 bg-red-50 dark:bg-red-950/40 px-4 py-3">
                {formLevelFailures.map((f, i) => (
                  <p key={i} className="text-[12px] text-red-600 dark:text-red-400">
                    {f.code}: {f.message}
                  </p>
                ))}
              </div>
            )}

            <div>
              <label className="block text-[11px] text-gray-400 dark:text-zinc-500 uppercase tracking-wide mb-1">
                Role id{" "}
                <span className="text-gray-400 dark:text-zinc-600 normal-case">
                  {form.id.length}/{ROLE_ID_MAX_LEN}
                </span>
              </label>
              <input
                type="text"
                disabled={mode === "edit"}
                value={form.id}
                onChange={(e) => setForm((f) => ({ ...f, id: e.target.value }))}
                placeholder="pr-reviewer-valkey"
                className={clsx(
                  "w-full rounded-lg border px-3 py-2 text-[13px] font-mono focus:outline-none focus:ring-1 transition-colors",
                  mode === "edit"
                    ? "bg-gray-100 dark:bg-zinc-800 border-gray-200 dark:border-zinc-700 text-gray-500 dark:text-zinc-500 cursor-not-allowed"
                    : idValid
                      ? "bg-gray-50 dark:bg-zinc-900 border-gray-200 dark:border-zinc-700 text-gray-800 dark:text-zinc-200 focus:border-indigo-500 focus:ring-indigo-500/30"
                      : "bg-red-50 dark:bg-red-950/40 border-red-400 dark:border-red-600 text-red-600 dark:text-red-400",
                )}
              />
              {!idValid && form.id.length > 0 && (
                <p className="text-[11px] text-red-500 dark:text-red-400 mt-1">
                  Must match <code>[a-z0-9][a-z0-9_-]*</code>, ≤ 64 chars (§D5).
                </p>
              )}
              {mode === "edit" && (
                <p className="text-[11px] text-gray-400 dark:text-zinc-600 mt-1 italic">
                  Role id is immutable after creation (§D6).
                </p>
              )}
              {fieldError("id")}
            </div>

            <div>
              <label className="block text-[11px] text-gray-400 dark:text-zinc-500 uppercase tracking-wide mb-1">
                Display name{" "}
                <span className="text-gray-400 dark:text-zinc-600 normal-case">
                  {form.name.length}/{NAME_MAX_CHARS}
                </span>
              </label>
              <input
                type="text"
                value={form.name}
                onChange={(e) => setForm((f) => ({ ...f, name: e.target.value }))}
                placeholder="Valkey PR Reviewer"
                className="w-full rounded-lg bg-gray-50 dark:bg-zinc-900 border border-gray-200 dark:border-zinc-700 px-3 py-2 text-[13px] text-gray-800 dark:text-zinc-200 focus:outline-none focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500/30 transition-colors"
              />
              {fieldError("name")}
            </div>

            <div>
              <label className="block text-[11px] text-gray-400 dark:text-zinc-500 uppercase tracking-wide mb-1">
                Tier
              </label>
              <select
                value={form.tier}
                disabled={mode === "edit"}
                onChange={(e) =>
                  setForm((f) => ({ ...f, tier: e.target.value as AgentRoleTier }))
                }
                className={clsx(
                  "w-full rounded-lg border px-3 py-2 text-[13px] font-mono focus:outline-none focus:ring-1",
                  mode === "edit"
                    ? "bg-gray-100 dark:bg-zinc-800 border-gray-200 dark:border-zinc-700 text-gray-500 dark:text-zinc-500 cursor-not-allowed"
                    : "bg-gray-50 dark:bg-zinc-900 border-gray-200 dark:border-zinc-700 text-gray-800 dark:text-zinc-200 focus:border-indigo-500 focus:ring-indigo-500/30",
                )}
              >
                <option value="standard">standard</option>
                <option value="research">research</option>
                <option value="orchestrator">orchestrator (reserved — built-in shadow only)</option>
                <option value="generic">generic (reserved — built-in shadow only)</option>
              </select>
              {mode === "edit" && (
                <p className="text-[11px] text-gray-400 dark:text-zinc-600 mt-1 italic">
                  Tier is immutable on PATCH (§D6).
                </p>
              )}
              {fieldError("tier")}
            </div>

            <div>
              <label className="block text-[11px] text-gray-400 dark:text-zinc-500 uppercase tracking-wide mb-1">
                Description{" "}
                <span className="text-gray-400 dark:text-zinc-600 normal-case">
                  {descriptionBytes}/{DESCRIPTION_MAX_BYTES} bytes
                </span>
              </label>
              <textarea
                value={form.description}
                onChange={(e) => setForm((f) => ({ ...f, description: e.target.value }))}
                rows={2}
                placeholder="One-to-three sentence orchestrator-facing summary."
                className="w-full rounded-lg bg-gray-50 dark:bg-zinc-900 border border-gray-200 dark:border-zinc-700 px-3 py-2 text-[12px] text-gray-800 dark:text-zinc-200 resize-none focus:outline-none focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500/30 transition-colors"
              />
              {fieldError("description")}
            </div>

            <div>
              <label className="block text-[11px] text-gray-400 dark:text-zinc-500 uppercase tracking-wide mb-1">
                Response shape
              </label>
              <div className="flex gap-3 text-[12px] text-gray-800 dark:text-zinc-200">
                <label className="flex items-center gap-1.5">
                  <input
                    type="radio"
                    checked={form.response_shape === "direct_answer"}
                    onChange={() =>
                      setForm((f) => ({ ...f, response_shape: "direct_answer" }))
                    }
                  />
                  <span>direct_answer</span>
                </label>
                <label className="flex items-center gap-1.5">
                  <input
                    type="radio"
                    checked={form.response_shape === "procedural_artifact"}
                    onChange={() =>
                      setForm((f) => ({ ...f, response_shape: "procedural_artifact" }))
                    }
                  />
                  <span>procedural_artifact</span>
                </label>
              </div>
              <p className="text-[11px] text-gray-400 dark:text-zinc-600 mt-1">
                Drives the DECIDE footer shape per §D12.
              </p>
              {fieldError("response_shape")}
            </div>

            <div>
              <label className="block text-[11px] text-gray-400 dark:text-zinc-500 uppercase tracking-wide mb-1">
                Max context tokens
              </label>
              <input
                type="number"
                value={form.max_context_tokens}
                onChange={(e) =>
                  setForm((f) => ({ ...f, max_context_tokens: e.target.value }))
                }
                placeholder="use role-tier default"
                className="w-full rounded-lg bg-gray-50 dark:bg-zinc-900 border border-gray-200 dark:border-zinc-700 px-3 py-2 text-[13px] font-mono text-gray-800 dark:text-zinc-200 focus:outline-none focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500/30 transition-colors"
              />
              {fieldError("max_context_tokens")}
            </div>

            <div>
              <label className="flex items-center gap-2 text-[12px] text-gray-800 dark:text-zinc-200">
                <input
                  type="checkbox"
                  checked={form.forbid_all_tools}
                  onChange={(e) =>
                    setForm((f) => ({ ...f, forbid_all_tools: e.target.checked }))
                  }
                />
                Forbid all tools
              </label>
              <p className="text-[11px] text-gray-400 dark:text-zinc-600 mt-1">
                When on, the role runs with an empty tool set regardless of{" "}
                <span className="font-mono">tools[]</span>. Only useful for read-only / text-only
                roles (§D3).
              </p>
              {fieldError("forbid_all_tools")}
            </div>

            <div>
              <label className="block text-[11px] text-gray-400 dark:text-zinc-500 uppercase tracking-wide mb-1">
                <Wrench size={10} className="inline mr-1" />
                Tools (allowlist)
              </label>
              <textarea
                value={form.tools}
                onChange={(e) => setForm((f) => ({ ...f, tools: e.target.value }))}
                disabled={form.forbid_all_tools}
                rows={3}
                placeholder="grep, read, bash, post_inline_comment"
                className={clsx(
                  "w-full rounded-lg border px-3 py-2 text-[12px] font-mono resize-none focus:outline-none focus:ring-1 transition-colors",
                  form.forbid_all_tools
                    ? "bg-gray-100 dark:bg-zinc-800 border-gray-200 dark:border-zinc-700 text-gray-400 dark:text-zinc-600 cursor-not-allowed"
                    : "bg-gray-50 dark:bg-zinc-900 border-gray-200 dark:border-zinc-700 text-gray-800 dark:text-zinc-200 focus:border-indigo-500 focus:ring-indigo-500/30",
                )}
              />
              <p className="text-[11px] text-gray-400 dark:text-zinc-600 mt-1">
                Comma- or whitespace-separated tool ids. Empty = no restriction (§D3). Unknown
                tool ids emit <span className="font-mono">ToolDeclaredButMissing</span> at
                DECIDE time, deduped per run.
              </p>
              {fieldError("tools")}
            </div>
          </div>

          {/* Right pane — system prompt */}
          <div className="space-y-2">
            <div className="flex items-baseline gap-3">
              <label className="text-[11px] text-gray-400 dark:text-zinc-500 uppercase tracking-wide">
                System prompt
              </label>
              <span
                className={clsx(
                  "text-[10px] font-mono",
                  systemPromptBytes > SYSTEM_PROMPT_MAX_BYTES
                    ? "text-red-500 dark:text-red-400"
                    : "text-gray-400 dark:text-zinc-600",
                )}
              >
                system_prompt: {systemPromptBytes.toLocaleString()} /{" "}
                {SYSTEM_PROMPT_MAX_BYTES.toLocaleString()} bytes · body estimate:{" "}
                {totalBodyBytes.toLocaleString()} / 131,072 bytes
              </span>
            </div>
            <textarea
              value={form.system_prompt}
              onChange={(e) =>
                setForm((f) => ({ ...f, system_prompt: e.target.value }))
              }
              rows={32}
              className="w-full h-[70vh] rounded-lg bg-gray-50 dark:bg-zinc-900 border border-gray-200 dark:border-zinc-700 px-3 py-2 text-[11px] font-mono text-gray-800 dark:text-zinc-200 resize-none focus:outline-none focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500/30 transition-colors leading-relaxed"
              placeholder={
                "## Specialty\n…\n\n## Workflow\n### Phase 1: …\n### Phase 2: …\n\n## Tools\n…\n\n## Completion criteria\n…\n\n## What not to do\n- …\n- …\n- …\n"
              }
            />
            <p className="text-[11px] text-gray-400 dark:text-zinc-600">
              RFC 031 §Prompt Contract: specialty roles need five H2 sections — Specialty,
              Workflow (≥2 phases), Tools, Completion criteria, What not to do (≥3 bullets).
              Orchestrator-shadow prompts only need Completion criteria + What not to do.
            </p>
            {fieldError("system_prompt")}
          </div>
        </div>

        {/* Warning banner (from prior save) */}
        {warnings.length > 0 && (
          <div className="max-w-6xl mt-4 rounded-lg border border-blue-400 dark:border-blue-600 bg-blue-50 dark:bg-blue-950/40 px-4 py-3">
            <p className="text-[12px] font-semibold text-blue-700 dark:text-blue-300 mb-1">
              Advisories
            </p>
            {warnings.map((w, i) => (
              <p key={i} className="text-[11px] text-blue-800 dark:text-blue-200">
                <span className="font-mono text-blue-600 dark:text-blue-400">{w.code}</span>
                {" — "}
                {w.message}
              </p>
            ))}
          </div>
        )}

        {/* Submission row */}
        <div className="max-w-6xl flex items-center justify-end gap-3 mt-5">
          <button
            onClick={() => {
              if (mode === "edit" && roleId) {
                window.location.hash = `agent/${encodeURIComponent(roleId)}`;
              } else {
                window.location.hash = "agents";
              }
            }}
            className="flex items-center gap-1 px-3 py-1.5 rounded-md border border-gray-300 dark:border-zinc-700 text-gray-600 dark:text-zinc-400 hover:bg-gray-100 dark:hover:bg-zinc-800 text-[12px] font-medium transition-colors"
          >
            <X size={12} />
            Cancel
          </button>
          <button
            onClick={() => saveMut.mutate()}
            disabled={!canSubmit || saveMut.isPending}
            className="flex items-center gap-1.5 px-4 py-1.5 rounded-md bg-indigo-600 hover:bg-indigo-500 text-white text-[12px] font-medium transition-colors shadow-sm focus:outline-none focus-visible:ring-2 focus-visible:ring-indigo-400 disabled:bg-gray-300 dark:disabled:bg-zinc-700 disabled:text-gray-500 dark:disabled:text-zinc-500 disabled:cursor-not-allowed"
          >
            {saveMut.isPending ? (
              <>
                <Loader2 size={12} className="animate-spin" />
                Saving…
              </>
            ) : (
              <>
                <Save size={12} />
                {mode === "new" ? "Create role" : "Save changes"}
              </>
            )}
          </button>
        </div>
      </div>
    </div>
  );
}

export default AgentRoleEditorPage;
