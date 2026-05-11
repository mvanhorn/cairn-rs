import { useEffect, useRef, useState } from "react";
import { useMutation } from "@tanstack/react-query";
import { Loader2, Plus, X } from "lucide-react";
import { defaultApi } from "../lib/api";
import { errorMessage } from "../lib/errors";
import { useToast } from "./Toast";
import type { RunRecord } from "../lib/types";

// ── Constants ────────────────────────────────────────────────────────────────

/** Minimum characters for a meaningful goal. Agents with a one-word prompt
 *  invariably burn iterations re-asking the operator; 10 chars is the
 *  smallest threshold that filters "hi" / "test" / "foo" without rejecting
 *  real one-liners like "rename X to Y" (10). */
const GOAL_MIN_CHARS = 10;

/** Orchestrator hard cap — keeps a runaway iteration loop from eating
 *  all the tenant's budget before breakers trip. The backend accepts
 *  larger values, but operators who need >200 should configure it as
 *  a breaker override on the orchestrate call, not as a per-run default. */
const MAX_ITERATIONS_CAP = 200;

/** Default iteration budget. Matches the orchestrator's own default when
 *  the operator does not specify one (see `OrchestrateRequest` in
 *  `crates/cairn-app/src/handlers/runs/orchestrate.rs`). */
const DEFAULT_MAX_ITERATIONS = 40;

// ── Types ────────────────────────────────────────────────────────────────────

interface NewRunDialogProps {
  /** Session to create the run under. */
  sessionId: string;
  /** Called on the Escape key or the backdrop-click close path. The parent
   *  is responsible for dropping the dialog from the tree (unmount). */
  onClose: () => void;
  /** Called after both `POST /v1/runs` and `POST /v1/runs/:id/orchestrate`
   *  have completed successfully. The parent typically invalidates the
   *  session-runs query and routes to the new run's detail page. */
  onCreated: (run: RunRecord) => void;
}

// ── Component ────────────────────────────────────────────────────────────────

/**
 * Single-run creation dialog for the session-detail page (issue #635).
 *
 * Two chained POSTs:
 *   1. `POST /v1/runs`                   — allocate the run ID under the
 *                                          current scope + session.
 *   2. `POST /v1/runs/:id/orchestrate`   — hand the goal + iteration
 *                                          budget to the orchestrator so
 *                                          the run starts executing.
 *
 * Partial failure handling: if (2) fails after (1) has succeeded, the
 * mutation surfaces both facts in a single error toast — the created
 * run still exists and is visible on the session-detail page, but the
 * operator knows the orchestration kick-off did not land and must
 * retry manually from the run-detail page.
 */
export function NewRunDialog({ sessionId, onClose, onCreated }: NewRunDialogProps) {
  const [goal, setGoal] = useState("");
  const [maxIterations, setMaxIterations] = useState<number>(DEFAULT_MAX_ITERATIONS);
  const [planMode, setPlanMode] = useState(false);
  const [touched, setTouched] = useState(false);
  const toast = useToast();

  const goalRef = useRef<HTMLTextAreaElement>(null);

  // Focus the goal textarea on mount — the goal is the only required
  // field and also the field the operator is most likely to type first.
  useEffect(() => {
    goalRef.current?.focus();
  }, []);

  // Escape key closes the dialog. Bound to window rather than the root
  // element so the keybind works even when focus is inside the number
  // input (which swallows its own Escape by default in some browsers).
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") onClose();
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [onClose]);

  // ── Validation ─────────────────────────────────────────────────────────────

  const trimmedGoal = goal.trim();
  const goalTooShort = trimmedGoal.length < GOAL_MIN_CHARS;
  const iterationsValid =
    Number.isFinite(maxIterations) &&
    Number.isInteger(maxIterations) &&
    maxIterations >= 1 &&
    maxIterations <= MAX_ITERATIONS_CAP;
  const canSubmit = !goalTooShort && iterationsValid;

  const goalError = touched && goalTooShort
    ? trimmedGoal.length === 0
      ? "Goal is required."
      : `Goal is too short — at least ${GOAL_MIN_CHARS} characters.`
    : null;
  const iterationsError = touched && !iterationsValid
    ? `Must be between 1 and ${MAX_ITERATIONS_CAP}.`
    : null;

  // ── Mutation ───────────────────────────────────────────────────────────────

  const mutation = useMutation({
    mutationFn: async () => {
      // Step 1 — allocate the run. `run_id` is REQUIRED by POST /v1/runs
      // (422 otherwise — see `CreateRunRequest::validate`); mint a
      // collision-resistant one client-side so the operator never has to
      // invent one. `crypto.randomUUID` is gated because the UI runs in a
      // mixed set of contexts (native browser, headless Playwright);
      // fall back to Date.now+random for the degraded case.
      const runId = `run_${
        typeof crypto !== "undefined" && "randomUUID" in crypto
          ? crypto.randomUUID().replace(/-/g, "").slice(0, 12)
          : `${Date.now().toString(36)}${Math.random().toString(36).slice(2, 8)}`
      }`;
      // Send the goal as both `prompt` on the create call (so the run's
      // default goal is persisted for any future auto-orchestrate that
      // does not carry a body) AND on the orchestrate body (explicit
      // per-invocation override — see F42 comment on `CreateRunRequest`).
      // The server treats the orchestrate body as authoritative when
      // both are present.
      const run = await defaultApi.createRun({
        session_id: sessionId,
        run_id: runId,
        mode: planMode ? { type: "plan" } : undefined,
        prompt: trimmedGoal,
      });

      // Step 2 — kick off orchestration. If this fails the caller still
      // has a visible run on the session page; the error copy makes the
      // partial state explicit so the operator knows to retry the
      // orchestrate action from the run-detail page rather than
      // creating a second run.
      try {
        await defaultApi.orchestrateRun(run.run_id, {
          goal: trimmedGoal,
          max_iterations: maxIterations,
        });
      } catch (orchestrateErr) {
        throw new OrchestrateKickoffError(run, orchestrateErr);
      }
      return run;
    },
    onSuccess: (run) => {
      toast.success(`Run ${run.run_id} started.`);
      onCreated(run);
    },
    onError: (err) => {
      if (err instanceof OrchestrateKickoffError) {
        // Surface BOTH facts: the run exists, but kick-off failed.
        // Giving the operator the run_id lets them navigate to it
        // and hit Orchestrate manually from the detail page.
        toast.error(
          `Run ${err.run.run_id} created, but orchestration kick-off failed: ${errorMessage(err.cause, "unknown error")}. Open the run and retry.`,
        );
        // Still dismiss the dialog — the run is created, so the
        // session page should refresh. The operator follows up from
        // run detail.
        onCreated(err.run);
        return;
      }
      toast.error(errorMessage(err, "Failed to create run."));
    },
  });

  function submit() {
    setTouched(true);
    if (!canSubmit || mutation.isPending) return;
    mutation.mutate();
  }

  // ── Render ─────────────────────────────────────────────────────────────────

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/60"
      role="dialog"
      aria-modal="true"
      aria-labelledby="newrun-dialog-title"
      data-testid="new-run-dialog"
      // Backdrop click closes. Inner card stops propagation so clicking
      // into the form does not dismiss it.
      onClick={onClose}
    >
      <div
        className="w-full max-w-md rounded-lg bg-gray-50 dark:bg-zinc-900 border border-gray-200 dark:border-zinc-800 shadow-xl"
        onClick={(e) => e.stopPropagation()}
      >
        {/* Header */}
        <div className="flex items-center justify-between px-4 py-3 border-b border-gray-200 dark:border-zinc-800">
          <h2
            id="newrun-dialog-title"
            className="text-[13px] font-medium text-gray-800 dark:text-zinc-200"
          >
            New Run
          </h2>
          <button
            onClick={onClose}
            aria-label="Close"
            className="p-1 rounded text-gray-400 dark:text-zinc-600 hover:text-gray-700 dark:hover:text-zinc-300 transition-colors"
          >
            <X size={14} />
          </button>
        </div>

        {/* Form */}
        <div className="px-4 py-4 space-y-4">
          <p className="text-[11px] text-gray-400 dark:text-zinc-500">
            Session{" "}
            <span className="font-mono text-gray-700 dark:text-zinc-300">{sessionId}</span>
          </p>

          <div>
            <label
              htmlFor="newrun-goal"
              className="block text-[11px] text-gray-400 dark:text-zinc-500 mb-1"
            >
              Goal <span className="text-red-500">*</span>
            </label>
            <textarea
              id="newrun-goal"
              ref={goalRef}
              data-testid="new-run-goal"
              rows={4}
              value={goal}
              onChange={(e) => setGoal(e.target.value)}
              onBlur={() => setTouched(true)}
              placeholder="Describe what the agent should accomplish. Be specific — the orchestrator passes this verbatim to the LLM as the user message."
              className="w-full rounded border border-gray-200 dark:border-zinc-700 bg-gray-100 dark:bg-zinc-800 text-gray-800 dark:text-zinc-200 text-[13px]
                         px-3 py-2 focus:outline-none focus:border-indigo-500 resize-y min-h-[96px]"
              aria-invalid={goalError ? "true" : "false"}
              aria-describedby={goalError ? "newrun-goal-err" : undefined}
            />
            {goalError ? (
              <p
                id="newrun-goal-err"
                data-testid="new-run-goal-err"
                className="mt-1 text-[11px] text-red-500"
              >
                {goalError}
              </p>
            ) : (
              <p className="mt-1 text-[10px] text-gray-400 dark:text-zinc-600">
                {trimmedGoal.length} / {GOAL_MIN_CHARS}+ characters
              </p>
            )}
          </div>

          <div>
            <label
              htmlFor="newrun-maxiter"
              className="block text-[11px] text-gray-400 dark:text-zinc-500 mb-1"
            >
              Max iterations
            </label>
            <input
              id="newrun-maxiter"
              data-testid="new-run-max-iter"
              type="number"
              min={1}
              max={MAX_ITERATIONS_CAP}
              value={maxIterations}
              onChange={(e) => {
                // Preserve NaN when the input is cleared so the user
                // can retype without the field snapping to a previous
                // valid value mid-edit. Validation gate catches NaN.
                const n = Number(e.target.value);
                setMaxIterations(Number.isFinite(n) ? n : NaN);
              }}
              onBlur={() => setTouched(true)}
              className="w-full rounded border border-gray-200 dark:border-zinc-700 bg-gray-100 dark:bg-zinc-800 text-gray-800 dark:text-zinc-200 text-[13px]
                         px-3 py-2 focus:outline-none focus:border-indigo-500 tabular-nums"
              aria-invalid={iterationsError ? "true" : "false"}
              aria-describedby={iterationsError ? "newrun-maxiter-err" : "newrun-maxiter-help"}
            />
            {iterationsError ? (
              <p
                id="newrun-maxiter-err"
                data-testid="new-run-max-iter-err"
                className="mt-1 text-[11px] text-red-500"
              >
                {iterationsError}
              </p>
            ) : (
              <p
                id="newrun-maxiter-help"
                className="mt-1 text-[10px] text-gray-400 dark:text-zinc-600"
              >
                Upper bound on GATHER → DECIDE → EXECUTE rounds before the orchestrator gives up.
                Maximum {MAX_ITERATIONS_CAP}.
              </p>
            )}
          </div>

          <label className="flex items-start gap-3 rounded border border-gray-200 dark:border-zinc-700 bg-gray-100 dark:bg-zinc-800/80 px-3 py-2 cursor-pointer">
            <input
              type="checkbox"
              data-testid="new-run-plan-mode"
              checked={planMode}
              onChange={(e) => setPlanMode(e.target.checked)}
              className="mt-0.5 rounded border-gray-300 dark:border-zinc-600 text-indigo-600 focus:ring-indigo-500"
            />
            <span className="space-y-1">
              <span className="block text-[12px] font-medium text-gray-800 dark:text-zinc-200">
                Plan mode
              </span>
              <span className="block text-[10px] text-gray-400 dark:text-zinc-500">
                Run starts in plan mode — the review panel on the run-detail page opens before execution.
              </span>
            </span>
          </label>
        </div>

        {/* Actions */}
        <div className="flex items-center justify-end gap-2 px-4 py-3 border-t border-gray-200 dark:border-zinc-800">
          <button
            onClick={onClose}
            disabled={mutation.isPending}
            className="rounded border border-gray-200 dark:border-zinc-700 text-gray-500 dark:text-zinc-400 text-[12px] px-3 py-1.5 hover:text-gray-800 dark:hover:text-zinc-200 transition-colors disabled:opacity-40"
          >
            Cancel
          </button>
          <button
            data-testid="new-run-submit"
            onClick={submit}
            disabled={mutation.isPending || !canSubmit}
            data-pending={mutation.isPending ? "true" : "false"}
            className="flex items-center gap-1.5 rounded bg-indigo-600 hover:bg-indigo-500
                       text-white text-[12px] font-medium px-3 py-1.5 disabled:opacity-40 transition-colors"
            title={
              !canSubmit
                ? goalTooShort
                  ? `Goal must be at least ${GOAL_MIN_CHARS} characters.`
                  : `Max iterations must be between 1 and ${MAX_ITERATIONS_CAP}.`
                : "Create run and start orchestration"
            }
          >
            {mutation.isPending
              ? <Loader2 size={12} className="animate-spin" />
              : <Plus size={12} />}
            {mutation.isPending ? "Creating…" : "Create run"}
          </button>
        </div>
      </div>
    </div>
  );
}

/**
 * Narrow error thrown from `createRun → orchestrateRun` when the run was
 * allocated but the orchestration kick-off failed. Carries the created
 * run record so the toast handler can surface its ID and the session
 * page can refresh to show the new (pending) row.
 */
class OrchestrateKickoffError extends Error {
  readonly run: RunRecord;
  readonly cause: unknown;
  constructor(run: RunRecord, cause: unknown) {
    super("orchestration kick-off failed");
    this.name = "OrchestrateKickoffError";
    this.run = run;
    this.cause = cause;
  }
}

export default NewRunDialog;
