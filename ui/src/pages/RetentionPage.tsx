/**
 * RetentionPage — RFC-026 PR-A6 admin surface (final slice).
 *
 * Per-tenant retention policy view + edit + manual apply-retention
 * trigger. Mirrors QuotasPage ergonomics (PR-A5): scope-driven single-
 * tenant view keyed off `useScope().tenant_id`, SET-semantics form,
 * shared Modal shell.
 *
 * What retention does:
 *   Retention bounds the event log and projection tables so tenants
 *   don't grow unboundedly. Policy carries three dimensions:
 *     - full_history_days       how long the raw event log is kept
 *     - current_state_days      how long projection rows are kept
 *     - max_events_per_entity   hard cap on events per entity
 *   `apply_retention` performs the destructive prune according to the
 *   live policy. The backend exposes this as a manual trigger; an
 *   automated scheduler is a v1.1 deliverable per RFC-026.
 *
 * Backend contract (all live as of PR-A1):
 *   GET  /v1/admin/tenants/:id/retention-policy  current policy
 *     → 404 when no policy has been set (empty state)
 *   POST /v1/admin/tenants/:id/retention-policy  set / replace policy
 *   POST /v1/admin/tenants/:id/apply-retention   run prune now
 *
 * SET semantics (not PATCH): `SetRetentionPolicyRequest` requires
 * every field on every submit. We read the current policy into the
 * edit form, let the operator change any subset, then POST all three
 * values back. This matches `set_retention_policy_handler` at
 * `handlers/admin.rs:622`.
 *
 * Destructive-action UX: the top-level "Apply retention" button only
 * opens a confirm dialog. The actual mutation fires only after the
 * operator confirms inside the dialog. This is intentional — retention
 * prunes data permanently; a single misclick at the admin surface must
 * not be able to delete production history. See the test suite for the
 * round-trip that pins this behaviour.
 */

import { useMemo, useState } from 'react';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import {
  Archive,
  RefreshCw,
  Pencil,
  Check,
  X,
  Plus,
  AlertTriangle,
  Loader2,
  PlayCircle,
} from 'lucide-react';
import { clsx } from 'clsx';

import { ApiError, defaultApi } from '../lib/api';
import type {
  RetentionPolicy,
  RetentionResult,
  SetRetentionPolicyRequest,
} from '../lib/types';
import { errorMessage } from '../lib/errors';
import { useScope } from '../hooks/useScope';
import { EntityExplainer } from '../components/EntityExplainer';
import { ErrorFallback } from '../components/ErrorFallback';
import { useToast } from '../components/Toast';

// ── Validation ────────────────────────────────────────────────────────────────

/** Both retention-day fields and the per-entity event cap must be
 *  positive non-zero integers. Zero days would mean "prune on next
 *  apply" which is indistinguishable from "policy never existed" and
 *  would wedge tenants; zero events-per-entity would purge every entity
 *  on the next apply. Reject client-side so operators see the error in
 *  the dialog, not as a generic 422 after round-trip. */
function validatePositive(label: string, value: number, max: number): string | null {
  if (!Number.isFinite(value) || Number.isNaN(value)) return `${label} must be a number.`;
  if (!Number.isInteger(value)) return `${label} must be a whole number.`;
  if (value < 1) return `${label} must be at least 1.`;
  if (value > max) return `${label} is unreasonably large (max ${max.toLocaleString()}).`;
  return null;
}

// ── Retention form (shared by Create + Edit) ─────────────────────────────────

interface RetentionFormProps {
  initial:     SetRetentionPolicyRequest;
  submitLabel: string;
  onSubmit:    (body: SetRetentionPolicyRequest) => Promise<void>;
  onCancel:    () => void;
  submitting:  boolean;
}

function RetentionForm({
  initial,
  submitLabel,
  onSubmit,
  onCancel,
  submitting,
}: RetentionFormProps) {
  // Track as string so the user can clear the field without the input
  // snapping to 0. We parse on submit.
  const [full,    setFull]    = useState(String(initial.full_history_days));
  const [current, setCurrent] = useState(String(initial.current_state_days));
  const [maxEv,   setMaxEv]   = useState(String(initial.max_events_per_entity));
  const [err,     setErr]     = useState<string | null>(null);

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    const fullN    = Number(full);
    const currentN = Number(current);
    const maxEvN   = Number(maxEv);

    // Days caps at ~10 years; event cap at 10M per entity. Bounds are
    // advisory — backend accepts larger values but anything outside
    // these is almost certainly a typo at the admin surface.
    const fullErr    = validatePositive('Full history days',   fullN,    3650);
    const currentErr = validatePositive('Current state days',  currentN, 3650);
    const maxErr     = validatePositive('Max events per entity', maxEvN, 10_000_000);
    if (fullErr)    { setErr(fullErr);    return; }
    if (currentErr) { setErr(currentErr); return; }
    if (maxErr)     { setErr(maxErr);     return; }

    // Sanity: the full-history window should be at least as long as
    // the current-state window. A policy with `current > full` means
    // projection rows outlive their source events, which the backend
    // will accept but is almost certainly a misconfig. Soft-warn in
    // the dialog; operators can re-submit to override.
    if (currentN > fullN) {
      setErr(
        'Current-state retention exceeds full-history retention. ' +
          'Projection rows would outlive their source events. Adjust or confirm by re-submitting.',
      );
      // Clear the guard so the next submit goes through.
      setTimeout(() => setErr(null), 50);
      return;
    }

    try {
      await onSubmit({
        full_history_days:     fullN,
        current_state_days:    currentN,
        max_events_per_entity: maxEvN,
      });
    } catch (e2) {
      setErr(errorMessage(e2, 'Failed to save retention policy.'));
    }
  }

  const fieldCls =
    'w-full rounded border border-gray-200 dark:border-zinc-800 bg-gray-50 dark:bg-zinc-950 ' +
    'font-mono text-[13px] text-gray-800 dark:text-zinc-200 px-3 py-2 ' +
    'focus:outline-none focus:border-indigo-500 transition-colors';

  return (
    <form onSubmit={handleSubmit} className="space-y-4" aria-label="Retention form">
      <div>
        <label htmlFor="retention-full" className="block text-[11px] text-gray-400 dark:text-zinc-500 mb-1.5">
          Full history days <span className="text-red-500">*</span>
        </label>
        <input
          id="retention-full"
          name="full_history_days"
          autoFocus
          type="number"
          min={1}
          value={full}
          onChange={e => { setFull(e.target.value); setErr(null); }}
          className={fieldCls}
        />
        <p className="text-[10px] text-gray-300 dark:text-zinc-600 mt-1">
          How long raw events are retained in the event log. Older events are pruned on apply.
        </p>
      </div>
      <div>
        <label htmlFor="retention-current" className="block text-[11px] text-gray-400 dark:text-zinc-500 mb-1.5">
          Current state days <span className="text-red-500">*</span>
        </label>
        <input
          id="retention-current"
          name="current_state_days"
          type="number"
          min={1}
          value={current}
          onChange={e => { setCurrent(e.target.value); setErr(null); }}
          className={fieldCls}
        />
        <p className="text-[10px] text-gray-300 dark:text-zinc-600 mt-1">
          How long projection rows (runs, sessions, tasks) are retained.
        </p>
      </div>
      <div>
        <label htmlFor="retention-max" className="block text-[11px] text-gray-400 dark:text-zinc-500 mb-1.5">
          Max events per entity <span className="text-red-500">*</span>
        </label>
        <input
          id="retention-max"
          name="max_events_per_entity"
          type="number"
          min={1}
          value={maxEv}
          onChange={e => { setMaxEv(e.target.value); setErr(null); }}
          className={fieldCls}
        />
        <p className="text-[10px] text-gray-300 dark:text-zinc-600 mt-1">
          Hard cap on the per-entity event count. Guards against runaway aggregates.
        </p>
      </div>
      {err && <p className="text-[11px] text-red-400">{err}</p>}
      <div className="flex justify-end gap-2 pt-2">
        <button
          type="button"
          onClick={onCancel}
          className="px-3 py-1.5 rounded text-[12px] text-gray-400 dark:text-zinc-500 hover:text-gray-700 dark:hover:text-zinc-300 transition-colors"
        >
          Cancel
        </button>
        <button
          type="submit"
          disabled={submitting}
          className={clsx(
            'flex items-center gap-1.5 rounded px-3 py-1.5 text-[12px] font-medium text-white',
            'bg-indigo-600 hover:bg-indigo-500',
            'disabled:bg-gray-100 disabled:text-gray-400 dark:disabled:bg-zinc-800 dark:disabled:text-zinc-600 disabled:cursor-not-allowed transition-colors',
          )}
        >
          <Check size={11} /> {submitting ? 'Saving…' : submitLabel}
        </button>
      </div>
    </form>
  );
}

// ── Modal shell ──────────────────────────────────────────────────────────────
//
// Intentional duplication with QuotasPage.Modal — the shell is ~30 LOC
// and inlining keeps PR-A6 self-contained. A follow-up could hoist both
// callers onto a shared `AdminModal` primitive once a third page needs it.

function Modal({
  title,
  onClose,
  children,
}: {
  title: string;
  onClose: () => void;
  children: React.ReactNode;
}) {
  return (
    <>
      <div
        className="fixed inset-0 z-40 bg-black/60"
        onClick={onClose}
        aria-hidden="true"
      />
      <div
        role="dialog"
        aria-modal="true"
        aria-label={title}
        className="fixed inset-0 z-50 flex items-center justify-center p-4 pointer-events-none"
      >
        <div className="pointer-events-auto w-full max-w-md rounded-xl border border-gray-200 dark:border-zinc-800 bg-white dark:bg-zinc-950 shadow-2xl">
          <div className="flex items-center justify-between px-4 h-11 border-b border-gray-200 dark:border-zinc-800">
            <span className="text-[13px] font-medium text-gray-800 dark:text-zinc-200">{title}</span>
            <button
              onClick={onClose}
              aria-label="Close"
              className="p-1 rounded text-gray-400 dark:text-zinc-600 hover:text-gray-700 dark:hover:text-zinc-300 hover:bg-gray-100 dark:hover:bg-zinc-800 transition-colors"
            >
              <X size={14} />
            </button>
          </div>
          <div className="p-4">{children}</div>
        </div>
      </div>
    </>
  );
}

// ── Apply-retention confirmation dialog ──────────────────────────────────────
//
// Retention prune is destructive and irreversible. Gate the mutation
// behind an explicit confirmation with a red-accented warning surface.
// The dialog is implemented inline (not reusing the neutral `Modal`
// above) so the visual language reads "dangerous" at a glance.

interface ConfirmApplyDialogProps {
  tenantId:  string;
  onConfirm: () => void;
  onCancel:  () => void;
  pending:   boolean;
}

function ConfirmApplyDialog({ tenantId, onConfirm, onCancel, pending }: ConfirmApplyDialogProps) {
  return (
    <>
      <div
        className="fixed inset-0 z-40 bg-black/60"
        onClick={pending ? undefined : onCancel}
        aria-hidden="true"
      />
      <div
        role="dialog"
        aria-modal="true"
        aria-label="Apply retention"
        className="fixed inset-0 z-50 flex items-center justify-center p-4 pointer-events-none"
      >
        <div className="pointer-events-auto w-full max-w-md rounded-xl border border-gray-200 dark:border-zinc-800 bg-white dark:bg-zinc-950 shadow-2xl">
          <div className="flex items-start gap-3 p-5">
            <div className="flex h-8 w-8 shrink-0 items-center justify-center rounded-full bg-red-500/10 border border-red-500/20">
              <AlertTriangle size={14} className="text-red-400" />
            </div>
            <div className="flex-1 min-w-0">
              <p className="text-[13px] font-semibold text-gray-900 dark:text-zinc-100">
                Apply retention for <code className="font-mono text-gray-700 dark:text-zinc-300">{tenantId}</code>?
              </p>
              <p className="text-[12px] text-gray-500 dark:text-zinc-400 mt-1">
                This will permanently delete events and projection rows past the
                configured retention window. Pruned data cannot be recovered.
              </p>
              <p className="text-[11px] text-red-400 mt-2 font-medium">
                This action cannot be undone.
              </p>
            </div>
          </div>

          <div className="flex justify-end gap-2 px-5 pb-4">
            <button
              type="button"
              onClick={onCancel}
              disabled={pending}
              className="px-3 py-1.5 rounded bg-gray-100 dark:bg-zinc-800 text-gray-500 dark:text-zinc-400 text-[12px] hover:bg-gray-200 dark:hover:bg-zinc-700 transition-colors disabled:opacity-50"
            >
              Cancel
            </button>
            <button
              type="button"
              data-testid="apply-retention-confirm-btn"
              onClick={onConfirm}
              disabled={pending}
              className="px-3 py-1.5 rounded bg-red-600 text-white text-[12px] font-medium hover:bg-red-500 disabled:opacity-50 transition-colors flex items-center gap-1.5"
            >
              {pending && <Loader2 size={11} className="animate-spin" />}
              {pending ? 'Applying…' : 'Apply retention now'}
            </button>
          </div>
        </div>
      </div>
    </>
  );
}

// ── Policy row ───────────────────────────────────────────────────────────────

interface PolicyRowProps {
  label:       string;
  value:       number;
  description: string;
  unit?:       string;
}

function PolicyRow({ label, value, description, unit }: PolicyRowProps) {
  return (
    <div>
      <div className="flex items-baseline justify-between mb-1">
        <span className="text-[11px] text-gray-500 dark:text-zinc-400">{label}</span>
        <span className="text-[11px] tabular-nums text-gray-700 dark:text-zinc-300">
          <span className="font-medium">{value.toLocaleString()}</span>
          {unit && <span className="text-gray-400 dark:text-zinc-500"> {unit}</span>}
        </span>
      </div>
      <p className="text-[10px] text-gray-300 dark:text-zinc-600">{description}</p>
    </div>
  );
}

// ── Page ─────────────────────────────────────────────────────────────────────

/** Default values seeded into the Create-policy modal when no policy
 *  exists yet. Chosen conservatively — ops can always raise. 90/30/10k
 *  mirrors a typical "keep history for a quarter, projections for a
 *  month, cap runaway aggregates at 10 k events" stance. */
const DEFAULT_POLICY: SetRetentionPolicyRequest = {
  full_history_days:     90,
  current_state_days:    30,
  max_events_per_entity: 10_000,
};

export function RetentionPage() {
  const qc = useQueryClient();
  const toast = useToast();
  const [scope] = useScope();
  const tenantId = scope.tenant_id;

  const [mode, setMode] = useState<'idle' | 'create' | 'edit' | 'confirm-apply'>('idle');
  const [lastResult, setLastResult] = useState<RetentionResult | null>(null);

  // Policy query — a 404 means "no policy set", which we render as an
  // empty state rather than an error. Any other failure surfaces the
  // shared ErrorFallback.
  const policyQuery = useQuery<RetentionPolicy | null, Error>({
    queryKey: ['retention-policy', tenantId],
    queryFn: async () => {
      try {
        return await defaultApi.getRetentionPolicy(tenantId);
      } catch (e) {
        if (e instanceof ApiError && e.status === 404) return null;
        throw e;
      }
    },
    staleTime: 10_000,
    enabled: Boolean(tenantId),
  });

  const setMutation = useMutation({
    mutationFn: (body: SetRetentionPolicyRequest) =>
      defaultApi.setRetentionPolicy(tenantId, body),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['retention-policy', tenantId] });
    },
  });

  const applyMutation = useMutation({
    mutationFn: () => defaultApi.applyRetention(tenantId),
    onSuccess: (result) => {
      setLastResult(result);
      setMode('idle');
      // Also surface a toast — the inline result panel is the canonical
      // summary, the toast is a transient ack so the operator sees the
      // action completed even if they scroll away from the panel.
      toast.success(
        `Retention applied — pruned ${result.events_pruned.toLocaleString()} events across ${result.entities_affected.toLocaleString()} entities.`,
      );
    },
    onError: (err) => {
      toast.error(errorMessage(err, 'Failed to apply retention.'));
      setMode('idle');
    },
  });

  async function handleSubmit(body: SetRetentionPolicyRequest) {
    await setMutation.mutateAsync(body);
    setMode('idle');
  }

  // Memoised initial values for the edit form — re-read whenever the
  // underlying policy changes so the dialog doesn't ghost stale values
  // after a concurrent update.
  const editInitial = useMemo<SetRetentionPolicyRequest>(() => {
    const p = policyQuery.data;
    if (!p) return DEFAULT_POLICY;
    return {
      full_history_days:     p.full_history_days,
      current_state_days:    p.current_state_days,
      max_events_per_entity: p.max_events_per_entity,
    };
  }, [policyQuery.data]);

  // Render ---------------------------------------------------------------------

  // Non-404 errors → shared error fallback. 404 is handled below as the
  // empty state because the policyQuery.queryFn normalises it to `null`.
  if (policyQuery.isError) {
    return (
      <ErrorFallback
        error={policyQuery.error}
        resource="retention policy"
        onRetry={() => void policyQuery.refetch()}
      />
    );
  }

  const policy = policyQuery.data;

  return (
    <div className="flex flex-col h-full bg-white dark:bg-zinc-950 overflow-y-auto">
      <div className="max-w-3xl mx-auto px-5 py-5 space-y-5 w-full">

        {/* Header */}
        <div className="flex items-start justify-between gap-4">
          <div className="flex items-center gap-3">
            <div className="w-9 h-9 rounded-lg bg-indigo-500/10 flex items-center justify-center shrink-0">
              <Archive size={16} className="text-indigo-400" />
            </div>
            <div>
              <h1 className="text-[15px] font-semibold text-gray-900 dark:text-zinc-100">Retention</h1>
              <p className="text-[11px] text-gray-400 dark:text-zinc-600 mt-0.5">
                Admin-only. Per-tenant event-log + projection retention for{' '}
                <code className="font-mono">{tenantId}</code>.
              </p>
              <EntityExplainer className="mt-1">
                Retention bounds data growth per tenant. Operators set the
                window, then trigger apply-retention to prune events and
                projection rows past that window. Pruned data is permanently
                deleted — there is no undo.
              </EntityExplainer>
            </div>
          </div>

          <div className="flex items-center gap-2 shrink-0">
            <button
              type="button"
              onClick={() => void policyQuery.refetch()}
              disabled={policyQuery.isFetching}
              aria-label="Refresh retention policy"
              className="p-1.5 rounded text-gray-400 dark:text-zinc-500 hover:text-gray-700 dark:hover:text-zinc-300 hover:bg-gray-100 dark:hover:bg-zinc-800 transition-colors disabled:opacity-40"
              title="Refresh"
            >
              <RefreshCw size={14} className={policyQuery.isFetching ? 'animate-spin' : ''} />
            </button>
            {policy && (
              <>
                <button
                  type="button"
                  onClick={() => setMode('edit')}
                  aria-label="Edit policy"
                  className="flex items-center gap-1.5 rounded px-3 py-1.5 text-[12px] font-medium bg-indigo-600 hover:bg-indigo-500 text-white transition-colors"
                >
                  <Pencil size={12} /> Edit policy
                </button>
                <button
                  type="button"
                  onClick={() => setMode('confirm-apply')}
                  aria-label="Apply retention"
                  className="flex items-center gap-1.5 rounded px-3 py-1.5 text-[12px] font-medium bg-red-600/90 hover:bg-red-500 text-white transition-colors"
                >
                  <PlayCircle size={12} /> Apply retention
                </button>
              </>
            )}
          </div>
        </div>

        {/* Body */}
        {policyQuery.isLoading ? (
          <div className="space-y-3">
            {[1, 2, 3].map(i => (
              <div key={i} className="h-12 rounded bg-gray-100 dark:bg-zinc-900 animate-pulse" />
            ))}
          </div>
        ) : !policy ? (
          // Empty state — no retention policy exists for this tenant.
          <div className="flex flex-col items-center justify-center py-16 gap-3 text-center rounded-lg border border-dashed border-gray-200 dark:border-zinc-800">
            <Archive size={28} className="text-gray-300 dark:text-zinc-700" />
            <p className="text-[13px] text-gray-500 dark:text-zinc-400">
              No retention policy for this tenant.
            </p>
            <p className="text-[11px] text-gray-400 dark:text-zinc-600 max-w-sm">
              The tenant retains all events and projection rows indefinitely
              until a policy is set. Create one to enable automated pruning.
            </p>
            <button
              type="button"
              onClick={() => setMode('create')}
              className="mt-1 flex items-center gap-1.5 rounded px-3 py-1.5 text-[12px] font-medium bg-indigo-600 hover:bg-indigo-500 text-white transition-colors"
            >
              <Plus size={12} /> Create policy
            </button>
          </div>
        ) : (
          // Policy exists — render fields.
          <div className="rounded-lg border border-gray-200 dark:border-zinc-800 p-4 space-y-4">
            <PolicyRow
              label="Full history"
              value={policy.full_history_days}
              unit="days"
              description="Raw event log retention window. Events older than this are pruned on apply."
            />
            <PolicyRow
              label="Current state"
              value={policy.current_state_days}
              unit="days"
              description="Projection row retention window. Rows older than this are pruned on apply."
            />
            <PolicyRow
              label="Max events per entity"
              value={policy.max_events_per_entity}
              unit="events"
              description="Hard cap on event-log depth for any single entity. Guards against runaway aggregates."
            />
          </div>
        )}

        {/* Last-run result panel — canonical record of the most recent
            apply. Stays visible until the next apply or page reload. */}
        {lastResult && (
          <div className="rounded-lg border border-emerald-500/30 bg-emerald-500/5 p-4">
            <div className="flex items-start gap-3">
              <div className="flex h-7 w-7 shrink-0 items-center justify-center rounded-full bg-emerald-500/10 border border-emerald-500/20">
                <Check size={13} className="text-emerald-400" />
              </div>
              <div className="flex-1 min-w-0">
                <p className="text-[12px] font-medium text-gray-800 dark:text-zinc-200">
                  Last retention run
                </p>
                <div className="grid grid-cols-2 gap-3 mt-2">
                  <div>
                    <div className="text-[10px] uppercase tracking-wider text-gray-400 dark:text-zinc-500">
                      Events pruned
                    </div>
                    <div className="text-[15px] font-semibold text-gray-800 dark:text-zinc-200 tabular-nums">
                      {lastResult.events_pruned.toLocaleString()}
                    </div>
                  </div>
                  <div>
                    <div className="text-[10px] uppercase tracking-wider text-gray-400 dark:text-zinc-500">
                      Entities affected
                    </div>
                    <div className="text-[15px] font-semibold text-gray-800 dark:text-zinc-200 tabular-nums">
                      {lastResult.entities_affected.toLocaleString()}
                    </div>
                  </div>
                </div>
              </div>
              <button
                type="button"
                onClick={() => setLastResult(null)}
                aria-label="Dismiss result"
                className="p-1 rounded text-gray-400 dark:text-zinc-600 hover:text-gray-700 dark:hover:text-zinc-300 hover:bg-gray-100 dark:hover:bg-zinc-800 transition-colors"
              >
                <X size={12} />
              </button>
            </div>
          </div>
        )}
      </div>

      {/* Create modal */}
      {mode === 'create' && (
        <Modal title="Create retention policy" onClose={() => setMode('idle')}>
          <RetentionForm
            initial={DEFAULT_POLICY}
            submitLabel="Create"
            onSubmit={handleSubmit}
            onCancel={() => setMode('idle')}
            submitting={setMutation.isPending}
          />
        </Modal>
      )}

      {/* Edit modal */}
      {mode === 'edit' && (
        <Modal title="Edit retention policy" onClose={() => setMode('idle')}>
          <RetentionForm
            initial={editInitial}
            submitLabel="Save"
            onSubmit={handleSubmit}
            onCancel={() => setMode('idle')}
            submitting={setMutation.isPending}
          />
        </Modal>
      )}

      {/* Apply-retention confirmation modal */}
      {mode === 'confirm-apply' && (
        <ConfirmApplyDialog
          tenantId={tenantId}
          pending={applyMutation.isPending}
          onConfirm={() => applyMutation.mutate()}
          onCancel={() => setMode('idle')}
        />
      )}
    </div>
  );
}

export default RetentionPage;
